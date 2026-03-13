import os
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, LongType


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FPTPlay-Transform")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


def clean(df):

    before = df.count()

    df = (
        df
        .dropDuplicates(["Mac", "session_timestamp", "Event", "ItemId"])
        .filter(F.col("Mac").isNotNull())
        .filter(F.col("session_timestamp").isNotNull())
        .fillna({"RealTimePlaying": 0.0, "ElapsedTimePlaying": 0})
        .withColumn("AppName", F.upper(F.trim(F.col("AppName"))))
        .withColumn("Event",   F.upper(F.trim(F.col("Event"))))
    )

    after = df.count()
    print(f"[clean] {before:,} → {after:,} dòng (bỏ {before - after:,} duplicates/nulls)")
    return df


def feature_engineering(df):
    
    df = (
        df
        .withColumn("hour_of_day",  F.hour("session_timestamp"))
        .withColumn("day_of_week",  F.dayofweek("session_timestamp"))   
        .withColumn("session_date", F.to_date("session_timestamp"))
        .withColumn("month",        F.month("session_timestamp"))
        .withColumn("year",         F.year("session_timestamp"))

        .withColumn(
            "completion_rate",
            F.when(
                (F.col("Duration").isNotNull()) & (F.col("Duration") > 0),
                F.round(F.col("RealTimePlaying") / F.col("Duration"), 4)
            ).otherwise(None)
        )

        .withColumn(
            "watch_category",
            F.when(F.col("RealTimePlaying") < 60,   "short")       
             .when(F.col("RealTimePlaying") < 600,  "medium")    
             .when(F.col("RealTimePlaying") < 3600, "long")      
             .otherwise("binge")                                 
        )

        .withColumn(
            "is_primetime",
            F.when((F.col("hour_of_day") >= 18) & (F.col("hour_of_day") <= 23), 1).otherwise(0)
        )

        .withColumn(
            "is_weekend",
            F.when(F.col("day_of_week").isin(1, 7), 1).otherwise(0)
        )
    )

    return df


def build_fact_sessions(df):
    return df.select(
        "Mac", "CustomerID", "Contract",
        "session_timestamp", "session_date",
        "hour_of_day", "day_of_week", "month", "year",
        "AppName", "Event",
        "ItemId", "ItemName",
        "RealTimePlaying", "ElapsedTimePlaying", "Duration",
        "completion_rate", "watch_category",
        "is_primetime", "is_weekend",
        "Screen", "Firmware", "PublishCountry",
    )


def build_user_profile(df):
   
    max_date = df.agg(F.max("session_date")).collect()[0][0]
    print(f"[user_profile] Dataset end date: {max_date}")

    w_user = Window.partitionBy("Mac").orderBy("session_timestamp")

    df = df.withColumn("session_rank", F.row_number().over(w_user))

    user_df = df.groupBy("Mac", "CustomerID", "Contract").agg(
        # Activity
        F.count("*").alias("total_events"),
        F.countDistinct("session_date").alias("active_days"),
        F.countDistinct(
            F.concat_ws("_", F.col("session_date"), F.col("hour_of_day"))
        ).alias("session_count"),

        # Watch time
        F.sum("RealTimePlaying").alias("total_watch_seconds"),
        F.avg("RealTimePlaying").alias("avg_watch_per_event"),
        F.max("RealTimePlaying").alias("max_single_watch"),

        # Content diversity
        F.countDistinct("ItemId").alias("unique_content_watched"),
        F.countDistinct("AppName").alias("unique_apps_used"),

        # Recency
        F.max("session_date").alias("last_active_date"),
        F.min("session_date").alias("first_active_date"),

        # Behavior patterns
        F.avg("is_primetime").alias("primetime_ratio"),
        F.avg("is_weekend").alias("weekend_ratio"),
        F.avg("completion_rate").alias("avg_completion_rate"),

        # Most used app
        F.first(
            F.col("AppName"), ignorenulls=True
        ).alias("most_used_app_approx"),   # dùng mode proper ở SQL layer
    )

    user_df = user_df.withColumn(
        "days_inactive",
        F.datediff(F.lit(max_date), F.col("last_active_date"))
    ).withColumn(
        "days_since_first",
        F.datediff(F.col("last_active_date"), F.col("first_active_date"))
    ).withColumn(
        "avg_daily_watch_seconds",
        F.when(
            F.col("active_days") > 0,
            F.col("total_watch_seconds") / F.col("active_days")
        ).otherwise(0)
    )

    total   = user_df.count()
    return user_df

def save(df, path, name):
    out = os.path.join(path, f"{name}.parquet")
    df.write.mode("overwrite").parquet(out)
    print(f"[save] {name} → {out}  ({df.count():,} rows)")


def main():
    INPUT_DIR  = "data/processed/logs_clean.parquet"
    OUTPUT_DIR = "data/processed"

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 50)
    print("STEP 1: Load cleaned Parquet")
    print("=" * 50)
    df = spark.read.parquet(INPUT_DIR)
    print(f"Loaded {df.count():,} rows")

    print("\nSTEP 2: Clean")
    df = clean(df)

    print("\nSTEP 3: Feature Engineering")
    df = feature_engineering(df)

    print("\nSTEP 4: Build fact_sessions")
    fact_df = build_fact_sessions(df)
    save(fact_df, OUTPUT_DIR, "fact_sessions")

    print("\nSTEP 5: Build user_profile (churn labels)")
    user_df = build_user_profile(df)
    user_df.show(5, truncate=40)
    save(user_df, OUTPUT_DIR, "user_profiles")

    print("\nTransformation Done!")
    spark.stop()


if __name__ == "__main__":
    main()

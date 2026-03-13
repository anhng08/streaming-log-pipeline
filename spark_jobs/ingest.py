import ast
import os
import glob
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, LongType

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FPTPlay-LogIngest")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.debug.maxToStringFields", "100")   # tắt WARN truncate
        .getOrCreate()
    )

def parse_dict_line(line: str) -> dict | None:
    line = line.strip()
    if not line:
        return None
    try:
        record = ast.literal_eval(line)
        if not isinstance(record, dict):
            return None
        return {k: str(v) if v is not None else None for k, v in record.items()}
    except Exception:
        return None

def ingest_raw_logs(spark: SparkSession, data_dir: str):
    files = glob.glob(os.path.join(data_dir, "logt*.txt"))
    if not files:
        raise FileNotFoundError(f"logt*.txt not found in '{data_dir}'")

    print(f"[ingest] Found {len(files)} file:")
    for f in sorted(files):
        size_mb = os.path.getsize(f) / 1024 / 1024
        print(f"         {os.path.basename(f)}  ({size_mb:.1f} MB)")

    raw_rdd = spark.sparkContext.textFile(",".join(sorted(files)))

    parsed_rdd = (
        raw_rdd
        .map(parse_dict_line)
        .filter(lambda x: x is not None)
        .filter(lambda x: "Mac" in x or "MAC" in x)
    )

    df = spark.createDataFrame(parsed_rdd)

    total = df.count()
    print(f"\n[ingest] {total:,} read successfully")

    print(f"\n[ingest] Columns in data ({len(df.columns)} col):")
    for col in sorted(df.columns):
        print(f"         {col}")

    return df

COLUMN_ALIASES = {
    "mac":              "Mac",
    "MAC":              "Mac",

    "customerid":       "CustomerID",
    "Customerid":       "CustomerID",

    "sessionmainmenu":  "SessionMainMenu",
    "sessionSubMenu":   "SessionSubMenu",

    "appname":          "AppName",
    "appid":            "AppId",

    "event":            "Event",

    "itemid":           "ItemId",
    "itemId":           "ItemId",
    "ItemID":           "ItemId",
    "itemname":         "ItemName",

    "realtimeplaying":  "RealTimePlaying",
    "realTimePlaying":  "RealTimePlaying",
    "Realtimeplaying":  "RealTimePlaying",

    "elapsedtimeplaying": "ElapsedTimePlaying",
    "ElapsedTimePlaying": "ElapsedTimePlaying",

    "duration":         "Duration",

    "firmware":         "Firmware",
    "screen":           "Screen",
    "publishcountry":   "PublishCountry",
    "publishCountry":   "PublishCountry",
}

KEEP_FIELDS = [
    "Mac", "CustomerID", "Contract",
    "Session", "SessionMainMenu",
    "AppName", "Event",
    "ItemId", "ItemName",
    "RealTimePlaying", "ElapsedTimePlaying", "Duration",
    "Screen", "Firmware", "PublishCountry",
]


def normalize_columns(df):
    
    for old_name, new_name in COLUMN_ALIASES.items():
        if old_name in df.columns and old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)
            print(f"[normalize] Rename: '{old_name}' → '{new_name}'")
    return df

def parse_session_timestamp(df):
    ts_col = "SessionMainMenu"
    if ts_col not in df.columns:
        print(f"[timestamp] WARN: cột '{ts_col}' không tìm thấy, bỏ qua")
        return df.withColumn("session_timestamp", F.lit(None).cast("timestamp"))

    df = df.withColumn(
        "session_ts_raw",
        F.regexp_extract(ts_col, r":(\d{4}:\d{2}:\d{2}:\d{2}:\d{2}:\d{2})", 1)
    )
    df = df.withColumn(
        "session_ts_str",
        F.regexp_replace(
            "session_ts_raw",
            r"^(\d{4}):(\d{2}):(\d{2}):(\d{2}):(\d{2}):(\d{2})$",
            "$1-$2-$3 $4:$5:$6"
        )
    )
    df = df.withColumn(
        "session_timestamp",
        F.to_timestamp("session_ts_str", "yyyy-MM-dd HH:mm:ss")
    )
    return df.drop("session_ts_raw", "session_ts_str")

def select_and_cast(df):
    available = [c for c in KEEP_FIELDS if c in df.columns]

    missing = [c for c in KEEP_FIELDS if c not in df.columns]
    if missing:
        print(f"[cast] WARN: các cột sau không có trong data, bỏ qua: {missing}")

    df = df.select(available + ["session_timestamp"])

    cast_map = {
        "RealTimePlaying":    FloatType(),
        "ElapsedTimePlaying": LongType(),
        "Duration":           LongType(),
    }
    for col_name, dtype in cast_map.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(dtype))

    return df

def save_to_parquet(df, output_dir: str):
    output_path = os.path.join(output_dir, "logs_clean.parquet")
    os.makedirs(output_dir, exist_ok=True)

    # partitionBy AppName nếu cột đó tồn tại
    writer = df.write.mode("overwrite")
    if "AppName" in df.columns:
        writer = writer.partitionBy("AppName")

    writer.parquet(output_path)
    print(f"[save] Parquet in: {output_path}")
    return output_path


def main():
    DATA_DIR   = "data/raw"
    OUTPUT_DIR = "data/processed"

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 55)
    print("STEP 1: Ingest raw logs")
    print("=" * 55)
    df_raw = ingest_raw_logs(spark, DATA_DIR)

    print("\nSTEP 2: Normalize column names")
    df = normalize_columns(df_raw)

    print("\nSTEP 3: Parse timestamps")
    df = parse_session_timestamp(df)

    print("\nSTEP 4: Select & cast")
    df = select_and_cast(df)

    print("\nSTEP 5: Preview schema + sample")
    df.printSchema()
    df.show(5, truncate=60)

    print("\nNull counts:")
    check_cols = [c for c in ["Mac", "session_timestamp", "RealTimePlaying", "Event"] if c in df.columns]
    df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in check_cols
    ]).show()

    print("\nSTEP 6: Save to Parquet")
    save_to_parquet(df, OUTPUT_DIR)

    print("\nIngestion Done!")
    spark.stop()


if __name__ == "__main__":
    main()

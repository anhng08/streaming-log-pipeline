import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    host   = os.getenv("POSTGRES_HOST",     "localhost")
    port   = os.getenv("POSTGRES_PORT",     "5432")
    db     = os.getenv("POSTGRES_DB",       "log_pipeline")
    user   = os.getenv("POSTGRES_USER",     "de_user")
    pwd    = os.getenv("POSTGRES_PASSWORD", "de_password")
    url    = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url)

def load_parquet_to_postgres(parquet_path: str, table_name: str, engine, chunksize: int = 50_000):

    print(f"[load] Read {parquet_path} ...")
    df = pd.read_parquet(parquet_path)

    total = len(df)
    print(f"[load] {total:,} rows → table '{table_name}'")

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",       
        index=False,
        chunksize=chunksize,
        method="multi",           
    )

    print(f"[load] Loaded {total:,} rows to '{table_name}'")


def verify(table_name: str, engine):
    with engine.connect() as conn:
        count = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar()
        print(f"[verify] {table_name}: {count:,} rows in PostgreSQL ✓")


def main():
    PROCESSED = "data/processed"
    engine = get_engine()

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("[load] PostgreSQL connection OK ✓")
    except Exception as e:
        print(f"[load] Can't connect to PostgreSQL: {e}")
        return

    load_parquet_to_postgres(
        parquet_path=f"{PROCESSED}/fact_sessions.parquet",
        table_name="fact_sessions",
        engine=engine,
    )
    verify("fact_sessions", engine)

    load_parquet_to_postgres(
        parquet_path=f"{PROCESSED}/user_profiles.parquet",
        table_name="user_profiles",
        engine=engine,
    )
    verify("user_profiles", engine)

    print("\n Loaded Successfully!")


if __name__ == "__main__":
    main()

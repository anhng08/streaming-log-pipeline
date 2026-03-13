-- sql/schema.sql
-- Tự động chạy khi docker compose up (mount vào initdb.d)

-- ─────────────────────────────────────────
-- FACT TABLE: mỗi row = 1 event xem
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_sessions (
    id                  SERIAL PRIMARY KEY,
    "Mac"               VARCHAR(20),
    "CustomerID"        VARCHAR(20),
    "Contract"          VARCHAR(20),
    session_timestamp   TIMESTAMP,
    session_date        DATE,
    hour_of_day         SMALLINT,
    day_of_week         SMALLINT,
    month               SMALLINT,
    year                SMALLINT,
    "AppName"           VARCHAR(20),
    "Event"             VARCHAR(30),
    "ItemId"            VARCHAR(20),
    "ItemName"          TEXT,
    "RealTimePlaying"   FLOAT,
    "ElapsedTimePlaying" BIGINT,
    "Duration"          BIGINT,
    completion_rate     FLOAT,
    watch_category      VARCHAR(10),
    is_primetime        SMALLINT,
    is_weekend          SMALLINT,
    "Screen"            VARCHAR(50),
    "Firmware"          VARCHAR(20),
    "PublishCountry"    VARCHAR(50)
);

-- ─────────────────────────────────────────
-- USER PROFILE: mỗi row = 1 user (aggregate)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS user_profiles (
    "Mac"                    VARCHAR(20) PRIMARY KEY,
    "CustomerID"             VARCHAR(20),
    "Contract"               VARCHAR(20),
    total_events             BIGINT,
    active_days              BIGINT,
    session_count            BIGINT,
    total_watch_seconds      FLOAT,
    avg_watch_per_event      FLOAT,
    max_single_watch         FLOAT,
    unique_content_watched   BIGINT,
    unique_apps_used         BIGINT,
    last_active_date         DATE,
    first_active_date        DATE,
    primetime_ratio          FLOAT,
    weekend_ratio            FLOAT,
    avg_completion_rate      FLOAT,
    days_inactive            INTEGER,
    days_since_first         INTEGER,
    avg_daily_watch_seconds  FLOAT,
    churn_label              SMALLINT
);

-- Indexes để query analytics nhanh hơn
CREATE INDEX IF NOT EXISTS idx_fact_date    ON fact_sessions (session_date);
CREATE INDEX IF NOT EXISTS idx_fact_mac     ON fact_sessions ("Mac");
CREATE INDEX IF NOT EXISTS idx_fact_app     ON fact_sessions ("AppName");
CREATE INDEX IF NOT EXISTS idx_user_churn   ON user_profiles (churn_label);

-- ─────────────────────────────────────────
-- Tạo database riêng cho Metabase app
-- Chạy 1 lần khi docker compose up lần đầu
-- ─────────────────────────────────────────
SELECT 'CREATE DATABASE metabase_app OWNER de_user'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'metabase_app'
)\gexec

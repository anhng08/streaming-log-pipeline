-- sql/analytics.sql
-- Chạy trên PostgreSQL sau khi đã load fact_sessions + user_profiles

-- ═══════════════════════════════════════════════════════
-- 1. DAU — Daily Active Users
-- ═══════════════════════════════════════════════════════
SELECT
    session_date,
    COUNT(DISTINCT "Mac")   AS dau,
    COUNT(*)                AS total_events,
    ROUND(AVG("RealTimePlaying"), 1) AS avg_watch_sec
FROM fact_sessions
GROUP BY session_date
ORDER BY session_date;


-- ═══════════════════════════════════════════════════════
-- 2. Top 10 nội dung theo tổng thời gian xem
-- ═══════════════════════════════════════════════════════
SELECT
    "ItemId",
    MAX("ItemName")                        AS item_name,
    SUM("RealTimePlaying")                 AS total_watch_seconds,
    ROUND(SUM("RealTimePlaying") / 3600, 1) AS total_watch_hours,
    COUNT(DISTINCT "Mac")                  AS unique_viewers,
    ROUND(AVG("completion_rate"), 3)       AS avg_completion
FROM fact_sessions
WHERE "Event" IN ('STOPVOD', 'PLAYVOD')
GROUP BY "ItemId"
ORDER BY total_watch_seconds DESC
LIMIT 10;


-- ═══════════════════════════════════════════════════════
-- 3. Heatmap: giờ xem × ngày trong tuần
--    day_of_week: 1=Sun, 2=Mon, ..., 7=Sat
-- ═══════════════════════════════════════════════════════
SELECT
    hour_of_day,
    day_of_week,
    COUNT(DISTINCT "Mac")           AS active_users,
    ROUND(AVG("RealTimePlaying"), 1) AS avg_watch_sec
FROM fact_sessions
GROUP BY hour_of_day, day_of_week
ORDER BY day_of_week, hour_of_day;


-- ═══════════════════════════════════════════════════════
-- 4. App usage breakdown
-- ═══════════════════════════════════════════════════════
SELECT
    "AppName",
    COUNT(DISTINCT "Mac")                        AS unique_users,
    COUNT(*)                                     AS total_events,
    ROUND(SUM("RealTimePlaying") / 3600, 0)      AS total_watch_hours,
    ROUND(AVG("RealTimePlaying"), 1)             AS avg_watch_per_event
FROM fact_sessions
GROUP BY "AppName"
ORDER BY total_watch_hours DESC;


-- ═══════════════════════════════════════════════════════
-- 5. Churn analysis — từ user_profiles
-- ═══════════════════════════════════════════════════════
SELECT
    churn_label,
    COUNT(*)                                    AS user_count,
    ROUND(AVG(total_watch_seconds) / 3600, 1)  AS avg_watch_hours,
    ROUND(AVG(active_days), 1)                 AS avg_active_days,
    ROUND(AVG(unique_content_watched), 1)      AS avg_content_variety,
    ROUND(AVG(avg_completion_rate), 3)         AS avg_completion,
    ROUND(AVG(primetime_ratio), 3)             AS avg_primetime_ratio
FROM user_profiles
GROUP BY churn_label;


-- ═══════════════════════════════════════════════════════
-- 6. User retention cohort (by first active month)
-- ═══════════════════════════════════════════════════════
WITH cohort AS (
    SELECT
        "Mac",
        DATE_TRUNC('month', first_active_date) AS cohort_month,
        active_days,
        days_since_first
    FROM user_profiles
)
SELECT
    TO_CHAR(cohort_month, 'YYYY-MM')  AS cohort,
    COUNT(*)                          AS cohort_size,
    ROUND(AVG(active_days), 1)        AS avg_active_days,
    SUM(CASE WHEN active_days >= 7  THEN 1 ELSE 0 END) AS retained_7d,
    SUM(CASE WHEN active_days >= 14 THEN 1 ELSE 0 END) AS retained_14d,
    SUM(CASE WHEN active_days >= 30 THEN 1 ELSE 0 END) AS retained_30d
FROM cohort
GROUP BY cohort_month
ORDER BY cohort_month;

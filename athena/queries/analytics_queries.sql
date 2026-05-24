-- =============================================================
-- Athena SQL Analytics Queries
-- =============================================================
-- Advanced analytical queries on S3 Parquet data via Athena.
-- All queries use partition pruning for cost optimization.
--
-- Author:  Data Engineering Team
-- Database: ecommerce_catalog
-- =============================================================


-- ─────────────────────────────────────────────
-- 1. TOP SELLING PRODUCTS (last 30 days)
-- ─────────────────────────────────────────────

SELECT
    item.product_id,
    item.product_name,
    item.category,
    item.brand,
    SUM(item.quantity)                                          AS units_sold,
    COUNT(DISTINCT o.order_id)                                  AS order_count,
    ROUND(SUM(item.final_price * item.quantity), 2)             AS total_revenue,
    ROUND(AVG(item.unit_price), 2)                              AS avg_unit_price,
    ROUND(AVG(item.discount_pct), 2)                            AS avg_discount_pct,
    DENSE_RANK() OVER (ORDER BY SUM(item.final_price * item.quantity) DESC) AS revenue_rank
FROM ecommerce_catalog.silver_orders o
CROSS JOIN UNNEST(o.items) AS t(item)
WHERE o.year  = YEAR(CURRENT_DATE)
  AND o.month >= MONTH(DATE_ADD('month', -1, CURRENT_DATE))
  AND o.order_status IN ('confirmed', 'shipped', 'delivered')
GROUP BY 1,2,3,4
ORDER BY total_revenue DESC
LIMIT 50;


-- ─────────────────────────────────────────────
-- 2. CONVERSION RATE BY CHANNEL & DEVICE
-- ─────────────────────────────────────────────

WITH sessions AS (
    SELECT
        utm_source,
        utm_medium,
        device_type,
        COUNT(DISTINCT session_id)  AS total_sessions,
        COUNT(DISTINCT user_id)     AS unique_users
    FROM ecommerce_catalog.silver_clickstream
    WHERE year  = YEAR(CURRENT_DATE)
      AND month = MONTH(CURRENT_DATE)
    GROUP BY 1,2,3
),
orders AS (
    SELECT
        utm_source,
        utm_medium,
        device_type,
        COUNT(DISTINCT session_id)          AS converting_sessions,
        COUNT(DISTINCT order_id)            AS total_orders,
        ROUND(SUM(total_amount), 2)         AS revenue
    FROM ecommerce_catalog.silver_orders
    WHERE year  = YEAR(CURRENT_DATE)
      AND month = MONTH(CURRENT_DATE)
      AND order_status IN ('confirmed', 'shipped', 'delivered')
    GROUP BY 1,2,3
)
SELECT
    s.utm_source,
    s.utm_medium,
    s.device_type,
    s.total_sessions,
    s.unique_users,
    COALESCE(o.total_orders, 0)             AS orders,
    COALESCE(o.revenue, 0)                  AS revenue,
    ROUND(
        CAST(COALESCE(o.converting_sessions, 0) AS DOUBLE)
        / NULLIF(s.total_sessions, 0) * 100, 2
    )                                       AS conversion_rate_pct,
    ROUND(
        COALESCE(o.revenue, 0)
        / NULLIF(COALESCE(o.total_orders, 0), 0), 2
    )                                       AS avg_order_value
FROM sessions s
LEFT JOIN orders o ON s.utm_source = o.utm_source
    AND s.utm_medium = o.utm_medium
    AND s.device_type = o.device_type
ORDER BY conversion_rate_pct DESC;


-- ─────────────────────────────────────────────
-- 3. HOURLY REVENUE TREND (today)
-- ─────────────────────────────────────────────

SELECT
    hour,
    country,
    COUNT(order_id)                         AS orders,
    COUNT(DISTINCT user_id)                 AS buyers,
    ROUND(SUM(total_amount), 2)             AS gross_revenue,
    ROUND(SUM(total_amount - discount_amount), 2) AS net_revenue,
    ROUND(AVG(total_amount), 2)             AS avg_order_value,
    ROUND(
        SUM(SUM(total_amount)) OVER (
            PARTITION BY country
            ORDER BY hour
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2
    )                                       AS cumulative_revenue
FROM ecommerce_catalog.silver_orders
WHERE year  = YEAR(CURRENT_DATE)
  AND month = MONTH(CURRENT_DATE)
  AND day   = DAY(CURRENT_DATE)
  AND order_status != 'cancelled'
GROUP BY 1,2
ORDER BY country, hour;


-- ─────────────────────────────────────────────
-- 4. ABANDONED CARTS ANALYSIS
-- ─────────────────────────────────────────────

WITH cart_adds AS (
    SELECT
        session_id,
        user_id,
        country,
        device_type,
        customer_segment,
        COUNT(DISTINCT product_id)          AS products_added,
        ROUND(SUM(cart_total), 2)           AS potential_revenue,
        MAX(event_timestamp)                AS last_activity
    FROM ecommerce_catalog.silver_carts
    WHERE action = 'item_added'
      AND year  = YEAR(CURRENT_DATE)
      AND month = MONTH(CURRENT_DATE)
    GROUP BY 1,2,3,4,5
),
converted AS (
    SELECT DISTINCT session_id
    FROM ecommerce_catalog.silver_orders
    WHERE year  = YEAR(CURRENT_DATE)
      AND month = MONTH(CURRENT_DATE)
)
SELECT
    c.country,
    c.device_type,
    c.customer_segment,
    COUNT(c.session_id)                     AS cart_sessions,
    COUNT(conv.session_id)                  AS converted_sessions,
    COUNT(c.session_id) - COUNT(conv.session_id) AS abandoned_carts,
    ROUND(
        CAST(COUNT(c.session_id) - COUNT(conv.session_id) AS DOUBLE)
        / NULLIF(COUNT(c.session_id), 0) * 100, 2
    )                                       AS abandonment_rate_pct,
    ROUND(
        SUM(CASE WHEN conv.session_id IS NULL THEN c.potential_revenue ELSE 0 END), 2
    )                                       AS lost_revenue
FROM cart_adds c
LEFT JOIN converted conv ON c.session_id = conv.session_id
GROUP BY 1,2,3
ORDER BY lost_revenue DESC;


-- ─────────────────────────────────────────────
-- 5. CUSTOMER RETENTION (Cohort Analysis)
-- ─────────────────────────────────────────────

WITH first_orders AS (
    SELECT
        user_id,
        DATE_TRUNC('month', MIN(event_timestamp))   AS cohort_month,
        DATE_FORMAT(MIN(event_timestamp), '%Y-%m')  AS cohort_label
    FROM ecommerce_catalog.silver_orders
    WHERE order_status IN ('confirmed', 'shipped', 'delivered')
    GROUP BY user_id
),
monthly_orders AS (
    SELECT
        o.user_id,
        DATE_TRUNC('month', o.event_timestamp)      AS order_month
    FROM ecommerce_catalog.silver_orders o
    WHERE order_status IN ('confirmed', 'shipped', 'delivered')
    GROUP BY o.user_id, DATE_TRUNC('month', o.event_timestamp)
)
SELECT
    f.cohort_label,
    DATE_DIFF('month',
        CAST(f.cohort_month AS TIMESTAMP),
        CAST(m.order_month AS TIMESTAMP)
    )                                               AS months_since_acquisition,
    COUNT(DISTINCT m.user_id)                       AS active_customers,
    COUNT(DISTINCT f.user_id)                       AS cohort_size,
    ROUND(
        CAST(COUNT(DISTINCT m.user_id) AS DOUBLE)
        / NULLIF(COUNT(DISTINCT f.user_id), 0) * 100, 2
    )                                               AS retention_rate_pct
FROM first_orders f
JOIN monthly_orders m ON f.user_id = m.user_id
WHERE DATE_DIFF('month',
    CAST(f.cohort_month AS TIMESTAMP),
    CAST(m.order_month AS TIMESTAMP)
) BETWEEN 0 AND 11
GROUP BY 1,2
ORDER BY cohort_label, months_since_acquisition;


-- ─────────────────────────────────────────────
-- 6. REAL-TIME ACTIVE USERS (last 15 minutes)
-- ─────────────────────────────────────────────

SELECT
    DATE_TRUNC('minute', event_timestamp)   AS minute_bucket,
    device_type,
    page_type,
    COUNT(DISTINCT session_id)              AS active_sessions,
    COUNT(DISTINCT user_id)                 AS active_users,
    COUNT(event_id)                         AS page_views,
    ROUND(AVG(time_on_page_seconds), 1)     AS avg_time_on_page,
    SUM(CASE WHEN is_bounce THEN 1 ELSE 0 END) AS bounces,
    ROUND(
        CAST(SUM(CASE WHEN is_bounce THEN 1 ELSE 0 END) AS DOUBLE)
        / NULLIF(COUNT(event_id), 0) * 100, 2
    )                                       AS bounce_rate_pct
FROM ecommerce_catalog.silver_clickstream
WHERE year  = YEAR(CURRENT_DATE)
  AND month = MONTH(CURRENT_DATE)
  AND day   = DAY(CURRENT_DATE)
  AND event_timestamp >= (NOW() - INTERVAL '15' MINUTE)
GROUP BY 1,2,3
ORDER BY minute_bucket DESC, active_users DESC;


-- ─────────────────────────────────────────────
-- 7. AVERAGE ORDER VALUE BY SEGMENT & COUNTRY
-- ─────────────────────────────────────────────

SELECT
    customer_segment,
    country,
    COUNT(DISTINCT user_id)                 AS customers,
    COUNT(order_id)                         AS orders,
    ROUND(AVG(total_amount), 2)             AS avg_order_value,
    ROUND(APPROX_PERCENTILE(total_amount, 0.25), 2) AS aov_p25,
    ROUND(APPROX_PERCENTILE(total_amount, 0.50), 2) AS aov_median,
    ROUND(APPROX_PERCENTILE(total_amount, 0.75), 2) AS aov_p75,
    ROUND(APPROX_PERCENTILE(total_amount, 0.95), 2) AS aov_p95,
    ROUND(SUM(total_amount), 2)             AS total_revenue,
    ROUND(AVG(item_count), 1)               AS avg_items_per_order
FROM ecommerce_catalog.silver_orders
WHERE year  = YEAR(CURRENT_DATE)
  AND order_status IN ('confirmed', 'shipped', 'delivered')
GROUP BY 1,2
HAVING COUNT(order_id) >= 10
ORDER BY avg_order_value DESC;


-- ─────────────────────────────────────────────
-- 8. PAYMENT SUCCESS RATE & FRAUD INDICATORS
-- ─────────────────────────────────────────────

SELECT
    payment_method,
    payment_gateway,
    country,
    COUNT(payment_id)                       AS total_transactions,
    COUNT_IF(payment_status = 'success')    AS successful,
    COUNT_IF(payment_status = 'failed')     AS failed,
    COUNT_IF(payment_status = 'declined')   AS declined,
    ROUND(
        CAST(COUNT_IF(payment_status = 'success') AS DOUBLE)
        / NULLIF(COUNT(payment_id), 0) * 100, 2
    )                                       AS success_rate_pct,
    ROUND(AVG(processing_time_ms), 0)       AS avg_processing_ms,
    ROUND(AVG(amount), 2)                   AS avg_transaction_amount,
    ROUND(SUM(CASE WHEN payment_status = 'success' THEN amount ELSE 0 END), 2) AS successful_volume,
    failure_reason,
    COUNT_IF(failure_reason = 'fraud_detected') AS fraud_count
FROM ecommerce_catalog.silver_payments
WHERE year  = YEAR(CURRENT_DATE)
  AND month = MONTH(CURRENT_DATE)
GROUP BY 1,2,3,11
ORDER BY total_transactions DESC;


-- ─────────────────────────────────────────────
-- 9. SEARCH QUERY PERFORMANCE
-- ─────────────────────────────────────────────

WITH search_events AS (
    SELECT
        LOWER(TRIM(search_query))           AS query,
        COUNT(event_id)                     AS search_count,
        COUNT(DISTINCT session_id)          AS searching_sessions,
        ROUND(AVG(search_result_count), 0)  AS avg_results,
        COUNT_IF(search_result_count = 0)   AS zero_result_searches
    FROM ecommerce_catalog.silver_clickstream
    WHERE page_type = 'search_results'
      AND search_query IS NOT NULL
      AND year  = YEAR(CURRENT_DATE)
      AND month = MONTH(CURRENT_DATE)
    GROUP BY 1
)
SELECT
    query,
    search_count,
    searching_sessions,
    avg_results,
    zero_result_searches,
    ROUND(
        CAST(zero_result_searches AS DOUBLE)
        / NULLIF(search_count, 0) * 100, 2
    )                                       AS zero_result_rate_pct,
    ROUND(
        CAST(search_count AS DOUBLE)
        / SUM(search_count) OVER () * 100, 3
    )                                       AS search_share_pct
FROM search_events
WHERE search_count >= 5
ORDER BY search_count DESC
LIMIT 100;


-- ─────────────────────────────────────────────
-- 10. DATA QUALITY MONITORING QUERY
-- ─────────────────────────────────────────────

SELECT
    'silver_orders'                         AS table_name,
    year, month, day,
    COUNT(*)                                AS total_records,
    COUNT_IF(order_id IS NULL)              AS null_order_ids,
    COUNT_IF(user_id IS NULL)               AS null_user_ids,
    COUNT_IF(total_amount <= 0)             AS invalid_amounts,
    COUNT_IF(total_amount > 10000)          AS suspicious_amounts,
    COUNT_IF(country IS NULL OR country = 'UNKNOWN') AS unknown_country,
    ROUND(AVG(total_amount), 2)             AS avg_order_value,
    MIN(event_timestamp)                    AS earliest_event,
    MAX(event_timestamp)                    AS latest_event,
    APPROX_COUNT_DISTINCT(order_id)         AS approx_distinct_orders,
    APPROX_COUNT_DISTINCT(user_id)          AS approx_distinct_users,
    CURRENT_TIMESTAMP                       AS report_generated_at
FROM ecommerce_catalog.silver_orders
WHERE year  = YEAR(CURRENT_DATE)
  AND month = MONTH(CURRENT_DATE)
GROUP BY 2,3,4
ORDER BY year DESC, month DESC, day DESC;

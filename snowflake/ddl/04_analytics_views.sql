-- =============================================================
-- Snowflake: Analytical Views for Power BI
-- =============================================================
-- Pre-joined, pre-filtered views that Power BI connects to
-- directly — avoids complex DAX joins and speeds up dashboards.
--
-- Author:  Data Engineering Team
-- Version: 1.0.0
-- =============================================================

USE DATABASE ECOMMERCE_DW;
USE SCHEMA ANALYTICS;
USE WAREHOUSE REPORTING_WH;


-- ─────────────────────────────────────────────
-- VW_ORDERS_ENRICHED
-- Full order details with dim lookups
-- ─────────────────────────────────────────────

CREATE OR REPLACE VIEW ECOMMERCE_DW.ANALYTICS.VW_ORDERS_ENRICHED AS
SELECT
    fo.ORDER_ID,
    fo.EVENT_ID,
    fo.USER_ID,
    fo.SESSION_ID,
    fo.ORDER_TIMESTAMP,
    fo.ORDER_STATUS,
    fo.PAYMENT_METHOD,
    fo.CURRENCY,
    fo.ITEM_COUNT,
    fo.ORDER_TOTAL,
    fo.SUBTOTAL,
    fo.TAX_AMOUNT,
    fo.SHIPPING_COST,
    fo.DISCOUNT_AMOUNT,
    fo.EFFECTIVE_DISCOUNT_PCT,
    fo.ORDER_VALUE_TIER,
    fo.PRIMARY_CATEGORY,
    fo.DISTINCT_PRODUCTS,
    fo.IS_PRIME,
    fo.IS_GIFT,
    fo.IS_WEEKEND,
    fo.ORDER_HOUR_BUCKET,
    fo.UTM_SOURCE,
    fo.UTM_MEDIUM,
    -- From DIM_USERS
    du.CUSTOMER_SEGMENT,
    du.TOTAL_ORDERS        AS CUSTOMER_TOTAL_ORDERS,
    du.TOTAL_SPEND         AS CUSTOMER_TOTAL_SPEND,
    du.AVG_ORDER_VALUE     AS CUSTOMER_AOV,
    du.CHURN_RISK_SCORE,
    du.LTV_SCORE,
    du.PREFERRED_CATEGORY,
    du.IS_ACTIVE           AS CUSTOMER_IS_ACTIVE,
    -- From DIM_DEVICES
    dd.DEVICE_TYPE,
    dd.DEVICE_OS,
    dd.BROWSER,
    dd.DEVICE_CLASS,
    dd.IS_MOBILE,
    -- From DIM_TIME
    dt.FULL_DATE           AS ORDER_DATE,
    dt.YEAR,
    dt.MONTH,
    dt.DAY,
    dt.WEEK_OF_YEAR,
    dt.DAY_NAME,
    dt.MONTH_NAME,
    dt.QUARTER,
    -- Derived
    fo.COUNTRY,
    CASE
        WHEN fo.ORDER_STATUS IN ('confirmed','shipped','delivered') THEN TRUE
        ELSE FALSE
    END                    AS IS_COMPLETED,
    fo.LOADED_AT
FROM ECOMMERCE_DW.ANALYTICS.FACT_ORDERS fo
LEFT JOIN ECOMMERCE_DW.ANALYTICS.DIM_USERS    du ON fo.USER_SK   = du.USER_SK
LEFT JOIN ECOMMERCE_DW.ANALYTICS.DIM_DEVICES  dd ON fo.DEVICE_SK = dd.DEVICE_SK
LEFT JOIN ECOMMERCE_DW.ANALYTICS.DIM_TIME     dt ON fo.DATE_KEY  = dt.DATE_KEY;


-- ─────────────────────────────────────────────
-- VW_REVENUE_BY_DAY
-- Daily revenue KPIs — Executive page
-- ─────────────────────────────────────────────

CREATE OR REPLACE VIEW ECOMMERCE_DW.ANALYTICS.VW_REVENUE_BY_DAY AS
SELECT
    dt.FULL_DATE                               AS ORDER_DATE,
    dt.YEAR,
    dt.MONTH,
    dt.MONTH_NAME,
    dt.QUARTER,
    fo.COUNTRY,
    fo.PAYMENT_METHOD,
    fo.ORDER_VALUE_TIER,
    -- Volumes
    COUNT(fo.ORDER_ID)                         AS TOTAL_ORDERS,
    COUNT_IF(fo.IS_WEEKEND)                    AS WEEKEND_ORDERS,
    COUNT(DISTINCT fo.USER_ID)                 AS UNIQUE_BUYERS,
    SUM(fo.ITEM_COUNT)                         AS TOTAL_UNITS,
    -- Revenue
    ROUND(SUM(fo.ORDER_TOTAL), 2)              AS GROSS_REVENUE,
    ROUND(SUM(fo.ORDER_TOTAL - fo.DISCOUNT_AMOUNT), 2) AS NET_REVENUE,
    ROUND(SUM(fo.DISCOUNT_AMOUNT), 2)          AS TOTAL_DISCOUNTS,
    ROUND(SUM(fo.SHIPPING_COST), 2)            AS TOTAL_SHIPPING,
    ROUND(SUM(fo.TAX_AMOUNT), 2)               AS TOTAL_TAX,
    -- KPIs
    ROUND(AVG(fo.ORDER_TOTAL), 2)              AS AOV,
    ROUND(MEDIAN(fo.ORDER_TOTAL), 2)           AS MEDIAN_ORDER_VALUE,
    ROUND(SUM(fo.ORDER_TOTAL) / NULLIF(COUNT(DISTINCT fo.USER_ID), 0), 2) AS REVENUE_PER_CUSTOMER,
    -- MoM comparison
    ROUND(SUM(fo.ORDER_TOTAL) - LAG(SUM(fo.ORDER_TOTAL), 30)
          OVER (PARTITION BY fo.COUNTRY ORDER BY dt.FULL_DATE), 2) AS REVENUE_DELTA_30D
FROM ECOMMERCE_DW.ANALYTICS.FACT_ORDERS fo
JOIN ECOMMERCE_DW.ANALYTICS.DIM_TIME dt ON fo.DATE_KEY = dt.DATE_KEY
WHERE fo.ORDER_STATUS IN ('confirmed','shipped','delivered')
GROUP BY 1,2,3,4,5,6,7,8;


-- ─────────────────────────────────────────────
-- VW_PRODUCT_SALES
-- Product performance for the Product page
-- ─────────────────────────────────────────────

CREATE OR REPLACE VIEW ECOMMERCE_DW.ANALYTICS.VW_PRODUCT_SALES AS
SELECT
    dp.PRODUCT_ID,
    dp.PRODUCT_NAME,
    dp.CATEGORY,
    dp.BRAND,
    dp.PRICE_TIER,
    dt.YEAR,
    dt.MONTH,
    dt.MONTH_NAME,
    dt.QUARTER,
    fo.COUNTRY,
    fo.DEVICE_TYPE,
    COUNT(fo.ORDER_ID)                         AS ORDER_COUNT,
    SUM(fo.ITEM_COUNT)                         AS UNITS_SOLD,
    ROUND(SUM(fo.ORDER_TOTAL), 2)              AS TOTAL_REVENUE,
    ROUND(AVG(fo.ORDER_TOTAL / NULLIF(fo.ITEM_COUNT, 0)), 2) AS AVG_ITEM_PRICE,
    ROUND(AVG(fo.EFFECTIVE_DISCOUNT_PCT), 2)   AS AVG_DISCOUNT_PCT,
    RANK() OVER (
        PARTITION BY dt.YEAR, dt.MONTH, dp.CATEGORY
        ORDER BY SUM(fo.ORDER_TOTAL) DESC
    )                                          AS CATEGORY_RANK,
    RANK() OVER (
        PARTITION BY dt.YEAR, dt.MONTH
        ORDER BY SUM(fo.ORDER_TOTAL) DESC
    )                                          AS OVERALL_RANK
FROM ECOMMERCE_DW.ANALYTICS.FACT_ORDERS fo
JOIN ECOMMERCE_DW.ANALYTICS.DIM_PRODUCTS dp
    ON fo.PRIMARY_CATEGORY = dp.CATEGORY          -- join on category (simplified)
JOIN ECOMMERCE_DW.ANALYTICS.DIM_TIME dt ON fo.DATE_KEY = dt.DATE_KEY
WHERE fo.ORDER_STATUS IN ('confirmed','shipped','delivered')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11;


-- ─────────────────────────────────────────────
-- VW_CONVERSION_FUNNEL
-- Session → cart → order funnel by segment
-- ─────────────────────────────────────────────

CREATE OR REPLACE VIEW ECOMMERCE_DW.ANALYTICS.VW_CONVERSION_FUNNEL AS
WITH sessions AS (
    SELECT
        fc.COUNTRY,
        fc.DEVICE_TYPE,
        fc.UTM_SOURCE,
        fc.CUSTOMER_SEGMENT,
        dt.FULL_DATE                              AS SESSION_DATE,
        COUNT(DISTINCT fc.SESSION_ID)             AS TOTAL_SESSIONS,
        COUNT(DISTINCT fc.USER_ID)                AS UNIQUE_USERS,
        COUNT(fc.EVENT_ID)                        AS PAGE_VIEWS,
        COUNT_IF(fc.IS_BOUNCE)                    AS BOUNCES,
        ROUND(AVG(fc.TIME_ON_PAGE_SECONDS), 1)    AS AVG_TIME_ON_PAGE
    FROM ECOMMERCE_DW.ANALYTICS.FACT_CLICKSTREAM fc
    JOIN ECOMMERCE_DW.ANALYTICS.DIM_TIME dt ON fc.DATE_KEY = dt.DATE_KEY
    GROUP BY 1,2,3,4,5
),
conversions AS (
    SELECT
        fo.COUNTRY,
        fo.DEVICE_TYPE,
        fo.UTM_SOURCE,
        fo.CUSTOMER_SEGMENT,
        dt.FULL_DATE                              AS ORDER_DATE,
        COUNT(DISTINCT fo.SESSION_ID)             AS CONVERTING_SESSIONS,
        COUNT(fo.ORDER_ID)                        AS ORDERS,
        ROUND(SUM(fo.ORDER_TOTAL), 2)             AS REVENUE
    FROM ECOMMERCE_DW.ANALYTICS.FACT_ORDERS fo
    JOIN ECOMMERCE_DW.ANALYTICS.DIM_TIME dt ON fo.DATE_KEY = dt.DATE_KEY
    WHERE fo.ORDER_STATUS IN ('confirmed','shipped','delivered')
    GROUP BY 1,2,3,4,5
)
SELECT
    s.SESSION_DATE,
    s.COUNTRY,
    s.DEVICE_TYPE,
    s.UTM_SOURCE,
    s.CUSTOMER_SEGMENT,
    s.TOTAL_SESSIONS,
    s.UNIQUE_USERS,
    s.PAGE_VIEWS,
    s.BOUNCES,
    s.AVG_TIME_ON_PAGE,
    ROUND(s.BOUNCES / NULLIF(s.TOTAL_SESSIONS, 0) * 100, 2)       AS BOUNCE_RATE_PCT,
    COALESCE(c.CONVERTING_SESSIONS, 0)                              AS CONVERTING_SESSIONS,
    COALESCE(c.ORDERS, 0)                                           AS ORDERS,
    COALESCE(c.REVENUE, 0)                                          AS REVENUE,
    ROUND(COALESCE(c.CONVERTING_SESSIONS, 0)
          / NULLIF(s.TOTAL_SESSIONS, 0) * 100, 2)                  AS CONVERSION_RATE_PCT,
    ROUND(COALESCE(c.REVENUE, 0)
          / NULLIF(COALESCE(c.ORDERS, 0), 0), 2)                   AS AOV
FROM sessions s
LEFT JOIN conversions c
    ON  s.SESSION_DATE    = c.ORDER_DATE
    AND s.COUNTRY         = c.COUNTRY
    AND s.DEVICE_TYPE     = c.DEVICE_TYPE
    AND s.UTM_SOURCE      = c.UTM_SOURCE
    AND s.CUSTOMER_SEGMENT = c.CUSTOMER_SEGMENT;


-- ─────────────────────────────────────────────
-- VW_PAYMENT_ANALYTICS
-- Payment success / failure analysis
-- ─────────────────────────────────────────────

CREATE OR REPLACE VIEW ECOMMERCE_DW.ANALYTICS.VW_PAYMENT_ANALYTICS AS
SELECT
    dt.FULL_DATE              AS PAYMENT_DATE,
    dt.YEAR,
    dt.MONTH,
    fp.PAYMENT_METHOD,
    fp.PAYMENT_GATEWAY,
    fp.PAYMENT_STATUS,
    fp.FAILURE_REASON,
    fp.COUNTRY,
    COUNT(fp.PAYMENT_ID)      AS TRANSACTIONS,
    ROUND(SUM(fp.AMOUNT), 2)  AS VOLUME,
    ROUND(AVG(fp.AMOUNT), 2)  AS AVG_AMOUNT,
    ROUND(AVG(fp.PROCESSING_TIME_MS), 0) AS AVG_PROCESSING_MS,
    COUNT_IF(fp.IS_3DS_AUTHENTICATED)    AS AUTHENTICATED_3DS,
    COUNT_IF(fp.PAYMENT_STATUS = 'success') AS SUCCESSFUL,
    COUNT_IF(fp.PAYMENT_STATUS IN ('failed','declined')) AS FAILED,
    ROUND(
        COUNT_IF(fp.PAYMENT_STATUS = 'success')
        / NULLIF(COUNT(fp.PAYMENT_ID), 0) * 100, 2
    )                         AS SUCCESS_RATE_PCT
FROM ECOMMERCE_DW.ANALYTICS.FACT_PAYMENTS fp
JOIN ECOMMERCE_DW.ANALYTICS.DIM_TIME dt ON fp.DATE_KEY = dt.DATE_KEY
GROUP BY 1,2,3,4,5,6,7,8;


-- ─────────────────────────────────────────────
-- VW_USER_SEGMENTS
-- Customer segmentation for User Behavior page
-- ─────────────────────────────────────────────

CREATE OR REPLACE VIEW ECOMMERCE_DW.ANALYTICS.VW_USER_SEGMENTS AS
SELECT
    du.USER_ID,
    du.CUSTOMER_SEGMENT,
    du.COUNTRY,
    du.IS_PRIME,
    du.TOTAL_ORDERS,
    du.TOTAL_SPEND,
    du.AVG_ORDER_VALUE,
    du.FIRST_ORDER_DATE,
    du.LAST_ORDER_DATE,
    du.DAYS_SINCE_LAST_ORDER,
    du.PREFERRED_CATEGORY,
    du.CHURN_RISK_SCORE,
    du.LTV_SCORE,
    CASE
        WHEN du.CHURN_RISK_SCORE >= 0.65 THEN 'High'
        WHEN du.CHURN_RISK_SCORE >= 0.35 THEN 'Medium'
        ELSE 'Low'
    END AS CHURN_RISK_BAND,
    CASE
        WHEN du.LTV_SCORE >= 1000 THEN 'Champion'
        WHEN du.LTV_SCORE >= 500  THEN 'Loyal'
        WHEN du.LTV_SCORE >= 200  THEN 'Potential'
        ELSE 'New'
    END AS LTV_BAND,
    DATEDIFF('day', du.FIRST_ORDER_DATE, du.LAST_ORDER_DATE) AS CUSTOMER_LIFESPAN_DAYS
FROM ECOMMERCE_DW.ANALYTICS.DIM_USERS du
WHERE du.IS_ACTIVE = TRUE;


-- ─────────────────────────────────────────────
-- VW_HOURLY_ACTIVITY
-- Real-time activity feed (Page 3 of dashboard)
-- ─────────────────────────────────────────────

CREATE OR REPLACE VIEW ECOMMERCE_DW.ANALYTICS.VW_HOURLY_ACTIVITY AS
SELECT
    DATE_TRUNC('hour', fo.ORDER_TIMESTAMP)  AS HOUR_BUCKET,
    fo.COUNTRY,
    fo.DEVICE_TYPE,
    COUNT(fo.ORDER_ID)                      AS ORDERS,
    COUNT(DISTINCT fo.USER_ID)              AS BUYERS,
    ROUND(SUM(fo.ORDER_TOTAL), 2)           AS REVENUE,
    ROUND(AVG(fo.ORDER_TOTAL), 2)           AS AOV,
    ROUND(SUM(SUM(fo.ORDER_TOTAL)) OVER (
        PARTITION BY fo.COUNTRY
        ORDER BY DATE_TRUNC('hour', fo.ORDER_TIMESTAMP)
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2)                                   AS CUMULATIVE_REVENUE
FROM ECOMMERCE_DW.ANALYTICS.FACT_ORDERS fo
WHERE fo.ORDER_STATUS IN ('confirmed','shipped','delivered')
  AND fo.ORDER_TIMESTAMP >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1,2,3;

-- =============================================================
-- Snowflake DML: Incremental Load & Upsert Procedures
-- =============================================================
-- MERGE statements, stored procedures, and incremental loading
-- logic for all fact and dimension tables.
--
-- Author:  Data Engineering Team
-- Version: 1.0.0
-- =============================================================

USE DATABASE ECOMMERCE_DW;
USE SCHEMA ANALYTICS;
USE WAREHOUSE LOADING_WH;


-- =============================================================
-- STAGING TABLES (temp landing zone from S3)
-- =============================================================

CREATE TABLE IF NOT EXISTS ECOMMERCE_DW.RAW.STG_FACT_ORDERS
    LIKE ECOMMERCE_DW.ANALYTICS.FACT_ORDERS;

CREATE TABLE IF NOT EXISTS ECOMMERCE_DW.RAW.STG_FACT_PAYMENTS
    LIKE ECOMMERCE_DW.ANALYTICS.FACT_PAYMENTS;

CREATE TABLE IF NOT EXISTS ECOMMERCE_DW.RAW.STG_FACT_CLICKSTREAM
    LIKE ECOMMERCE_DW.ANALYTICS.FACT_CLICKSTREAM;

CREATE TABLE IF NOT EXISTS ECOMMERCE_DW.RAW.STG_DIM_USERS
    LIKE ECOMMERCE_DW.ANALYTICS.DIM_USERS;

CREATE TABLE IF NOT EXISTS ECOMMERCE_DW.RAW.STG_DIM_PRODUCTS
    LIKE ECOMMERCE_DW.ANALYTICS.DIM_PRODUCTS;


-- =============================================================
-- COPY INTO: Load from S3 Parquet into Staging
-- =============================================================

-- Load orders
COPY INTO ECOMMERCE_DW.RAW.STG_FACT_ORDERS
FROM (
    SELECT
        MD5($1:order_id::STRING)                              AS ORDER_SK,
        $1:order_id::STRING                                   AS ORDER_ID,
        $1:event_id::STRING                                   AS EVENT_ID,
        MD5($1:user_id::STRING)                               AS USER_SK,
        MD5(CONCAT_WS('|', $1:device_type::STRING,
                           $1:device_os::STRING,
                           $1:browser::STRING))               AS DEVICE_SK,
        TO_NUMBER(TO_CHAR($1:event_timestamp::TIMESTAMP_NTZ, 'YYYYMMDD')) AS DATE_KEY,
        $1:user_id::STRING                                    AS USER_ID,
        $1:session_id::STRING                                 AS SESSION_ID,
        $1:country::STRING                                    AS COUNTRY,
        COALESCE($1:currency::STRING, 'USD')                  AS CURRENCY,
        $1:payment_method::STRING                             AS PAYMENT_METHOD,
        $1:order_status::STRING                               AS ORDER_STATUS,
        $1:event_timestamp::TIMESTAMP_NTZ                     AS ORDER_TIMESTAMP,
        $1:item_count::INT                                    AS ITEM_COUNT,
        $1:total_amount::FLOAT                                AS ORDER_TOTAL,
        $1:subtotal::FLOAT                                    AS SUBTOTAL,
        $1:tax_amount::FLOAT                                  AS TAX_AMOUNT,
        $1:shipping_cost::FLOAT                               AS SHIPPING_COST,
        $1:discount_amount::FLOAT                             AS DISCOUNT_AMOUNT,
        $1:effective_discount_pct::FLOAT                      AS EFFECTIVE_DISCOUNT_PCT,
        $1:order_value_tier::STRING                           AS ORDER_VALUE_TIER,
        $1:primary_category::STRING                           AS PRIMARY_CATEGORY,
        $1:distinct_products::INT                             AS DISTINCT_PRODUCTS,
        $1:customer_segment::STRING                           AS CUSTOMER_SEGMENT,
        $1:utm_source::STRING                                 AS UTM_SOURCE,
        $1:utm_medium::STRING                                 AS UTM_MEDIUM,
        $1:is_prime::BOOLEAN                                  AS IS_PRIME,
        $1:is_gift::BOOLEAN                                   AS IS_GIFT,
        $1:is_weekend::BOOLEAN                                AS IS_WEEKEND,
        $1:order_hour_bucket::STRING                          AS ORDER_HOUR_BUCKET,
        CURRENT_TIMESTAMP()                                   AS LOADED_AT
    FROM @ECOMMERCE_DW.RAW.GOLD_STAGE/fact_orders/
)
FILE_FORMAT = (FORMAT_NAME = ECOMMERCE_DW.RAW.PARQUET_FORMAT)
ON_ERROR = 'SKIP_FILE';


-- =============================================================
-- MERGE PROCEDURE: FACT_ORDERS
-- =============================================================

CREATE OR REPLACE PROCEDURE ECOMMERCE_DW.ANALYTICS.SP_MERGE_FACT_ORDERS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_inserted INT := 0;
    rows_updated  INT := 0;
    start_ts      TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
BEGIN
    MERGE INTO ECOMMERCE_DW.ANALYTICS.FACT_ORDERS tgt
    USING ECOMMERCE_DW.RAW.STG_FACT_ORDERS src
        ON tgt.ORDER_ID = src.ORDER_ID
    WHEN MATCHED AND src.ORDER_STATUS != tgt.ORDER_STATUS THEN
        UPDATE SET
            ORDER_STATUS    = src.ORDER_STATUS,
            ORDER_TOTAL     = src.ORDER_TOTAL,
            LOADED_AT       = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            ORDER_SK, ORDER_ID, EVENT_ID, USER_SK, DEVICE_SK, DATE_KEY,
            USER_ID, SESSION_ID, COUNTRY, CURRENCY, PAYMENT_METHOD,
            ORDER_STATUS, ORDER_TIMESTAMP, ITEM_COUNT, ORDER_TOTAL,
            SUBTOTAL, TAX_AMOUNT, SHIPPING_COST, DISCOUNT_AMOUNT,
            EFFECTIVE_DISCOUNT_PCT, ORDER_VALUE_TIER, PRIMARY_CATEGORY,
            DISTINCT_PRODUCTS, CUSTOMER_SEGMENT, UTM_SOURCE, UTM_MEDIUM,
            IS_PRIME, IS_GIFT, IS_WEEKEND, ORDER_HOUR_BUCKET, LOADED_AT
        )
        VALUES (
            src.ORDER_SK, src.ORDER_ID, src.EVENT_ID, src.USER_SK, src.DEVICE_SK, src.DATE_KEY,
            src.USER_ID, src.SESSION_ID, src.COUNTRY, src.CURRENCY, src.PAYMENT_METHOD,
            src.ORDER_STATUS, src.ORDER_TIMESTAMP, src.ITEM_COUNT, src.ORDER_TOTAL,
            src.SUBTOTAL, src.TAX_AMOUNT, src.SHIPPING_COST, src.DISCOUNT_AMOUNT,
            src.EFFECTIVE_DISCOUNT_PCT, src.ORDER_VALUE_TIER, src.PRIMARY_CATEGORY,
            src.DISTINCT_PRODUCTS, src.CUSTOMER_SEGMENT, src.UTM_SOURCE, src.UTM_MEDIUM,
            src.IS_PRIME, src.IS_GIFT, src.IS_WEEKEND, src.ORDER_HOUR_BUCKET, CURRENT_TIMESTAMP()
        );

    -- Truncate staging after successful merge
    TRUNCATE TABLE ECOMMERCE_DW.RAW.STG_FACT_ORDERS;

    RETURN 'Merge complete at ' || CURRENT_TIMESTAMP()::STRING;
END;
$$;


-- =============================================================
-- MERGE PROCEDURE: DIM_USERS (SCD Type 1)
-- =============================================================

CREATE OR REPLACE PROCEDURE ECOMMERCE_DW.ANALYTICS.SP_MERGE_DIM_USERS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Build enriched user snapshot from fact_orders
    CREATE OR REPLACE TEMP TABLE TMP_USER_METRICS AS
    SELECT
        USER_ID,
        COUNT(DISTINCT ORDER_ID)                        AS TOTAL_ORDERS,
        ROUND(SUM(ORDER_TOTAL), 2)                      AS TOTAL_SPEND,
        ROUND(AVG(ORDER_TOTAL), 2)                      AS AVG_ORDER_VALUE,
        MIN(ORDER_TIMESTAMP::DATE)                      AS FIRST_ORDER_DATE,
        MAX(ORDER_TIMESTAMP::DATE)                      AS LAST_ORDER_DATE,
        DATEDIFF('day', MAX(ORDER_TIMESTAMP::DATE), CURRENT_DATE()) AS DAYS_SINCE_LAST_ORDER,
        MODE(PRIMARY_CATEGORY)                          AS PREFERRED_CATEGORY,
        MAX(COUNTRY)                                    AS COUNTRY,
        MAX(CUSTOMER_SEGMENT)                           AS CUSTOMER_SEGMENT,
        MAX(IS_PRIME)                                   AS IS_PRIME
    FROM ECOMMERCE_DW.ANALYTICS.FACT_ORDERS
    GROUP BY USER_ID;

    MERGE INTO ECOMMERCE_DW.ANALYTICS.DIM_USERS tgt
    USING (
        SELECT
            MD5(s.USER_ID)                         AS USER_SK,
            s.USER_ID,
            s.COUNTRY,
            s.CUSTOMER_SEGMENT,
            s.IS_PRIME,
            s.FIRST_ORDER_DATE,
            s.LAST_ORDER_DATE,
            s.TOTAL_ORDERS,
            s.TOTAL_SPEND,
            s.AVG_ORDER_VALUE,
            s.PREFERRED_CATEGORY,
            s.DAYS_SINCE_LAST_ORDER,
            -- Churn risk: higher score = higher risk
            CASE
                WHEN s.DAYS_SINCE_LAST_ORDER > 180 THEN 0.90
                WHEN s.DAYS_SINCE_LAST_ORDER > 90  THEN 0.65
                WHEN s.DAYS_SINCE_LAST_ORDER > 45  THEN 0.35
                WHEN s.DAYS_SINCE_LAST_ORDER > 21  THEN 0.15
                ELSE 0.05
            END AS CHURN_RISK_SCORE,
            -- LTV: simplified RFM-based scoring
            ROUND(
                (s.TOTAL_SPEND * 0.5) +
                (s.TOTAL_ORDERS * 10) +
                (CASE WHEN s.DAYS_SINCE_LAST_ORDER < 30 THEN 100 ELSE 0 END),
            2) AS LTV_SCORE
        FROM TMP_USER_METRICS s
    ) src ON tgt.USER_ID = src.USER_ID
    WHEN MATCHED THEN
        UPDATE SET
            COUNTRY               = src.COUNTRY,
            CUSTOMER_SEGMENT      = src.CUSTOMER_SEGMENT,
            IS_PRIME              = src.IS_PRIME,
            LAST_ORDER_DATE       = src.LAST_ORDER_DATE,
            TOTAL_ORDERS          = src.TOTAL_ORDERS,
            TOTAL_SPEND           = src.TOTAL_SPEND,
            AVG_ORDER_VALUE       = src.AVG_ORDER_VALUE,
            PREFERRED_CATEGORY    = src.PREFERRED_CATEGORY,
            DAYS_SINCE_LAST_ORDER = src.DAYS_SINCE_LAST_ORDER,
            CHURN_RISK_SCORE      = src.CHURN_RISK_SCORE,
            LTV_SCORE             = src.LTV_SCORE,
            UPDATED_AT            = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            USER_SK, USER_ID, COUNTRY, CUSTOMER_SEGMENT, IS_PRIME,
            FIRST_ORDER_DATE, LAST_ORDER_DATE, TOTAL_ORDERS, TOTAL_SPEND,
            AVG_ORDER_VALUE, PREFERRED_CATEGORY, DAYS_SINCE_LAST_ORDER,
            CHURN_RISK_SCORE, LTV_SCORE, IS_ACTIVE, CREATED_AT, UPDATED_AT
        )
        VALUES (
            src.USER_SK, src.USER_ID, src.COUNTRY, src.CUSTOMER_SEGMENT, src.IS_PRIME,
            src.FIRST_ORDER_DATE, src.LAST_ORDER_DATE, src.TOTAL_ORDERS, src.TOTAL_SPEND,
            src.AVG_ORDER_VALUE, src.PREFERRED_CATEGORY, src.DAYS_SINCE_LAST_ORDER,
            src.CHURN_RISK_SCORE, src.LTV_SCORE, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        );

    RETURN 'DIM_USERS merge complete: ' || CURRENT_TIMESTAMP()::STRING;
END;
$$;


-- =============================================================
-- REPORTING REFRESH PROCEDURE
-- =============================================================

CREATE OR REPLACE PROCEDURE ECOMMERCE_DW.ANALYTICS.SP_REFRESH_REPORTING()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Daily Revenue
    CREATE OR REPLACE TABLE ECOMMERCE_DW.REPORTING.RPT_DAILY_REVENUE AS
    SELECT
        ORDER_TIMESTAMP::DATE                       AS REPORT_DATE,
        COUNTRY,
        DEVICE_TYPE,  -- needs join to dim_devices if needed
        PAYMENT_METHOD,
        CUSTOMER_SEGMENT,
        COUNT(ORDER_ID)                             AS TOTAL_ORDERS,
        COUNT(DISTINCT USER_ID)                     AS UNIQUE_CUSTOMERS,
        ROUND(SUM(ORDER_TOTAL), 2)                  AS GROSS_REVENUE,
        ROUND(SUM(ORDER_TOTAL - DISCOUNT_AMOUNT), 2) AS NET_REVENUE,
        ROUND(SUM(DISCOUNT_AMOUNT), 2)              AS TOTAL_DISCOUNTS,
        ROUND(AVG(ORDER_TOTAL), 2)                  AS AVG_ORDER_VALUE,
        ROUND(SUM(SHIPPING_COST), 2)                AS TOTAL_SHIPPING,
        ROUND(SUM(TAX_AMOUNT), 2)                   AS TOTAL_TAX,
        ROUND(SUM(ORDER_TOTAL) / NULLIF(COUNT(DISTINCT USER_ID), 0), 2) AS REVENUE_PER_CUSTOMER,
        CURRENT_TIMESTAMP()                         AS UPDATED_AT
    FROM ECOMMERCE_DW.ANALYTICS.FACT_ORDERS
    WHERE ORDER_STATUS IN ('confirmed', 'shipped', 'delivered')
    GROUP BY 1,2,3,4,5;

    -- Product Performance
    CREATE OR REPLACE TABLE ECOMMERCE_DW.REPORTING.RPT_PRODUCT_PERFORMANCE AS
    SELECT
        YEAR(fo.ORDER_TIMESTAMP)        AS YEAR,
        MONTH(fo.ORDER_TIMESTAMP)       AS MONTH,
        dp.PRODUCT_ID,
        dp.PRODUCT_NAME,
        dp.CATEGORY,
        dp.BRAND,
        SUM(fo.ITEM_COUNT)              AS UNITS_SOLD,
        COUNT(fo.ORDER_ID)              AS ORDER_COUNT,
        ROUND(SUM(fo.ORDER_TOTAL), 2)   AS TOTAL_REVENUE,
        ROUND(AVG(fo.ORDER_TOTAL / NULLIF(fo.ITEM_COUNT, 0)), 2) AS AVG_SELLING_PRICE,
        ROUND(AVG(fo.EFFECTIVE_DISCOUNT_PCT), 2) AS AVG_DISCOUNT_PCT,
        RANK() OVER (
            PARTITION BY YEAR(fo.ORDER_TIMESTAMP), MONTH(fo.ORDER_TIMESTAMP), dp.CATEGORY
            ORDER BY SUM(fo.ORDER_TOTAL) DESC
        ) AS REVENUE_RANK,
        CURRENT_TIMESTAMP()             AS UPDATED_AT
    FROM ECOMMERCE_DW.ANALYTICS.FACT_ORDERS fo
    JOIN ECOMMERCE_DW.ANALYTICS.DIM_PRODUCTS dp ON fo.PRIMARY_CATEGORY = dp.CATEGORY
    WHERE fo.ORDER_STATUS IN ('confirmed', 'shipped', 'delivered')
    GROUP BY 1,2,3,4,5,6;

    RETURN 'Reporting tables refreshed at ' || CURRENT_TIMESTAMP()::STRING;
END;
$$;


-- =============================================================
-- MONITORING VIEWS
-- =============================================================

CREATE OR REPLACE VIEW ECOMMERCE_DW.ANALYTICS.VW_PIPELINE_HEALTH AS
SELECT
    'fact_orders'         AS table_name,
    COUNT(*)              AS total_rows,
    MAX(LOADED_AT)        AS last_load,
    DATEDIFF('minute', MAX(LOADED_AT), CURRENT_TIMESTAMP()) AS minutes_since_load,
    COUNT_IF(LOADED_AT >= DATEADD('hour', -1, CURRENT_TIMESTAMP())) AS rows_last_hour
FROM ECOMMERCE_DW.ANALYTICS.FACT_ORDERS
UNION ALL
SELECT 'fact_payments', COUNT(*), MAX(LOADED_AT),
    DATEDIFF('minute', MAX(LOADED_AT), CURRENT_TIMESTAMP()),
    COUNT_IF(LOADED_AT >= DATEADD('hour', -1, CURRENT_TIMESTAMP()))
FROM ECOMMERCE_DW.ANALYTICS.FACT_PAYMENTS
UNION ALL
SELECT 'fact_clickstream', COUNT(*), MAX(LOADED_AT),
    DATEDIFF('minute', MAX(LOADED_AT), CURRENT_TIMESTAMP()),
    COUNT_IF(LOADED_AT >= DATEADD('hour', -1, CURRENT_TIMESTAMP()))
FROM ECOMMERCE_DW.ANALYTICS.FACT_CLICKSTREAM;

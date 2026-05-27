-- =============================================================
-- Snowflake: Snowpipe Auto-Ingest + Full Load Orchestration
-- =============================================================
-- Configures Snowpipe for continuous S3→Snowflake ingestion
-- and stored procedures for full load orchestration.
--
-- Author:  Data Engineering Team
-- Version: 1.0.0
-- =============================================================

USE DATABASE ECOMMERCE_DW;
USE SCHEMA RAW;
USE WAREHOUSE LOADING_WH;

-- ─────────────────────────────────────────────
-- SQS Notification-Based Snowpipe (auto-ingest)
-- ─────────────────────────────────────────────

-- Pipe for fact_orders (continuous ingest from Gold S3)
CREATE PIPE IF NOT EXISTS ECOMMERCE_DW.RAW.PIPE_FACT_ORDERS
    AUTO_INGEST = TRUE
    ERROR_INTEGRATION = ECOMMERCE_SNS_ERROR_INTEGRATION
    COMMENT = 'Auto-ingest pipe for Gold orders → staging'
AS
COPY INTO ECOMMERCE_DW.RAW.STG_FACT_ORDERS
FROM (
    SELECT
        MD5($1:order_id::STRING)                       AS ORDER_SK,
        $1:order_id::STRING                            AS ORDER_ID,
        $1:event_id::STRING                            AS EVENT_ID,
        MD5($1:user_id::STRING)                        AS USER_SK,
        MD5(CONCAT_WS('|',
            $1:device_type::STRING,
            $1:device_os::STRING,
            $1:browser::STRING))                       AS DEVICE_SK,
        TRY_TO_NUMBER(TO_CHAR(
            $1:event_timestamp::TIMESTAMP_NTZ, 'YYYYMMDD'))    AS DATE_KEY,
        $1:user_id::STRING                             AS USER_ID,
        $1:session_id::STRING                          AS SESSION_ID,
        $1:country::STRING                             AS COUNTRY,
        COALESCE($1:currency::STRING, 'USD')           AS CURRENCY,
        $1:payment_method::STRING                      AS PAYMENT_METHOD,
        $1:order_status::STRING                        AS ORDER_STATUS,
        $1:event_timestamp::TIMESTAMP_NTZ              AS ORDER_TIMESTAMP,
        $1:item_count::INT                             AS ITEM_COUNT,
        $1:order_total::FLOAT                          AS ORDER_TOTAL,
        $1:subtotal::FLOAT                             AS SUBTOTAL,
        $1:tax_amount::FLOAT                           AS TAX_AMOUNT,
        $1:shipping_cost::FLOAT                        AS SHIPPING_COST,
        $1:discount_amount::FLOAT                      AS DISCOUNT_AMOUNT,
        $1:effective_discount_pct::FLOAT               AS EFFECTIVE_DISCOUNT_PCT,
        $1:order_value_tier::STRING                    AS ORDER_VALUE_TIER,
        $1:primary_category::STRING                    AS PRIMARY_CATEGORY,
        $1:distinct_products::INT                      AS DISTINCT_PRODUCTS,
        $1:customer_segment::STRING                    AS CUSTOMER_SEGMENT,
        $1:utm_source::STRING                          AS UTM_SOURCE,
        $1:utm_medium::STRING                          AS UTM_MEDIUM,
        $1:is_prime::BOOLEAN                           AS IS_PRIME,
        $1:is_gift::BOOLEAN                            AS IS_GIFT,
        $1:is_weekend::BOOLEAN                         AS IS_WEEKEND,
        $1:order_hour_bucket::STRING                   AS ORDER_HOUR_BUCKET,
        CURRENT_TIMESTAMP()                            AS LOADED_AT
    FROM @ECOMMERCE_DW.RAW.GOLD_STAGE/fact_orders/
)
FILE_FORMAT = (FORMAT_NAME = ECOMMERCE_DW.RAW.PARQUET_FORMAT)
ON_ERROR    = 'SKIP_FILE';


CREATE PIPE IF NOT EXISTS ECOMMERCE_DW.RAW.PIPE_FACT_PAYMENTS
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest pipe for Gold payments → staging'
AS
COPY INTO ECOMMERCE_DW.RAW.STG_FACT_PAYMENTS
FROM (
    SELECT
        MD5($1:payment_id::STRING)              AS PAYMENT_SK,
        $1:payment_id::STRING                   AS PAYMENT_ID,
        $1:order_id::STRING                     AS ORDER_ID,
        $1:user_id::STRING                      AS USER_ID,
        $1:session_id::STRING                   AS SESSION_ID,
        TRY_TO_NUMBER(TO_CHAR(
            $1:payment_timestamp::TIMESTAMP_NTZ, 'YYYYMMDD')) AS DATE_KEY,
        $1:payment_timestamp::TIMESTAMP_NTZ     AS PAYMENT_TIMESTAMP,
        $1:payment_method::STRING               AS PAYMENT_METHOD,
        $1:payment_gateway::STRING              AS PAYMENT_GATEWAY,
        $1:payment_status::STRING               AS PAYMENT_STATUS,
        $1:amount::FLOAT                        AS AMOUNT,
        $1:currency::STRING                     AS CURRENCY,
        $1:failure_reason::STRING               AS FAILURE_REASON,
        $1:processing_time_ms::INT              AS PROCESSING_TIME_MS,
        $1:is_3ds_authenticated::BOOLEAN        AS IS_3DS_AUTHENTICATED,
        $1:gateway_response_code::STRING        AS GATEWAY_RESPONSE_CODE,
        $1:installment_plan::INT                AS INSTALLMENT_PLAN,
        CURRENT_TIMESTAMP()                     AS LOADED_AT
    FROM @ECOMMERCE_DW.RAW.GOLD_STAGE/fact_payments/
)
FILE_FORMAT = (FORMAT_NAME = ECOMMERCE_DW.RAW.PARQUET_FORMAT)
ON_ERROR    = 'SKIP_FILE';


CREATE PIPE IF NOT EXISTS ECOMMERCE_DW.RAW.PIPE_FACT_CLICKSTREAM
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest pipe for Gold clickstream → staging'
AS
COPY INTO ECOMMERCE_DW.RAW.STG_FACT_CLICKSTREAM
FROM (
    SELECT
        MD5($1:event_id::STRING)                AS CLICK_SK,
        $1:event_id::STRING                     AS EVENT_ID,
        $1:session_id::STRING                   AS SESSION_ID,
        $1:user_id::STRING                      AS USER_ID,
        TRY_TO_NUMBER(TO_CHAR(
            $1:view_timestamp::TIMESTAMP_NTZ, 'YYYYMMDD'))     AS DATE_KEY,
        $1:view_timestamp::TIMESTAMP_NTZ        AS VIEW_TIMESTAMP,
        $1:page_type::STRING                    AS PAGE_TYPE,
        $1:product_id::STRING                   AS PRODUCT_ID,
        $1:category::STRING                     AS CATEGORY,
        $1:search_query::STRING                 AS SEARCH_QUERY,
        $1:time_on_page_seconds::INT            AS TIME_ON_PAGE_SECONDS,
        $1:scroll_depth_pct::INT                AS SCROLL_DEPTH_PCT,
        $1:load_time_ms::INT                    AS LOAD_TIME_MS,
        $1:is_bounce::BOOLEAN                   AS IS_BOUNCE,
        $1:device_type::STRING                  AS DEVICE_TYPE,
        $1:country::STRING                      AS COUNTRY,
        $1:utm_source::STRING                   AS UTM_SOURCE,
        $1:utm_medium::STRING                   AS UTM_MEDIUM,
        $1:customer_segment::STRING             AS CUSTOMER_SEGMENT,
        $1:ab_test_variant::STRING              AS AB_TEST_VARIANT,
        $1:viewport_width::INT                  AS VIEWPORT_WIDTH,
        CURRENT_TIMESTAMP()                     AS LOADED_AT
    FROM @ECOMMERCE_DW.RAW.GOLD_STAGE/fact_clickstream/
)
FILE_FORMAT = (FORMAT_NAME = ECOMMERCE_DW.RAW.PARQUET_FORMAT)
ON_ERROR    = 'SKIP_FILE';


-- ─────────────────────────────────────────────
-- MASTER ORCHESTRATION PROCEDURE
-- Runs after Snowpipe fills staging tables
-- ─────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE ECOMMERCE_DW.ANALYTICS.SP_RUN_FULL_LOAD()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    start_ts  TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    results   VARIANT;
BEGIN
    -- 1. Merge facts
    CALL ECOMMERCE_DW.ANALYTICS.SP_MERGE_FACT_ORDERS();
    CALL ECOMMERCE_DW.ANALYTICS.SP_MERGE_FACT_PAYMENTS();
    CALL ECOMMERCE_DW.ANALYTICS.SP_MERGE_FACT_CLICKSTREAM();

    -- 2. Rebuild dimensions
    CALL ECOMMERCE_DW.ANALYTICS.SP_MERGE_DIM_USERS();
    CALL ECOMMERCE_DW.ANALYTICS.SP_MERGE_DIM_PRODUCTS();

    -- 3. Refresh reporting
    CALL ECOMMERCE_DW.ANALYTICS.SP_REFRESH_REPORTING();

    SELECT OBJECT_CONSTRUCT(
        'status',        'SUCCESS',
        'started_at',    :start_ts::STRING,
        'completed_at',  CURRENT_TIMESTAMP()::STRING,
        'duration_secs', DATEDIFF('second', :start_ts, CURRENT_TIMESTAMP())
    ) INTO :results;

    RETURN results;
END;
$$;


-- ─────────────────────────────────────────────
-- MERGE: FACT_PAYMENTS
-- ─────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE ECOMMERCE_DW.ANALYTICS.SP_MERGE_FACT_PAYMENTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO ECOMMERCE_DW.ANALYTICS.FACT_PAYMENTS tgt
    USING ECOMMERCE_DW.RAW.STG_FACT_PAYMENTS src
        ON tgt.PAYMENT_ID = src.PAYMENT_ID
    WHEN MATCHED AND src.PAYMENT_STATUS != tgt.PAYMENT_STATUS THEN
        UPDATE SET
            PAYMENT_STATUS = src.PAYMENT_STATUS,
            FAILURE_REASON = src.FAILURE_REASON,
            LOADED_AT      = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            PAYMENT_SK, PAYMENT_ID, ORDER_ID, USER_ID, SESSION_ID, DATE_KEY,
            PAYMENT_TIMESTAMP, PAYMENT_METHOD, PAYMENT_GATEWAY, PAYMENT_STATUS,
            AMOUNT, CURRENCY, FAILURE_REASON, PROCESSING_TIME_MS,
            IS_3DS_AUTHENTICATED, GATEWAY_RESPONSE_CODE, INSTALLMENT_PLAN, LOADED_AT
        )
        VALUES (
            src.PAYMENT_SK, src.PAYMENT_ID, src.ORDER_ID, src.USER_ID, src.SESSION_ID, src.DATE_KEY,
            src.PAYMENT_TIMESTAMP, src.PAYMENT_METHOD, src.PAYMENT_GATEWAY, src.PAYMENT_STATUS,
            src.AMOUNT, src.CURRENCY, src.FAILURE_REASON, src.PROCESSING_TIME_MS,
            src.IS_3DS_AUTHENTICATED, src.GATEWAY_RESPONSE_CODE, src.INSTALLMENT_PLAN, CURRENT_TIMESTAMP()
        );

    TRUNCATE TABLE ECOMMERCE_DW.RAW.STG_FACT_PAYMENTS;
    RETURN 'Payments merge complete: ' || CURRENT_TIMESTAMP()::STRING;
END;
$$;


-- ─────────────────────────────────────────────
-- MERGE: FACT_CLICKSTREAM
-- ─────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE ECOMMERCE_DW.ANALYTICS.SP_MERGE_FACT_CLICKSTREAM()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO ECOMMERCE_DW.ANALYTICS.FACT_CLICKSTREAM tgt
    USING ECOMMERCE_DW.RAW.STG_FACT_CLICKSTREAM src
        ON tgt.EVENT_ID = src.EVENT_ID
    WHEN NOT MATCHED THEN
        INSERT (
            CLICK_SK, EVENT_ID, SESSION_ID, USER_ID, DATE_KEY, VIEW_TIMESTAMP,
            PAGE_TYPE, PRODUCT_ID, CATEGORY, SEARCH_QUERY, TIME_ON_PAGE_SECONDS,
            SCROLL_DEPTH_PCT, LOAD_TIME_MS, IS_BOUNCE, DEVICE_TYPE, COUNTRY,
            UTM_SOURCE, UTM_MEDIUM, CUSTOMER_SEGMENT, AB_TEST_VARIANT, VIEWPORT_WIDTH, LOADED_AT
        )
        VALUES (
            src.CLICK_SK, src.EVENT_ID, src.SESSION_ID, src.USER_ID, src.DATE_KEY, src.VIEW_TIMESTAMP,
            src.PAGE_TYPE, src.PRODUCT_ID, src.CATEGORY, src.SEARCH_QUERY, src.TIME_ON_PAGE_SECONDS,
            src.SCROLL_DEPTH_PCT, src.LOAD_TIME_MS, src.IS_BOUNCE, src.DEVICE_TYPE, src.COUNTRY,
            src.UTM_SOURCE, src.UTM_MEDIUM, src.CUSTOMER_SEGMENT, src.AB_TEST_VARIANT, src.VIEWPORT_WIDTH, CURRENT_TIMESTAMP()
        );

    TRUNCATE TABLE ECOMMERCE_DW.RAW.STG_FACT_CLICKSTREAM;
    RETURN 'Clickstream merge complete: ' || CURRENT_TIMESTAMP()::STRING;
END;
$$;


-- ─────────────────────────────────────────────
-- MERGE: DIM_PRODUCTS
-- ─────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE ECOMMERCE_DW.ANALYTICS.SP_MERGE_DIM_PRODUCTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO ECOMMERCE_DW.ANALYTICS.DIM_PRODUCTS tgt
    USING ECOMMERCE_DW.RAW.STG_DIM_PRODUCTS src
        ON tgt.PRODUCT_ID = src.PRODUCT_ID
    WHEN MATCHED THEN
        UPDATE SET
            PRODUCT_NAME   = src.PRODUCT_NAME,
            AVG_UNIT_PRICE = src.AVG_UNIT_PRICE,
            MIN_UNIT_PRICE = src.MIN_UNIT_PRICE,
            MAX_UNIT_PRICE = src.MAX_UNIT_PRICE,
            UPDATED_AT     = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (PRODUCT_SK, PRODUCT_ID, PRODUCT_NAME, CATEGORY, BRAND,
                AVG_UNIT_PRICE, MIN_UNIT_PRICE, MAX_UNIT_PRICE, IS_ACTIVE,
                CREATED_AT, UPDATED_AT)
        VALUES (src.PRODUCT_SK, src.PRODUCT_ID, src.PRODUCT_NAME, src.CATEGORY, src.BRAND,
                src.AVG_UNIT_PRICE, src.MIN_UNIT_PRICE, src.MAX_UNIT_PRICE, TRUE,
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

    TRUNCATE TABLE ECOMMERCE_DW.RAW.STG_DIM_PRODUCTS;
    RETURN 'DIM_PRODUCTS merge complete: ' || CURRENT_TIMESTAMP()::STRING;
END;
$$;


-- ─────────────────────────────────────────────
-- Scheduled Task: Run full load every 15 minutes
-- ─────────────────────────────────────────────

ALTER TASK IF EXISTS ECOMMERCE_DW.ANALYTICS.REFRESH_REPORTING_TASK SUSPEND;

CREATE OR REPLACE TASK ECOMMERCE_DW.ANALYTICS.FULL_LOAD_TASK
    WAREHOUSE = LOADING_WH
    SCHEDULE  = 'USING CRON */15 * * * * UTC'
    COMMENT   = 'Run full merge + reporting refresh every 15 minutes'
AS
    CALL ECOMMERCE_DW.ANALYTICS.SP_RUN_FULL_LOAD();

ALTER TASK ECOMMERCE_DW.ANALYTICS.FULL_LOAD_TASK RESUME;


-- ─────────────────────────────────────────────
-- Pipe Status Check Query (run manually)
-- ─────────────────────────────────────────────

-- SELECT SYSTEM$PIPE_STATUS('ECOMMERCE_DW.RAW.PIPE_FACT_ORDERS');
-- SELECT SYSTEM$PIPE_STATUS('ECOMMERCE_DW.RAW.PIPE_FACT_PAYMENTS');
-- SELECT SYSTEM$PIPE_STATUS('ECOMMERCE_DW.RAW.PIPE_FACT_CLICKSTREAM');

-- Copy history (last 24h)
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
--     TABLE_NAME => 'STG_FACT_ORDERS',
--     START_TIME => DATEADD('hours', -24, CURRENT_TIMESTAMP())
-- ));

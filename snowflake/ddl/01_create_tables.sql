-- =============================================================
-- Snowflake DDL: E-Commerce Data Warehouse
-- =============================================================
-- Star schema implementation with fact + dimension tables,
-- incremental loading support, clustering, and optimization.
--
-- Author:  Data Engineering Team
-- Version: 1.0.0
-- =============================================================

-- ─────────────────────────────────────────────
-- DATABASE & SCHEMA SETUP
-- ─────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS ECOMMERCE_DW
    DATA_RETENTION_TIME_IN_DAYS = 14
    COMMENT = 'E-Commerce Real-Time Data Warehouse';

CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DW.RAW
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Raw staging tables for initial S3 loads';

CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DW.ANALYTICS
    DATA_RETENTION_TIME_IN_DAYS = 30
    COMMENT = 'Analytics-ready fact and dimension tables';

CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DW.REPORTING
    DATA_RETENTION_TIME_IN_DAYS = 30
    COMMENT = 'Pre-aggregated reporting tables for BI tools';

USE DATABASE ECOMMERCE_DW;
USE SCHEMA ANALYTICS;


-- ─────────────────────────────────────────────
-- VIRTUAL WAREHOUSES
-- ─────────────────────────────────────────────

CREATE WAREHOUSE IF NOT EXISTS LOADING_WH
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    COMMENT = 'Used for ETL loading jobs';

CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Used for BI and analytics queries';

CREATE WAREHOUSE IF NOT EXISTS REPORTING_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Used by Power BI / Tableau dashboards';


-- ─────────────────────────────────────────────
-- STORAGE INTEGRATION (S3)
-- ─────────────────────────────────────────────

CREATE STORAGE INTEGRATION IF NOT EXISTS S3_ECOMMERCE_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::ACCOUNT_ID:role/snowflake-s3-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://ecommerce-data-lake/gold/');


-- ─────────────────────────────────────────────
-- FILE FORMAT
-- ─────────────────────────────────────────────

CREATE FILE FORMAT IF NOT EXISTS ECOMMERCE_DW.RAW.PARQUET_FORMAT
    TYPE = 'PARQUET'
    SNAPPY_COMPRESSION = TRUE
    BINARY_AS_TEXT = FALSE;

CREATE FILE FORMAT IF NOT EXISTS ECOMMERCE_DW.RAW.JSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = FALSE
    NULL_IF = ('NULL', 'null', '');


-- ─────────────────────────────────────────────
-- EXTERNAL STAGES
-- ─────────────────────────────────────────────

CREATE STAGE IF NOT EXISTS ECOMMERCE_DW.RAW.GOLD_STAGE
    URL = 's3://ecommerce-data-lake/gold/'
    STORAGE_INTEGRATION = S3_ECOMMERCE_INTEGRATION
    FILE_FORMAT = ECOMMERCE_DW.RAW.PARQUET_FORMAT
    COMMENT = 'Gold layer S3 stage for Snowflake ingestion';


-- =============================================================
-- DIMENSION TABLES
-- =============================================================

-- ─────────────────────────────────────────────
-- DIM_TIME
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ECOMMERCE_DW.ANALYTICS.DIM_TIME (
    DATE_KEY          INT           NOT NULL,
    FULL_DATE         DATE          NOT NULL,
    YEAR              SMALLINT      NOT NULL,
    MONTH             TINYINT       NOT NULL,
    DAY               TINYINT       NOT NULL,
    WEEK_OF_YEAR      TINYINT       NOT NULL,
    DAY_NAME          VARCHAR(10)   NOT NULL,
    MONTH_NAME        VARCHAR(10)   NOT NULL,
    QUARTER           TINYINT       NOT NULL,
    IS_WEEKEND        BOOLEAN       NOT NULL DEFAULT FALSE,
    FISCAL_YEAR       SMALLINT,
    FISCAL_QUARTER    TINYINT,
    SEASON            VARCHAR(10),
    IS_HOLIDAY        BOOLEAN       DEFAULT FALSE,
    UPDATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_DIM_TIME PRIMARY KEY (DATE_KEY)
)
COMMENT = 'Date/time dimension — pre-generated 2024-2027';


-- ─────────────────────────────────────────────
-- DIM_USERS
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ECOMMERCE_DW.ANALYTICS.DIM_USERS (
    USER_SK               VARCHAR(32)   NOT NULL,
    USER_ID               VARCHAR(20)   NOT NULL,
    COUNTRY               VARCHAR(5),
    CITY                  VARCHAR(100),
    DEVICE_TYPE           VARCHAR(20),
    CUSTOMER_SEGMENT      VARCHAR(20),
    IS_PRIME              BOOLEAN       DEFAULT FALSE,
    IS_ACTIVE             BOOLEAN       DEFAULT TRUE,
    FIRST_ORDER_DATE      DATE,
    LAST_ORDER_DATE       DATE,
    TOTAL_ORDERS          INT           DEFAULT 0,
    TOTAL_SPEND           NUMBER(14,2)  DEFAULT 0,
    AVG_ORDER_VALUE       NUMBER(10,2),
    PREFERRED_CATEGORY    VARCHAR(50),
    PREFERRED_DEVICE      VARCHAR(20),
    DAYS_SINCE_LAST_ORDER INT,
    CHURN_RISK_SCORE      NUMBER(5,4),
    LTV_SCORE             NUMBER(10,2),
    CREATED_AT            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_DIM_USERS PRIMARY KEY (USER_SK),
    CONSTRAINT UQ_DIM_USERS_USER_ID UNIQUE (USER_ID)
)
CLUSTER BY (COUNTRY, CUSTOMER_SEGMENT)
COMMENT = 'Customer dimension with SCD Type 1 updates';


-- ─────────────────────────────────────────────
-- DIM_PRODUCTS
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ECOMMERCE_DW.ANALYTICS.DIM_PRODUCTS (
    PRODUCT_SK        VARCHAR(32)   NOT NULL,
    PRODUCT_ID        VARCHAR(20)   NOT NULL,
    PRODUCT_NAME      VARCHAR(500),
    CATEGORY          VARCHAR(100),
    SUBCATEGORY       VARCHAR(100),
    BRAND             VARCHAR(200),
    AVG_UNIT_PRICE    NUMBER(10,2),
    MIN_UNIT_PRICE    NUMBER(10,2),
    MAX_UNIT_PRICE    NUMBER(10,2),
    PRICE_TIER        VARCHAR(20),
    IS_ACTIVE         BOOLEAN       DEFAULT TRUE,
    LAUNCH_DATE       DATE,
    CREATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_DIM_PRODUCTS PRIMARY KEY (PRODUCT_SK),
    CONSTRAINT UQ_DIM_PRODUCTS_PRODUCT_ID UNIQUE (PRODUCT_ID)
)
CLUSTER BY (CATEGORY, BRAND)
COMMENT = 'Product catalog dimension';


-- ─────────────────────────────────────────────
-- DIM_DEVICES
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ECOMMERCE_DW.ANALYTICS.DIM_DEVICES (
    DEVICE_SK     VARCHAR(32)  NOT NULL,
    DEVICE_TYPE   VARCHAR(20)  NOT NULL,
    DEVICE_OS     VARCHAR(50),
    BROWSER       VARCHAR(50),
    DEVICE_CLASS  VARCHAR(20),
    IS_MOBILE     BOOLEAN      DEFAULT FALSE,
    UPDATED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_DIM_DEVICES PRIMARY KEY (DEVICE_SK)
)
COMMENT = 'Device & browser dimension';


-- =============================================================
-- FACT TABLES
-- =============================================================

-- ─────────────────────────────────────────────
-- FACT_ORDERS
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ECOMMERCE_DW.ANALYTICS.FACT_ORDERS (
    ORDER_SK              VARCHAR(32)   NOT NULL,
    ORDER_ID              VARCHAR(25)   NOT NULL,
    EVENT_ID              VARCHAR(40),
    USER_SK               VARCHAR(32),
    DEVICE_SK             VARCHAR(32),
    DATE_KEY              INT,
    USER_ID               VARCHAR(20),
    SESSION_ID            VARCHAR(30),
    COUNTRY               VARCHAR(5),
    CURRENCY              VARCHAR(5)    DEFAULT 'USD',
    PAYMENT_METHOD        VARCHAR(30),
    ORDER_STATUS          VARCHAR(20),
    ORDER_TIMESTAMP       TIMESTAMP_NTZ,
    ITEM_COUNT            INT,
    ORDER_TOTAL           NUMBER(12,2),
    SUBTOTAL              NUMBER(12,2),
    TAX_AMOUNT            NUMBER(10,2),
    SHIPPING_COST         NUMBER(10,2),
    DISCOUNT_AMOUNT       NUMBER(10,2),
    EFFECTIVE_DISCOUNT_PCT NUMBER(6,4),
    ORDER_VALUE_TIER      VARCHAR(15),
    PRIMARY_CATEGORY      VARCHAR(100),
    DISTINCT_PRODUCTS     INT,
    CUSTOMER_SEGMENT      VARCHAR(20),
    UTM_SOURCE            VARCHAR(50),
    UTM_MEDIUM            VARCHAR(50),
    IS_PRIME              BOOLEAN       DEFAULT FALSE,
    IS_GIFT               BOOLEAN       DEFAULT FALSE,
    IS_WEEKEND            BOOLEAN       DEFAULT FALSE,
    ORDER_HOUR_BUCKET     VARCHAR(15),
    LOADED_AT             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_FACT_ORDERS PRIMARY KEY (ORDER_SK),
    CONSTRAINT UQ_FACT_ORDERS_ORDER_ID UNIQUE (ORDER_ID),
    CONSTRAINT FK_FACT_ORDERS_USERS FOREIGN KEY (USER_SK) REFERENCES DIM_USERS(USER_SK),
    CONSTRAINT FK_FACT_ORDERS_DEVICES FOREIGN KEY (DEVICE_SK) REFERENCES DIM_DEVICES(DEVICE_SK),
    CONSTRAINT FK_FACT_ORDERS_TIME FOREIGN KEY (DATE_KEY) REFERENCES DIM_TIME(DATE_KEY)
)
CLUSTER BY (DATE_KEY, COUNTRY, ORDER_STATUS)
COMMENT = 'Core orders fact table — one row per order';


-- ─────────────────────────────────────────────
-- FACT_PAYMENTS
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ECOMMERCE_DW.ANALYTICS.FACT_PAYMENTS (
    PAYMENT_SK            VARCHAR(32)   NOT NULL,
    PAYMENT_ID            VARCHAR(25)   NOT NULL,
    ORDER_ID              VARCHAR(25),
    USER_ID               VARCHAR(20),
    SESSION_ID            VARCHAR(30),
    DATE_KEY              INT,
    PAYMENT_TIMESTAMP     TIMESTAMP_NTZ,
    PAYMENT_METHOD        VARCHAR(30),
    PAYMENT_GATEWAY       VARCHAR(30),
    PAYMENT_STATUS        VARCHAR(20),
    AMOUNT                NUMBER(12,2),
    CURRENCY              VARCHAR(5),
    FAILURE_REASON        VARCHAR(100),
    PROCESSING_TIME_MS    INT,
    IS_3DS_AUTHENTICATED  BOOLEAN       DEFAULT FALSE,
    GATEWAY_RESPONSE_CODE VARCHAR(10),
    INSTALLMENT_PLAN      TINYINT       DEFAULT 1,
    LOADED_AT             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_FACT_PAYMENTS PRIMARY KEY (PAYMENT_SK),
    CONSTRAINT UQ_FACT_PAYMENTS_PAYMENT_ID UNIQUE (PAYMENT_ID),
    CONSTRAINT FK_FACT_PAYMENTS_TIME FOREIGN KEY (DATE_KEY) REFERENCES DIM_TIME(DATE_KEY)
)
CLUSTER BY (DATE_KEY, PAYMENT_STATUS, PAYMENT_METHOD)
COMMENT = 'Payment transactions fact table';


-- ─────────────────────────────────────────────
-- FACT_CLICKSTREAM
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ECOMMERCE_DW.ANALYTICS.FACT_CLICKSTREAM (
    CLICK_SK              VARCHAR(32)   NOT NULL,
    EVENT_ID              VARCHAR(40)   NOT NULL,
    SESSION_ID            VARCHAR(30),
    USER_ID               VARCHAR(20),
    DATE_KEY              INT,
    VIEW_TIMESTAMP        TIMESTAMP_NTZ,
    PAGE_TYPE             VARCHAR(30),
    PRODUCT_ID            VARCHAR(20),
    CATEGORY              VARCHAR(100),
    SEARCH_QUERY          VARCHAR(500),
    TIME_ON_PAGE_SECONDS  INT,
    SCROLL_DEPTH_PCT      TINYINT,
    LOAD_TIME_MS          INT,
    IS_BOUNCE             BOOLEAN       DEFAULT FALSE,
    DEVICE_TYPE           VARCHAR(20),
    COUNTRY               VARCHAR(5),
    UTM_SOURCE            VARCHAR(50),
    UTM_MEDIUM            VARCHAR(50),
    CUSTOMER_SEGMENT      VARCHAR(20),
    AB_TEST_VARIANT       VARCHAR(20),
    VIEWPORT_WIDTH        SMALLINT,
    LOADED_AT             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_FACT_CLICKSTREAM PRIMARY KEY (CLICK_SK),
    CONSTRAINT FK_FACT_CLICKSTREAM_TIME FOREIGN KEY (DATE_KEY) REFERENCES DIM_TIME(DATE_KEY)
)
CLUSTER BY (DATE_KEY, PAGE_TYPE, COUNTRY)
COMMENT = 'Web/app clickstream events fact table';


-- =============================================================
-- REPORTING TABLES (Pre-aggregated)
-- =============================================================

USE SCHEMA ECOMMERCE_DW.REPORTING;

CREATE TABLE IF NOT EXISTS ECOMMERCE_DW.REPORTING.RPT_DAILY_REVENUE (
    REPORT_DATE           DATE          NOT NULL,
    COUNTRY               VARCHAR(5),
    DEVICE_TYPE           VARCHAR(20),
    PAYMENT_METHOD        VARCHAR(30),
    CUSTOMER_SEGMENT      VARCHAR(20),
    TOTAL_ORDERS          INT,
    UNIQUE_CUSTOMERS      INT,
    GROSS_REVENUE         NUMBER(14,2),
    NET_REVENUE           NUMBER(14,2),
    TOTAL_DISCOUNTS       NUMBER(14,2),
    AVG_ORDER_VALUE       NUMBER(10,2),
    TOTAL_SHIPPING        NUMBER(10,2),
    TOTAL_TAX             NUMBER(10,2),
    REVENUE_PER_CUSTOMER  NUMBER(10,2),
    UPDATED_AT            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_RPT_DAILY_REVENUE PRIMARY KEY (REPORT_DATE, COUNTRY, DEVICE_TYPE, PAYMENT_METHOD, CUSTOMER_SEGMENT)
);

CREATE TABLE IF NOT EXISTS ECOMMERCE_DW.REPORTING.RPT_PRODUCT_PERFORMANCE (
    YEAR              SMALLINT     NOT NULL,
    MONTH             TINYINT      NOT NULL,
    PRODUCT_ID        VARCHAR(20)  NOT NULL,
    PRODUCT_NAME      VARCHAR(500),
    CATEGORY          VARCHAR(100),
    BRAND             VARCHAR(200),
    UNITS_SOLD        BIGINT,
    ORDER_COUNT       INT,
    TOTAL_REVENUE     NUMBER(14,2),
    AVG_SELLING_PRICE NUMBER(10,2),
    AVG_DISCOUNT_PCT  NUMBER(6,2),
    REVENUE_RANK      INT,
    UPDATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_RPT_PRODUCT PRIMARY KEY (YEAR, MONTH, PRODUCT_ID)
);

CREATE TABLE IF NOT EXISTS ECOMMERCE_DW.REPORTING.RPT_FUNNEL (
    REPORT_DATE         DATE         NOT NULL,
    COUNTRY             VARCHAR(5)   NOT NULL,
    DEVICE_TYPE         VARCHAR(20)  NOT NULL,
    SESSIONS            BIGINT,
    PAGE_VIEWS          BIGINT,
    CART_SESSIONS       BIGINT,
    ORDER_SESSIONS      BIGINT,
    CART_RATE           NUMBER(8,4),
    CONVERSION_RATE     NUMBER(8,4),
    CART_TO_ORDER_RATE  NUMBER(8,4),
    REVENUE             NUMBER(14,2),
    UPDATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_RPT_FUNNEL PRIMARY KEY (REPORT_DATE, COUNTRY, DEVICE_TYPE)
);


-- =============================================================
-- STREAMS & TASKS (Change Data Capture)
-- =============================================================

USE SCHEMA ECOMMERCE_DW.ANALYTICS;

-- Stream on fact_orders for CDC
CREATE STREAM IF NOT EXISTS FACT_ORDERS_STREAM
    ON TABLE FACT_ORDERS
    COMMENT = 'CDC stream to detect new/updated orders for downstream';

-- Task: refresh reporting tables every hour
CREATE TASK IF NOT EXISTS REFRESH_REPORTING_TASK
    WAREHOUSE = REPORTING_WH
    SCHEDULE  = 'USING CRON 0 * * * * UTC'
    COMMENT   = 'Hourly refresh of pre-aggregated reporting tables'
AS
    CALL ECOMMERCE_DW.ANALYTICS.SP_REFRESH_REPORTING();

# =============================================================
# Power BI: Schema Guide, DAX Measures & Dashboard Specs
# =============================================================
# Complete guide for connecting Power BI to Snowflake and
# building the 6-page executive dashboard.
#
# Author:  Data Engineering Team
# Version: 1.0.0
# =============================================================


## 1. SNOWFLAKE CONNECTION (Power BI Desktop)

```
Data Source:   Snowflake
Server:        <account>.snowflakecomputing.com
Warehouse:     REPORTING_WH
Database:      ECOMMERCE_DW
Schema:        ANALYTICS
Authentication: Username/Password  (or SSO for enterprise)
```

## 2. TABLES TO IMPORT

Import Mode (cached) → better performance for dashboards:

| Table                          | Import Mode | Refresh |
|-------------------------------|-------------|---------|
| ANALYTICS.FACT_ORDERS         | Import      | 15 min  |
| ANALYTICS.FACT_PAYMENTS       | Import      | 15 min  |
| ANALYTICS.FACT_CLICKSTREAM    | Import      | 15 min  |
| ANALYTICS.DIM_USERS           | Import      | 1 hr    |
| ANALYTICS.DIM_PRODUCTS        | Import      | 1 hr    |
| ANALYTICS.DIM_TIME            | Import      | Daily   |
| ANALYTICS.DIM_DEVICES         | Import      | Daily   |
| REPORTING.RPT_DAILY_REVENUE   | Import      | 15 min  |
| REPORTING.RPT_PRODUCT_PERFORMANCE | Import  | 1 hr    |
| REPORTING.RPT_FUNNEL          | Import      | 15 min  |


## 3. DATA MODEL RELATIONSHIPS

```
FACT_ORDERS[USER_SK]    → DIM_USERS[USER_SK]       (Many:1)
FACT_ORDERS[DEVICE_SK]  → DIM_DEVICES[DEVICE_SK]   (Many:1)
FACT_ORDERS[DATE_KEY]   → DIM_TIME[DATE_KEY]        (Many:1)
FACT_PAYMENTS[DATE_KEY] → DIM_TIME[DATE_KEY]        (Many:1)
FACT_CLICKSTREAM[DATE_KEY] → DIM_TIME[DATE_KEY]     (Many:1)
```


## 4. DAX MEASURES (paste into Power BI DAX editor)

### ── Core Revenue Measures ─────────────────────────────────

```dax
-- Total Revenue (confirmed orders only)
Total Revenue = 
CALCULATE(
    SUM(FACT_ORDERS[ORDER_TOTAL]),
    FACT_ORDERS[ORDER_STATUS] IN {"confirmed", "shipped", "delivered"}
)

-- Net Revenue (after discounts)
Net Revenue = 
CALCULATE(
    SUM(FACT_ORDERS[ORDER_TOTAL]) - SUM(FACT_ORDERS[DISCOUNT_AMOUNT]),
    FACT_ORDERS[ORDER_STATUS] IN {"confirmed", "shipped", "delivered"}
)

-- Average Order Value
AOV = 
DIVIDE(
    [Total Revenue],
    CALCULATE(COUNTROWS(FACT_ORDERS),
              FACT_ORDERS[ORDER_STATUS] IN {"confirmed","shipped","delivered"}),
    0
)

-- Revenue vs Prior Period
Revenue MoM % = 
VAR CurrentMonth = [Total Revenue]
VAR PriorMonth   = CALCULATE([Total Revenue],
                    DATEADD(DIM_TIME[FULL_DATE], -1, MONTH))
RETURN DIVIDE(CurrentMonth - PriorMonth, PriorMonth, 0)

-- Revenue 7-day rolling average
Revenue 7D Avg = 
CALCULATE(
    AVERAGEX(
        DATESINPERIOD(DIM_TIME[FULL_DATE], LASTDATE(DIM_TIME[FULL_DATE]), -7, DAY),
        [Total Revenue]
    )
)
```

### ── Order Metrics ──────────────────────────────────────────

```dax
-- Total Orders
Total Orders = 
CALCULATE(
    COUNTROWS(FACT_ORDERS),
    FACT_ORDERS[ORDER_STATUS] IN {"confirmed","shipped","delivered"}
)

-- Orders vs Prior Month
Orders MoM % = 
VAR curr = [Total Orders]
VAR prev = CALCULATE([Total Orders], DATEADD(DIM_TIME[FULL_DATE], -1, MONTH))
RETURN DIVIDE(curr - prev, prev, 0)

-- Cancelled Rate
Cancel Rate % = 
DIVIDE(
    CALCULATE(COUNTROWS(FACT_ORDERS), FACT_ORDERS[ORDER_STATUS] = "cancelled"),
    COUNTROWS(FACT_ORDERS),
    0
) * 100
```

### ── Customer Metrics ───────────────────────────────────────

```dax
-- Unique Customers
Unique Customers = DISTINCTCOUNT(FACT_ORDERS[USER_ID])

-- New vs Returning Customers
New Customers = 
CALCULATE(
    DISTINCTCOUNT(FACT_ORDERS[USER_ID]),
    DIM_USERS[CUSTOMER_SEGMENT] = "new"
)

Returning Customers = 
CALCULATE(
    DISTINCTCOUNT(FACT_ORDERS[USER_ID]),
    DIM_USERS[CUSTOMER_SEGMENT] IN {"returning","vip"}
)

-- Customer Lifetime Value (average)
Avg LTV = AVERAGE(DIM_USERS[LTV_SCORE])

-- Churn Risk: High Risk Count
High Churn Risk = 
CALCULATE(
    COUNTROWS(DIM_USERS),
    DIM_USERS[CHURN_RISK_SCORE] >= 0.65
)
```

### ── Payment Metrics ────────────────────────────────────────

```dax
-- Payment Success Rate
Payment Success % = 
DIVIDE(
    CALCULATE(COUNTROWS(FACT_PAYMENTS), FACT_PAYMENTS[PAYMENT_STATUS] = "success"),
    COUNTROWS(FACT_PAYMENTS),
    0
) * 100

-- Failed Payments
Failed Payments = 
CALCULATE(COUNTROWS(FACT_PAYMENTS),
          FACT_PAYMENTS[PAYMENT_STATUS] IN {"failed","declined"})

-- Avg Processing Time (ms)
Avg Processing Time = AVERAGE(FACT_PAYMENTS[PROCESSING_TIME_MS])

-- GMV (Gross Merchandise Value)
GMV = 
CALCULATE(
    SUM(FACT_PAYMENTS[AMOUNT]),
    FACT_PAYMENTS[PAYMENT_STATUS] = "success"
)
```

### ── Conversion & Funnel ────────────────────────────────────

```dax
-- Sessions
Total Sessions = DISTINCTCOUNT(FACT_CLICKSTREAM[SESSION_ID])

-- Conversion Rate
Conversion Rate % = 
DIVIDE([Total Orders], [Total Sessions], 0) * 100

-- Bounce Rate
Bounce Rate % = 
DIVIDE(
    CALCULATE(COUNTROWS(FACT_CLICKSTREAM), FACT_CLICKSTREAM[IS_BOUNCE] = TRUE),
    COUNTROWS(FACT_CLICKSTREAM),
    0
) * 100

-- Avg Time on Page
Avg Time on Page (s) = AVERAGE(FACT_CLICKSTREAM[TIME_ON_PAGE_SECONDS])
```

### ── Product Performance ─────────────────────────────────────

```dax
-- Top Category Revenue
Top Category = 
TOPN(1,
    SUMMARIZE(FACT_ORDERS, FACT_ORDERS[PRIMARY_CATEGORY],
              "Rev", [Total Revenue]),
    [Rev], DESC
)

-- Units Sold (approx from order count × avg items)
Units Sold = SUM(FACT_ORDERS[ITEM_COUNT])
```

### ── KPI Cards / Conditional Formatting ────────────────────

```dax
-- Revenue RAG Status
Revenue Status = 
VAR target = 100000  -- set your daily target
VAR actual = [Total Revenue]
RETURN
    IF(actual >= target,       "Green",
    IF(actual >= target * 0.8, "Amber",
                               "Red"))
```


## 5. DASHBOARD PAGE SPECIFICATIONS

### Page 1: Executive Overview
```
Layout: 4 KPI cards (top) + 2 charts + 1 table

KPI Cards:
  • Total Revenue     [with MoM %]
  • Total Orders      [with MoM %]
  • Avg Order Value   [with trend sparkline]
  • Unique Customers  [with MoM %]

Charts:
  • Revenue trend — Line chart, daily, 30 days, segmented by country (top 5)
  • Orders by status — Donut chart

Table:
  • Top 10 products: Name | Category | Revenue | Units | Rank
```

### Page 2: Sales Analytics
```
Visuals:
  • Revenue by country — Filled map (geo)
  • Revenue by payment method — Clustered bar
  • Hourly revenue heatmap — Matrix (day × hour)
  • Revenue by customer segment — Stacked bar
  • Daily revenue vs 7D rolling avg — Combo chart
  • Order value distribution — Histogram
```

### Page 3: Real-Time Activity
```
Visuals (set auto-refresh = 5 min):
  • Active sessions — Card
  • Orders last hour — Card
  • Revenue last hour — Card
  • Live order feed — Table (latest 50)
  • Sessions by page type — Bar chart
  • Events per minute — Line chart
```

### Page 4: User Behavior
```
Visuals:
  • Sessions by device type — Pie chart
  • Bounce rate by page — Bar chart
  • Avg time on page by page type — Bar chart
  • User journey (page flow) — Sankey or funnel
  • New vs Returning customers — Donut
  • Churn risk distribution — Histogram
```

### Page 5: Product Performance
```
Visuals:
  • Top 20 products by revenue — Horizontal bar
  • Revenue by category — Treemap
  • Brand performance — Bubble chart (revenue vs units vs aov)
  • Product revenue rank over time — Line (top 5)
  • Category mix by country — 100% stacked bar
```

### Page 6: Funnel Analytics
```
Visuals:
  • Conversion funnel — Funnel chart
      Sessions → Cart Adds → Orders → Paid
  • Conversion rate by device — Matrix
  • Conversion rate by UTM source — Bar
  • Cart abandonment by country — Map
  • Abandonment rate trend — Line chart
  • Revenue lost to abandonment — Card (est.)
```


## 6. SLICERS / FILTERS (Global across all pages)

```
• Date Range          — DIM_TIME[FULL_DATE]   (between)
• Country             — FACT_ORDERS[COUNTRY]  (multi-select)
• Device Type         — DIM_DEVICES[DEVICE_TYPE]
• Customer Segment    — DIM_USERS[CUSTOMER_SEGMENT]
• UTM Source          — FACT_ORDERS[UTM_SOURCE]
• Product Category    — DIM_PRODUCTS[CATEGORY]
```


## 7. INCREMENTAL REFRESH SETTINGS (Power BI Service)

```
Table:          FACT_ORDERS
Archive data:   90 days
Refresh data:   3 days
Detect changes: LOADED_AT column
```


## 8. ROW-LEVEL SECURITY (optional)

```dax
-- Regional Manager role: sees only their country
[COUNTRY] = USERPRINCIPALNAME()
-- Map email → country in a security mapping table
```


## 9. PERFORMANCE TIPS

1. Import mode > DirectQuery for all tables except real-time page.
2. Disable auto date/time (File > Options > Data Load).
3. Hide foreign key columns from report view.
4. Use Aggregations for FACT_CLICKSTREAM (largest table).
5. Set scheduled refresh to 15 minutes on Power BI Service Premium.
6. Pre-aggregate in Snowflake REPORTING schema; avoid heavy DAX aggregations.

-- =========================================================
-- Data Enrichment
-- =========================================================
CREATE OR REPLACE VIEW sales_enriched AS
SELECT
  s.order_number,
  s.line_item,
  s.order_date,
  s.delivery_date,
  s.delivered,
  DATEDIFF(s.delivery_date, s.order_date) AS delivery_days,  -- null when not delivered
  s.customer_key,
  s.store_key,
  s.product_key,
  s.quantity,
  s.currency_code,
  p.product_name,
  p.brand,
  p.color,
  p.unit_cost_usd,
  p.unit_price_usd,
  st.country,
  st.state
FROM sales s
JOIN products p ON s.product_key = p.product_key
JOIN stores   st ON s.store_key   = st.store_key;


-- =========================================================
-- Indexes for Performance
-- =========================================================
CREATE INDEX ix_sales_orderdate ON sales(order_date);
CREATE INDEX ix_sales_store ON sales(store_key);
CREATE INDEX ix_sales_product ON sales(product_key);
CREATE INDEX ix_sales_customer ON sales(customer_key);
CREATE INDEX ix_exrate_date_curr 
ON exchange_rates(date, currency(10));


-- =========================================================
-- Business Views
-- =========================================================

-- Top Products by Country
CREATE OR REPLACE VIEW v_top_products_per_country AS
SELECT *
FROM (
  SELECT
    se.country,
    se.product_name,
    ROUND(SUM(se.unit_price_usd * se.quantity), 2) AS total_revenue_usd,
    RANK() OVER (
      PARTITION BY se.country
      ORDER BY SUM(se.unit_price_usd * se.quantity) DESC, se.product_name ASC
    ) AS product_rank
  FROM sales_enriched se
  GROUP BY se.country, se.product_name
) ranked
WHERE product_rank <= 3
ORDER BY country, product_rank;

-- Month on Month Revenue 
CREATE OR REPLACE VIEW v_month_on_month_revenue AS
WITH monthly AS (
  SELECT
    STR_TO_DATE(CONCAT(DATE_FORMAT(order_date, '%Y-%m'), '-01'), '%Y-%m-%d') AS month_start,
    SUM(unit_price_usd * quantity) AS revenue_usd
  FROM sales_enriched
  GROUP BY month_start
)
SELECT
  month_start,
  ROUND(revenue_usd, 2) AS revenue_usd,
  -- Month-over-month % change
  ROUND(
    100.0 * (revenue_usd - LAG(revenue_usd) OVER (ORDER BY month_start))
    / NULLIF(LAG(revenue_usd) OVER (ORDER BY month_start), 0), 2
  ) AS mom_pct_change,
  -- 3-month moving average (current + 2 previous months)
  ROUND(
    AVG(revenue_usd) OVER (
      ORDER BY month_start
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2
  ) AS ma_3mo
FROM monthly
ORDER BY month_start;

-- Top Customers
CREATE OR REPLACE VIEW v_top_customers AS
SELECT
  s.customer_key,
  c.name,
  ROUND(SUM(s.unit_price_usd * s.quantity), 2) AS lifetime_revenue_usd,
  ROUND(
    100.0 * SUM(s.unit_price_usd * s.quantity)
    / SUM(SUM(s.unit_price_usd * s.quantity)) OVER (), 2
  ) AS pct_of_total
FROM sales_enriched s
JOIN customers c ON c.customer_key = s.customer_key
GROUP BY s.customer_key, c.name
ORDER BY lifetime_revenue_usd DESC
LIMIT 10;

-- Category Year on Year Growth
CREATE OR REPLACE VIEW v_category_yoy_growth AS
WITH cat_month AS (
  SELECT
    p.category,
    STR_TO_DATE(CONCAT(DATE_FORMAT(se.order_date, '%Y-%m'), '-01'), '%Y-%m-%d') AS month_start,
    SUM(se.unit_price_usd * se.quantity) AS revenue_usd
  FROM sales_enriched se
  JOIN products p ON p.product_key = se.product_key
  GROUP BY p.category, month_start
)
SELECT
  category,
  month_start,
  ROUND(revenue_usd, 2) AS revenue_usd,
  ROUND(LAG(revenue_usd, 12) OVER (PARTITION BY category ORDER BY month_start), 2) AS revenue_last_year,
  ROUND(
    100.0 * (revenue_usd - LAG(revenue_usd, 12) OVER (PARTITION BY category ORDER BY month_start))
    / NULLIF(LAG(revenue_usd, 12) OVER (PARTITION BY category ORDER BY month_start), 0), 2
  ) AS yoy_growth_pct
FROM cat_month
ORDER BY category, month_start;

-- Category Seasonality 
CREATE OR REPLACE VIEW v_category_seasonality AS
WITH cat_month AS (
  SELECT
    p.category,
    YEAR(se.order_date) AS yr,
    MONTH(se.order_date) AS month_num,
    SUM(se.unit_price_usd * se.quantity) AS revenue_usd
  FROM sales_enriched se
  JOIN products p ON p.product_key = se.product_key
  GROUP BY p.category, YEAR(se.order_date), MONTH(se.order_date)
),
avg_month AS (
  SELECT
    category,
    month_num,
    AVG(revenue_usd) AS avg_revenue_usd
  FROM cat_month
  GROUP BY category, month_num
)
SELECT
  category,
  month_num,
  ROUND(avg_revenue_usd, 2) AS avg_revenue_usd,
  RANK() OVER (PARTITION BY category ORDER BY avg_revenue_usd DESC, month_num ASC) AS month_rank_in_category
FROM avg_month
ORDER BY category, month_rank_in_category, month_num;

-- Customer Repeat Rate
CREATE OR REPLACE VIEW v_customer_repeat_rate AS
WITH cust_orders AS (
  SELECT
    customer_key,
    COUNT(DISTINCT order_number) AS order_count
  FROM sales_enriched
  GROUP BY customer_key
)
SELECT
  COUNT(*) AS customers_total,
  SUM(order_count >= 2) AS customers_repeat,
  ROUND(100.0 * SUM(order_count >= 2) / COUNT(*), 2) AS repeat_pct
FROM cust_orders;

-- Customer Cohort Retention
CREATE OR REPLACE VIEW v_customer_cohort_retention AS
WITH first_order AS (
  SELECT
    customer_key,
    STR_TO_DATE(CONCAT(DATE_FORMAT(MIN(order_date), '%Y-%m'), '-01'), '%Y-%m-%d') AS cohort_month
  FROM sales_enriched
  GROUP BY customer_key
),
activity AS (
  SELECT
    customer_key,
    STR_TO_DATE(CONCAT(DATE_FORMAT(order_date, '%Y-%m'), '-01'), '%Y-%m-%d') AS activity_month
  FROM sales_enriched
  GROUP BY customer_key, activity_month
),
cohort AS (
  SELECT
    fo.cohort_month,
    a.activity_month,
    TIMESTAMPDIFF(MONTH, fo.cohort_month, a.activity_month) AS cohort_age_mo,
    COUNT(DISTINCT a.customer_key) AS active_customers
  FROM first_order fo
  JOIN activity a ON a.customer_key = fo.customer_key
  GROUP BY fo.cohort_month, a.activity_month, cohort_age_mo
)
SELECT
  cohort_month,
  cohort_age_mo,
  active_customers,
  ROUND(
    100.0 * active_customers /
    NULLIF(MAX(CASE WHEN cohort_age_mo = 0 THEN active_customers END)
      OVER (PARTITION BY cohort_month), 0), 2
  ) AS retention_pct
FROM cohort
ORDER BY cohort_month, cohort_age_mo;

-- Store Performance
CREATE OR REPLACE VIEW v_store_performance AS
WITH store_rev AS (
  SELECT
    se.store_key,
    st.country,
    st.state,
    st.square_meters,
    SUM(se.unit_price_usd * se.quantity) AS revenue_usd
  FROM sales_enriched se
  JOIN stores st ON st.store_key = se.store_key
  GROUP BY se.store_key, st.country, st.state, st.square_meters
)
SELECT
  country,
  revenue_usd,
  square_meters,
  ROUND(revenue_usd / NULLIF(square_meters, 0), 2) AS revenue_per_sqm,
  RANK() OVER (PARTITION BY country ORDER BY revenue_usd DESC) AS revenue_rank_in_country,
  RANK() OVER (PARTITION BY country ORDER BY (revenue_usd / NULLIF(square_meters, 0)) DESC) AS productivity_rank_in_country
FROM store_rev
ORDER BY country, revenue_rank_in_country;

-- Online vs Physical
CREATE OR REPLACE VIEW v_online_vs_physical_monthly AS
WITH monthly AS (
  SELECT
    CASE WHEN st.state = 'Online' THEN 'Online' ELSE 'Physical' END AS channel,
    STR_TO_DATE(CONCAT(DATE_FORMAT(se.order_date, '%Y-%m'), '-01'), '%Y-%m-%d') AS month_start,
    SUM(se.unit_price_usd * se.quantity) AS revenue_usd
  FROM sales_enriched se
  JOIN stores st ON st.store_key = se.store_key
  GROUP BY channel, month_start
)
SELECT
  channel,
  month_start,
  ROUND(revenue_usd, 2) AS revenue_usd,
  ROUND(
    SUM(revenue_usd) OVER (PARTITION BY channel ORDER BY month_start ROWS UNBOUNDED PRECEDING), 2
  ) AS running_total_usd
FROM monthly
ORDER BY channel, month_start;

-- Monthly Delivery Trends
CREATE OR REPLACE VIEW v_monthly_delivery_trends AS
WITH monthly AS (
  SELECT
    STR_TO_DATE(CONCAT(DATE_FORMAT(order_date, '%Y-%m'), '-01'), '%Y-%m-%d') AS month_start,
    AVG(delivery_days) AS avg_delivery_days
  FROM sales_enriched
  WHERE delivered = 1
  GROUP BY month_start
)
SELECT
  month_start,
  ROUND(avg_delivery_days, 2) AS avg_delivery_days,
  ROUND(avg_delivery_days - LAG(avg_delivery_days) OVER (ORDER BY month_start), 2) AS mom_change_days,
  ROUND(AVG(avg_delivery_days) OVER (ORDER BY month_start ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS ma_3mo
FROM monthly
ORDER BY month_start;

-- Customer Repeat
CREATE OR REPLACE VIEW v_customer_repeat_breakdown AS
WITH firsts AS (
  SELECT customer_key, MIN(DATE(order_date)) AS first_order_date
  FROM sales_enriched
  GROUP BY customer_key
),
period_customers AS (
  SELECT
    STR_TO_DATE(CONCAT(DATE_FORMAT(se.order_date, '%Y-%m'), '-01'), '%Y-%m-%d') AS month_start,
    st.country,
    pr.category,
    se.customer_key
  FROM sales_enriched se
  JOIN stores   st ON st.store_key  = se.store_key
  JOIN products pr ON pr.product_key = se.product_key
  GROUP BY month_start, st.country, pr.category, se.customer_key
),
classified AS (
  SELECT
    pc.month_start,
    pc.country,
    pc.category,
    CASE
      WHEN STR_TO_DATE(CONCAT(DATE_FORMAT(f.first_order_date, '%Y-%m'), '-01'), '%Y-%m-%d') = pc.month_start
        THEN 'New Customers'
      ELSE 'Repeat Customers'
    END AS status,
    pc.customer_key
  FROM period_customers pc
  JOIN firsts f ON f.customer_key = pc.customer_key
)
SELECT
  month_start, country, category, status,
  COUNT(DISTINCT customer_key) AS customers
FROM classified
GROUP BY month_start, country, category, status
ORDER BY month_start, country, category, status;

-- Monthly Revenue 
CREATE OR REPLACE VIEW v_fx_revenue_monthly AS
WITH month_rev AS (
  SELECT
    st.country,
    se.currency_code,
    STR_TO_DATE(CONCAT(DATE_FORMAT(se.order_date, '%Y-%m'), '-01'), '%Y-%m-%d') AS month_start,
    SUM(se.unit_price_usd * se.quantity) AS revenue_usd
  FROM sales_enriched se
  JOIN stores st ON st.store_key = se.store_key
  GROUP BY st.country, se.currency_code, month_start
),
month_fx AS (
  SELECT
    er.currency,
    STR_TO_DATE(CONCAT(DATE_FORMAT(er.date, '%Y-%m'), '-01'), '%Y-%m-%d') AS month_start,
    AVG(er.exchange) AS avg_fx
  FROM exchange_rates er
  GROUP BY er.currency, month_start
)
SELECT
  r.country,
  r.currency_code,
  r.month_start,
  ROUND(r.revenue_usd, 2) AS revenue_usd,
  ROUND(f.avg_fx, 6) AS avg_fx,
  ROUND(r.revenue_usd * f.avg_fx, 2) AS revenue_local_est
FROM month_rev r
LEFT JOIN month_fx f
  ON f.currency = r.currency_code
 AND f.month_start = r.month_start
ORDER BY r.country, r.currency_code, r.month_start;


-- each view returns rows
SELECT * FROM v_top_products_per_country LIMIT 5;
SELECT * FROM v_month_on_month_revenue LIMIT 5;

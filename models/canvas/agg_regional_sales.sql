WITH fct_orders AS (
  SELECT
    *
  FROM {{ ref('dbt-demo-platform', 'fct_orders') }}
), dim_customers AS (
  SELECT
    *
  FROM {{ ref('dbt-demo-platform', 'dim_customers') }}
), "join" AS (
  SELECT
    *
  FROM fct_orders
  LEFT JOIN dim_customers
    USING (CUSTOMER_KEY)
), aggregation AS (
  SELECT
    CUSTOMER_REGION,
    SUM(NET_ITEM_SALES_AMOUNT) AS NET_SALES
  FROM "join"
  GROUP BY
    CUSTOMER_REGION
), agg_regional_sales_sql AS (
  SELECT
    *
  FROM aggregation
)
SELECT
  *
FROM agg_regional_sales_sql
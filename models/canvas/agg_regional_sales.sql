WITH fct_orders AS (
  /* Core fact table for order line items. Grain: one row per order line (order_key + line_number). Contains only foreign keys, measures and degenerate dimensions. Join to dim_customers, dim_suppliers and dim_parts for descriptive attributes.
*/
  SELECT
    *
  FROM {{ ref('dbt-demo-platform', 'fct_orders') }}
), dim_customers AS (
  /* Customer dimension. Grain: one row per customer. Includes geography (nation and region) resolved from intermediate.
*/
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
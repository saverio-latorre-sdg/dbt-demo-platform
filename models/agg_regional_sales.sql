WITH fct_orders AS (
  SELECT
    *
  FROM {{ ref('fct_orders') }}
), dim_customers AS (
  SELECT
    *
  FROM {{ ref('dim_customers') }}
), join_1 AS (
  SELECT
    *
  FROM fct_orders
  LEFT JOIN dim_customers
    USING (CUSTOMER_KEY)
), aggregate_1 AS (
  SELECT
    CUSTOMER_REGION,
    SUM(NET_ITEM_SALES_AMOUNT) AS sum_NET_ITEM_SALES_AMOUNT
  FROM join_1
  GROUP BY
    CUSTOMER_REGION
), rename_1 AS (
  SELECT
    CUSTOMER_REGION AS CUSTOMER_REGION,
    sum_NET_ITEM_SALES_AMOUNT AS NET_SALES_AMOUNT,
    *
    EXCLUDE (CUSTOMER_REGION, sum_NET_ITEM_SALES_AMOUNT)
  FROM aggregate_1
), order_1 AS (
  SELECT
    *
  FROM rename_1
  ORDER BY
    NET_SALES_AMOUNT DESC
), formula_1 AS (
  SELECT
    *,
    RANK() OVER (ORDER BY NET_SALES_AMOUNT DESC) AS rn
  FROM order_1
), filter_1 AS (
  SELECT
    *
  FROM formula_1
  WHERE
    rn <= '3'
), agg_regional_sales_sql AS (
  SELECT
    *
  FROM filter_1
)
SELECT
  *
FROM agg_regional_sales_sql
WITH fct_orders AS (
  SELECT
    *
  FROM {{ ref('dbt-demo-platform', 'fct_orders') }}
), dim_customers AS (
  SELECT
    *
  FROM {{ ref('dbt-demo-platform', 'dim_customers') }}
), rename_1 AS (
  SELECT
    CUSTOMER_KEY AS FCT_ORDERS_CUSTOMER_KEY,
    *
    EXCLUDE (CUSTOMER_KEY)
  FROM fct_orders
), rename_2 AS (
  SELECT
    CUSTOMER_KEY AS DIM_CUSTOMERS_CUSTOMER_KEY,
    *
    EXCLUDE (CUSTOMER_KEY)
  FROM dim_customers
), join_1 AS (
  SELECT
    *
  FROM rename_1
  LEFT JOIN rename_2
    ON rename_1.FCT_ORDERS_CUSTOMER_KEY = rename_2.DIM_CUSTOMERS_CUSTOMER_KEY
), rename_3 AS (
  SELECT
    FCT_ORDERS_CUSTOMER_KEY AS CUSTOMER_KEY,
    CUSTOMER_REGION,
    NET_ITEM_SALES_AMOUNT
  FROM join_1
), aggregate_1 AS (
  SELECT
    CUSTOMER_REGION,
    SUM(NET_ITEM_SALES_AMOUNT) AS NET_SALES
  FROM rename_3
  GROUP BY
    CUSTOMER_REGION
), untitled_sql AS (
  SELECT
    CUSTOMER_REGION,
    NET_SALES
  FROM aggregate_1
)
SELECT
  *
FROM untitled_sql
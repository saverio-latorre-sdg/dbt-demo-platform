WITH int_order_items AS (
  SELECT
    *
  FROM {{ ref('int_order_items') }}
), formula_2d22 AS (
  SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(["order_key", "line_number"]) }} AS ORDER_ITEM_KEY,
    DATEDIFF(DAY, ORDER_DATE, SHIP_DATE) AS DAYS_TO_SHIP,
    DATEDIFF(DAY, SHIP_DATE, RECEIPT_DATE) AS DAYS_IN_TRANSIT
  FROM int_order_items
), fct_orders AS (
  SELECT
    ORDER_ITEM_KEY,
    ORDER_KEY,
    LINE_NUMBER,
    CUSTOMER_KEY,
    SUPPLIER_KEY,
    PART_KEY,
    ORDER_DATE,
    SHIP_DATE,
    COMMIT_DATE,
    RECEIPT_DATE,
    ORDER_STATUS_CODE,
    ORDER_PRIORITY_CODE,
    LINE_STATUS,
    SHIP_MODE,
    QUANTITY,
    EXTENDED_PRICE,
    DISCOUNT_PERCENTAGE,
    TAX_RATE,
    NET_ITEM_SALES_AMOUNT,
    GROSS_ITEM_SALES_AMOUNT,
    DAYS_TO_SHIP,
    DAYS_IN_TRANSIT
  FROM formula_2d22
)
SELECT
  *
FROM fct_orders
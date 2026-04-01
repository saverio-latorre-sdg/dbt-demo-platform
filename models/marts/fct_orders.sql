with order_items as (

    select * from {{ ref('int_order_items') }}

),

final as (

    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['order_key', 'line_number']) }} as order_item_key,

        -- foreign keys
        order_key,
        line_number,
        customer_key,
        supplier_key,
        part_key,

        -- dates
        order_date,
        ship_date,
        commit_date,
        receipt_date,

        -- degenerate dimensions (attributes of the event, no dedicated dim)
        order_status_code,
        order_priority_code,
        return_flag,
        line_status,
        ship_mode,

        -- measures
        quantity,
        extended_price,
        discount_percentage,
        tax_rate,
        net_item_sales_amount,
        gross_item_sales_amount,

        -- derived measures
        datediff('day', order_date, ship_date)      as days_to_ship,
        datediff('day', ship_date, receipt_date)    as days_in_transit

    from order_items

)

select * from final

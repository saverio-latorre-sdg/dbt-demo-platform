with
    lineitems as (select * from {{ ref("stg_line_items") }}),

    orders as (select * from {{ ref("stg_orders") }}),

    final as (

        select
            -- keys
            li.order_key,
            li.line_number,
            li.part_key,
            li.supplier_key,
            o.customer_key,

            -- dates
            o.order_date,
            li.ship_date,
            li.commit_date,
            li.receipt_date,

            -- degenerate dimensions
            o.status_code as order_status_code,
            o.priority_code as order_priority_code,
            case
                when li.return_flag = 'R'
                then 'returned'
                when li.return_flag = 'A'
                then 'accepted'
                else 'na'
            end as return_flag,
            li.line_status,
            li.ship_mode,

            -- measures
            li.quantity,
            li.extended_price,
            li.discount_percentage,
            li.tax_rate,
            li.net_item_sales_amount,
            li.gross_item_sales_amount

        from lineitems li
        inner join orders o on li.order_key = o.order_key

    )

select *
from final

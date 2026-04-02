with customers as (

    select * from {{ ref('int_customers') }}

),

final as (

    select
        customer_key,
        customer_name,
        customer_phone,
        customer_account_balance,
        market_segment,
        customer_nation,
        customer_region

    from customers

)

select * from final

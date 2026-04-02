with customers as (

    select * from {{ ref('stg_customers') }}

),

nations as (

    select * from {{ ref('stg_nations') }}

),

regions as (

    select * from {{ ref('stg_regions') }}

),

final as (

    select
        -- keys
        c.customer_key,
        c.nation_key,
        n.region_key,

        -- customer info
        c.customer_name,
        c.phone_number          as customer_phone,
        c.account_balance       as customer_account_balance,
        c.market_segment,

        -- geography
        n.nation_name           as customer_nation,
        r.region_name           as customer_region

    from customers c
    inner join nations n
        on c.nation_key = n.nation_key
    inner join regions r
        on n.region_key = r.region_key

)

select * from final

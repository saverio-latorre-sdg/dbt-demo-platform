with suppliers as (

    select * from {{ ref('stg_suppliers') }}

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
        s.supplier_key,
        s.nation_key,
        n.region_key,

        -- supplier info
        s.supplier_name,
        s.phone_number        as supplier_phone,
        s.account_balance     as supplier_account_balance,

        -- geography
        n.nation_name         as supplier_nation,
        r.region_name         as supplier_region

    from suppliers s
    inner join nations n
        on s.nation_key = n.nation_key
    inner join regions r
        on n.region_key = r.region_key

)

select * from final
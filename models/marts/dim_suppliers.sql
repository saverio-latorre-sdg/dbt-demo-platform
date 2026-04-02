with suppliers as (

    select * from {{ ref('int_suppliers') }}

),

final as (

    select
        supplier_key,
        supplier_name,
        supplier_phone,
        supplier_account_balance,
        supplier_nation,
        supplier_region

    from suppliers

)

select * from final

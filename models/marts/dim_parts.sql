with parts_suppliers as (

    select * from {{ ref('int_parts_suppliers') }}

),

final as (

    select
        -- key
        part_key,

        -- part attributes (same for all suppliers, take any)
        any_value(part_name)        as part_name,
        any_value(manufacturer)     as manufacturer,
        any_value(brand)            as brand,
        any_value(part_type)        as part_type,
        any_value(part_size)        as part_size,
        any_value(container_type)   as container_type,
        any_value(retail_price)     as retail_price,

        -- aggregated supply info across all suppliers
        count(supplier_key)                     as supplier_count,
        sum(available_quantity)                 as total_available_quantity,
        round(avg(supply_cost), 2)              as avg_supply_cost,
        min(supply_cost)                        as min_supply_cost,
        round(avg(gross_margin_percentage), 2)  as avg_gross_margin_percentage

    from parts_suppliers
    group by part_key

)

select * from final
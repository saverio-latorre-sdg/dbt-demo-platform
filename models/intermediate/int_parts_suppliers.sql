with
    parts as (select * from {{ ref("stg_parts") }}),

    partsupps as (select * from {{ ref("stg_part_suppliers") }}),

    final as (

        select
            -- keys (composite grain: part_key + supplier_key)
            p.part_key,
            ps.supplier_key,

            -- part attributes
            p.part_name,
            p.manufacturer,
            p.brand,
            p.part_type,
            p.part_size,
            p.container_type,
            p.retail_price,

            -- supply attributes
            ps.available_quantity,
            ps.supply_cost,

            -- derived
            round(p.retail_price - ps.supply_cost, 2) as gross_margin,
            round(
                (p.retail_price - ps.supply_cost) / nullif(p.retail_price, 0) * 100, 2
            ) as gross_margin_percentage

        from parts p
        inner join partsupps ps on p.part_key = ps.part_key

    )

select *
from final

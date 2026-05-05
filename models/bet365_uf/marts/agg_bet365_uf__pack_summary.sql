with

packs as (

    select 
        *,
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', pack_created_at) as date) as pack_created_date 
    from {{ ref('mart_bet365_uf__user_packs') }}
    where is_tester = false

),

agg_packs as (

    select
        pack_name,
        pack_created_date,
        count(*) as packs,
        count(distinct user_id) as pack_openers,
        case
            when grouping(pack_name) = 0 and grouping(pack_created_date) = 0 then 'DETAIL'
            when grouping(pack_name) = 0 and grouping(pack_created_date) = 1 then 'PACK_NAME_TOTAL'
            when grouping(pack_name) = 1 and grouping(pack_created_date) = 0 then 'DATE_TOTAL'
            else 'GRAND_TOTAL'
        end as grouping_level
    from packs
    group by grouping sets (
        (pack_name, pack_created_date),
        (pack_name),
        (pack_created_date),
        ()
    )

)

select
    coalesce(pack_name, 'TOTAL') as pack_name,
    coalesce(to_varchar(pack_created_date), null) as pack_created_date,
    packs,
    pack_openers,
    grouping_level
from agg_packs

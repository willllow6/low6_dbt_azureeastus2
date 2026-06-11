with

group_contests as (

    select * from {{ ref('stg_gana_gamezone__group_contests') }}

),

-- Flatten the countries array to get the member teams per group
group_members as (

    select
        gc.group_id,
        gc.group_name,
        listagg(c.value:country_id::varchar, ',') within group (order by c.index)   as member_country_ids,
        count(c.index)                                                              as member_count
    from group_contests as gc,
    lateral flatten(input => gc.countries) as c
    group by gc.group_id, gc.group_name

)

select
    gc.group_id,
    gc.group_name,
    gc.description,
    gc.countries,
    gm.member_country_ids,
    gm.member_count,
    gc.created_at,
    gc.updated_at
from group_contests as gc
left join group_members as gm
    on gc.group_id = gm.group_id

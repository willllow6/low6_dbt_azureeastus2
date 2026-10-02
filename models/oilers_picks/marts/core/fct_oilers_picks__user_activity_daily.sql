with

entries as (

    select *
    from {{ ref('fct_oilers_picks__entries') }}

),

aggregated as (

    select
        user_id,
        entry_date_et as activity_date_et,

        client_id,
        tenant_id,
        tenant_name,
        game_type,

        count(*) as entries,
        sum(entry_points) as entry_points,

        min(user_entry_number) = 1 as is_first_entry_day,

        min(entered_at) as first_entered_at,
        max(entered_at) as last_entered_at

    from entries
    group by 1, 2, 3, 4, 5, 6

)

select
    user_id,
    activity_date_et,

    client_id,
    tenant_id,
    tenant_name,
    game_type,

    entries,
    entry_points,

    is_first_entry_day,

    first_entered_at,
    last_entered_at

from aggregated

with

entries as (

    select *
    from {{ ref('fct_bet365_overunder__entries') }}

),

aggregated as (

    select
        user_id,
        entry_date_et as activity_date_et,

        client_id,
        tenant_id,
        game_type,

        country,

        count(*) as entries,
        sum(entered_picks) as entered_picks,
        count_if(is_winner) as winning_entries,
        sum(prize_amount) as prize_amount,

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
    game_type,

    country,

    entries,
    entered_picks,
    winning_entries,
    prize_amount,

    is_first_entry_day,

    first_entered_at,
    last_entered_at

from aggregated

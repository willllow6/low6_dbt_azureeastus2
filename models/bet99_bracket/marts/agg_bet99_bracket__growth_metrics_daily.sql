with

date_spine as (

    select dateadd(day, seq4(), '{{ var("bet99_bracket_start_date") }}'::date) as date_day
    from table(generator(rowcount => 700))
    where date_day <= current_date

),

dates as (

    select cast(date_day as date) as date_day
    from date_spine

),

registrations as (

    select
        created_date_et     as date_day,
        count(*)            as registrations
    from {{ ref('dim_bet99_bracket__users') }}
    group by 1

),

entries_ranked as (

    select
        user_id,
        created_date_et,
        row_number() over (partition by user_id order by created_at) as entry_rank
    from {{ ref('fct_bet99_bracket__entries') }}

),

entries as (

    select
        created_date_et                                                     as date_day,
        count(distinct case when entry_rank = 1 then user_id end)           as first_entries,
        count(*)                                                             as total_entries
    from entries_ranked
    group by 1

),

leagues_created as (

    select
        league_created_date_et  as date_day,
        count(*)                as leagues_created
    from {{ ref('dim_bet99_bracket__leagues') }}
    where league_name != 'Overall League'
    group by 1

),

league_joins as (

    select
        lm.league_joined_date_et    as date_day,
        count(*)                    as league_joins
    from {{ ref('fct_bet99_bracket__league_members') }} as lm
    inner join {{ ref('dim_bet99_bracket__leagues') }} as l
        on lm.league_id = l.league_id
    where l.league_name != 'Overall League'
    group by 1

)

select
    d.date_day,
    'bet99'         as client_id,
    'bet99'         as tenant_id,
    'Bet99'         as tenant_name,
    'bracket'       as game_type,
    coalesce(r.registrations,       0)  as registrations,
    coalesce(e.first_entries,       0)  as first_entries,
    coalesce(e.total_entries,       0)  as total_entries,
    coalesce(lc.leagues_created,    0)  as leagues_created,
    coalesce(lj.league_joins,       0)  as league_joins
from dates as d
left join registrations as r        on d.date_day = r.date_day
left join entries as e              on d.date_day = e.date_day
left join leagues_created as lc     on d.date_day = lc.date_day
left join league_joins as lj        on d.date_day = lj.date_day

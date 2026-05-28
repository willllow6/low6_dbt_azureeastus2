with date_spine as (
        select dateadd(day, seq4(), '{{ var("cfl_fantasy_start_date") }}'::date) as date_day
    from table(generator(rowcount => 700))
    where date_day <= current_date
),

dates as (
    select cast(date_day as date) as date_day
    from date_spine
),

registrations as (
    select
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', registered_at) as date) as date_day,
        count(*) as new_registrations
    from {{ ref('stg_cfl_fantasy__users') }}
    where is_user = true
    group by 1
),

teams as (
    select
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', entered_at) as date) as date_day,
        count(*)                             as teams_created,
        count_if(is_auto_team = true)        as bot_teams_created,
        count_if(is_auto_team = false)       as user_teams_created
    from {{ ref('stg_cfl_fantasy__teams') }}
    group by 1
),

user_first_team_created as (

    select
        user_id,
        min(entered_at) as first_entered_at
    from {{ ref('stg_cfl_fantasy__teams') }}
    group by 1

),

first_drafts as (

    select
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', first_entered_at) as date) as date_day,
        count(*) as first_drafts
    from user_first_team_created
    group by 1

),

leagues as (
    select
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', created_at) as date) as date_day,
        count(*)                             as leagues_created,
        count_if(max_teams = 4)              as four_team_leagues_created,
        count_if(max_teams = 6)              as six_team_leagues_created
    from {{ ref('stg_cfl_fantasy__leagues') }}
    group by 1
),

final as (
    select
        d.date_day,
        'cfl'     as client_id,
        'cfl'     as tenant_id,
        'CFL'     as tenant_name,
        'fantasy' as game_type,
        coalesce(r.new_registrations,          0) as new_registrations,
        coalesce(fd.first_drafts,              0) as first_drafts,
        coalesce(t.teams_created,              0) as teams_created,
        coalesce(t.bot_teams_created,          0) as bot_teams_created,
        coalesce(t.user_teams_created,         0) as user_teams_created,
        coalesce(l.leagues_created,            0) as leagues_created,
        coalesce(l.four_team_leagues_created,  0) as four_team_leagues_created,
        coalesce(l.six_team_leagues_created,   0) as six_team_leagues_created
    from dates d
    left join registrations r on d.date_day = r.date_day
    left join teams t         on d.date_day = t.date_day
    left join leagues l       on d.date_day = l.date_day
    left join first_drafts fd on d.date_day = fd.date_day
)

select * from final

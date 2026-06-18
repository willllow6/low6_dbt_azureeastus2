with rosters as (
    select * from {{ ref('stg_cfl_fantasy__rosters') }}
),

teams as (
    select
        entry_id,
        is_auto_team
    from {{ ref('stg_cfl_fantasy__teams') }}
),

-- exclude roster spots belonging to auto-filled bot teams
human_rosters as (
    select r.*
    from rosters r
    inner join teams t on r.entry_id = t.entry_id
    where t.is_auto_team = false
),

players as (
    select * from {{ ref('stg_cfl_fantasy__players') }}
),

sport_teams as (
    select
        team_abbreviation,
        team_name,
        team_city_name,
        team_short_name
    from {{ ref('stg_cfl_fantasy__sport_teams') }}
),

roster_stats as (
    select
        player_id,
        fantasy_week,
        count(*) as total_roster_spots,
        count(distinct entry_id) as teams_rostered_on,
        count(distinct contest_id) as leagues_present_in,
        sum(case when is_bench = false then 1 else 0 end) as starter_slots,
        sum(case when is_bench = true then 1 else 0 end) as bench_slots
    from human_rosters
    group by 1, 2
),

enriched as (
    select
        rs.player_id,
        rs.fantasy_week,
        p.full_name as player_name,
        p.first_name,
        p.last_name,
        p.position,
        p.team_code,
        p.jersey_number,
        p.is_canadian,
        p.is_national,
        p.player_status,
        p.is_platform_active,
        st.team_name as real_team_name,
        st.team_city_name,
        st.team_short_name,
        rs.total_roster_spots,
        rs.teams_rostered_on,
        rs.leagues_present_in,
        rs.starter_slots,
        rs.bench_slots

    from roster_stats rs
    inner join players p on rs.player_id = p.player_id
    left join sport_teams st on p.team_code = st.team_abbreviation
)

select * from enriched
order by teams_rostered_on desc

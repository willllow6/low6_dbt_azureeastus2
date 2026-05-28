with draft_picks as (
    select * from {{ ref('stg_cfl_fantasy__draft_picks') }}
),

teams as (
    select
        entry_id,
        is_auto_team
    from {{ ref('stg_cfl_fantasy__teams') }}
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

-- exclude picks made by auto-filled bot teams
human_picks as (
    select dp.*
    from draft_picks dp
    inner join teams t on dp.team_id = t.entry_id
    where t.is_auto_team = false
),

pick_stats as (
    select
        player_id,
        count(distinct draft_id) as times_drafted,
        avg(pick_number) as adp,
        min(pick_number) as best_pick_number,
        max(pick_number) as worst_pick_number,
        avg(draft_round) as avg_round
    from human_picks
    group by 1
),

enriched as (
    select
        ps.player_id,
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
        ps.times_drafted,
        ps.adp,
        ps.best_pick_number,
        ps.worst_pick_number,
        ps.avg_round

    from pick_stats ps
    inner join players p on ps.player_id = p.player_id
    left join sport_teams st on p.team_code = st.team_abbreviation
)

select * from enriched
order by adp asc

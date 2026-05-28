with players as (
    select * from {{ ref('stg_cfl_fantasy__players') }}
),

sport_teams as (
    select
        sport_team_id,
        team_abbreviation,
        team_name,
        team_short_name,
        team_city_name
    from {{ ref('stg_cfl_fantasy__sport_teams') }}
),

enriched as (
    select
        p.player_id,
        p.external_player_id,
        p.sport_key,
        p.team_external_id,
        p.full_name,
        p.first_name,
        p.last_name,
        p.position,
        p.team_code,
        p.country_code,
        p.nationality,
        p.player_status,
        p.jersey_number,
        p.is_national,
        p.is_canadian,
        p.is_platform_active,
        p.metadata,
        p.updated_metadata,
        p.created_at,
        st.sport_team_id,
        st.team_name as real_team_name,
        st.team_short_name,
        st.team_city_name

    from players p
    left join sport_teams st on p.team_code = st.team_abbreviation
)

select * from enriched

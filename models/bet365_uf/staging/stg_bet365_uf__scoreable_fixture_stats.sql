with

source as (

    select *
    from {{ source('bet365_uf', 'SCOREABLE_STATS') }}

),

renamed as (

    select

        ----------  ids
        ScoreableStatsId as scoreable_stats_id,
        ScoreableId as scoreable_id,
        FixtureId as fixture_id,
        'bet365' as client_id,

        ---------- strings
        Season as season,

        ---------- numerics
        Stat_0 as player_goals,
        Stat_1 as player_assists,
        Stat_2 as player_shots,
        Stat_3 as player_hits,
        Stat_4 as player_blocked_shots,
        Stat_5 as team_win,
        Stat_6 as team_power_play_goals,
        Stat_7 as team_short_handed_goals,
        Stat_8 as team_goals_against,
        Stat_9 as team_shutout,
        Stat_10 as team_saves,

        ---------- dates
        cast(CreatedAt as date) as created_date,

        ---------- timestamps
        CreatedAt as created_at

    from source

)

select * from renamed

with

completed_pickem_entries as (

    select * from {{ ref('oilers_picks__entries') }} where pickem_status = 'COMPLETE'

),

overall_leaderboards as (

    select * from {{ ref('stg_oilers_picks__leaderboards') }} where league_id = 1

),

calculate_overall_leaderboard_points as (

    select
        leaderboard_id,
        league_id,
        leaderboard_name,
        leaderboard_type,
        leaderboard_starts_at,
        leaderboard_ends_at,
        entries.user_id,
        entries.low6_username,
        entries.first_name,
        entries.last_name,
        entries.email,
        count(*) as entries,
        sum(entries.entry_points) as total_points
    from overall_leaderboards as leaderboard 
        left join completed_pickem_entries as entries 
            on leaderboard.leaderboard_starts_at <= entries.pickem_starts_at
            and leaderboard.leaderboard_ends_at >= entries.pickem_starts_at
    group by 1,2,3,4,5,6,7,8,9,10,11

),

union_leaderboards as (

    select
        null as leaderboard_id,
        null as league_id,
        pickem_title as leaderboard_name,
        'CONTEST' as leaderboard_type,
        pickem_starts_at as leaderboard_starts_at,
        pickem_starts_at as leaderboard_ends_at,
        user_id,
        low6_username,
        first_name,
        last_name,
        email,
        1 as entries,
        entry_points as total_points,
        tiebreak_error
    from completed_pickem_entries

    union all

    select
        leaderboard_id,
        league_id,
        leaderboard_name,
        leaderboard_type,
        leaderboard_starts_at,
        leaderboard_ends_at,
        user_id,
        low6_username,
        first_name,
        last_name,
        email,
        entries,
        total_points,
        null as tiebreak_error
    from calculate_overall_leaderboard_points

),

calculate_leaderboard_positions as (

    select
        *,
        rank() over(
            partition by leaderboard_name
            order by total_points desc, tiebreak_error asc
        ) as leaderboard_position
    from union_leaderboards

)

select * from calculate_leaderboard_positions






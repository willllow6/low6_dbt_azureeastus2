with

entries as (

    select * from {{ ref('INT_ELF_BLAST__ENTRIES')}}

),

time_diff_flags as (

    select
        *,
        case
            when lag(entry_starts_at_utc) over (partition by sso_user_id order by entry_starts_at_utc) is null
                then 1
            when timediff(second, lag(entry_starts_at_utc) over (partition by sso_user_id order by entry_starts_at_utc),entry_starts_at_utc) > 1800
                then 1
            else 0
        end as time_diff_flag
    from entries

),

time_diff_partitions as (

    select
        *,
        sum(time_diff_flag) over (partition by sso_user_id order by entry_starts_at_utc rows between unbounded preceding and current row) as time_diff_partition
    from time_diff_flags
        
),

session_ids as (

    select
        *,
        first_value(entry_id) over (partition by sso_user_id, time_diff_partition order by entry_starts_at_utc) as session_id,
        last_value(level_name) over (partition by sso_user_id, time_diff_partition order by entry_ends_at_utc) as session_abandoned_level
    from time_diff_partitions
    
),

user_sessions as (

    select 
        session_id,
        user_id,
        player_id,
        sso_user_id,
        username,
        email,
        tenant,
        is_user_active,
        is_user_deleted,
        has_completed_tutorial,
        session_abandoned_level,
        min(entry_starts_at_utc) as session_starts_at_utc,
        max(entry_starts_at_utc) as last_entry_starts_at_utc,
        max(entry_ends_at_utc) as last_entry_ends_at_utc,
        case
            when datediff(seconds,max(entry_starts_at_utc),max(entry_ends_at_utc)) > 550
                then max(entry_starts_at_utc)
            else max(entry_ends_at_utc)
        end as session_ends_at_utc,
        case 
            when datediff(seconds,session_starts_at_utc,session_ends_at_utc) = 0
                then null
            else datediff(seconds,session_starts_at_utc,session_ends_at_utc)
        end as session_elapsed_time_seconds,
        cast(session_starts_at_utc as date) as session_date_utc,
        hour(session_starts_at_utc) as session_hour_utc,
        convert_timezone('UTC','America/New_York',session_starts_at_utc) as session_starts_at_et,
        cast(session_starts_at_et as date) as session_date_et,
        hour(session_starts_at_et) as session_hour_et,
        count(*) as levels_played
    from session_ids
    group by 1,2,3,4,5,6,7,8,9,10,11

),

ranked as (

    select
        *,
        row_number() over (partition by user_id order by session_starts_at_utc) as user_session_number
    from user_sessions

)

select * from ranked
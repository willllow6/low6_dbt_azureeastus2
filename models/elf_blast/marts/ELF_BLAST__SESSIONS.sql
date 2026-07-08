with

user_sessions as (

    select * from {{ ref('INT_ELF_BLAST__SESSIONS') }}

),

final as (

    select
        tenant,
        session_date_utc,
        session_date_et,
        session_hour_et,
        case 
            when user_session_number = 1
                then 'First Session'
            else 'Subsequent Session'
        end as session_type,
        count(*) as sesssions,
        sum(case when user_session_number = 1 then 1 else 0 end) as users,
        sum(session_elapsed_time_seconds) as total_elapsed_time,
        sum(case when session_elapsed_time_seconds is not null then 1 else 0 end) as timed_sessions
    from user_sessions
    group by 1,2,3,4,5

)

select * from final
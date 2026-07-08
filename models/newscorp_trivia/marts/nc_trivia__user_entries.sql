with

user_selections as (

    select * 
    from {{ ref('nc_trivia__user_selections') }}

),

selections_to_entries as (

    select
        contest_id,
        user_id,
        service_user_id,
        sso_user_id,
        username,
        nickname,
        contest_title,
        contest_status,
        contest_start_date_aet,
        min(started_at_aet) as entered_at_aet,
        cast(entered_at_aet as date) as entered_date_aet,
        dayname(entered_at_aet) as entered_day_aet,
        hour(entered_at_aet) as entered_hour_aet
    from user_selections
    group by 1,2,3,4,5,6,7,8,9

),

rank_user_entries as (

    select
        *,
        row_number()
            over (partition by user_id order by entered_at_aet)
            as user_entry_number
    from selections_to_entries

),

entry_type as (

    select
        contest_id,
        user_id,
        service_user_id,
        sso_user_id,
        username,
        nickname,
        contest_title,
        contest_status,
        contest_start_date_aet,
        entered_at_aet,
        entered_date_aet,
        entered_day_aet,
        entered_hour_aet,
        user_entry_number,
        case
            when user_entry_number = 1
                then 'First Entry'
            else 'Subsequent Entry'
        end as user_entry_type

    from rank_user_entries

)

select * from entry_type

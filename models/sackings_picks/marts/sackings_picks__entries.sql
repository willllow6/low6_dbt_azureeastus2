with

--user information from  user table
users as (

    select * 
    from {{ ref('stg_sackings_picks__users') }}

),

--filter to current 
bets as (

    select * 
    from {{ ref('stg_sackings_picks__entry_scores') }} 
    where is_active = 'TRUE'

),

status_type as (

    select * 
    from {{ ref('stg_sackings_picks__list_types') }}

),

responses as (

    select distinct
        user_id,
        contest_id 
    from {{ ref('stg_sackings_picks__selections') }} 
    where is_active = 'TRUE'

),

first_entries as (

    select
        user_id,
        contest_id,
        MIN(selected_at_et)  as entry_at
    from {{ ref('stg_sackings_picks__selections') }} 
    group by user_id, contest_id

),

--information for contest
contest as (

    select * 
    from {{ ref('stg_sackings_picks__contests') }} 

),

--to get the name of the account the pick is on - will then need to do a lookup on to what we've called it in all our bidness
account as (

   select * 
   from {{ ref('stg_sackings_picks__accounts') }} 
   where is_active = 'True'
),

user_entries as (

    select distinct
        r.user_id||r.contest_id as entry_id,
        u.account_id,
        u.user_id,
        r.contest_id,
        c.contest_name,
        acc.account_name as app_entered_on,
        list_type_text as pickem_status,
        acc.account_name as pickem_available_on,
        c.entry_fee,
        case when c.entry_fee > 0 then 1 else 0 end as has_wager,
        b.points,
        cast(fe.entry_at as date) as entry_date,
        cast(fe.entry_at as time) as entry_time,
        c.contest_date as pickem_start_date
    from responses as r
    left join bets as b
        on r.user_id=b.user_id 
        and r.contest_id=b.contest_id
    left join contest as c
        on r.contest_id=c.contest_id
    inner join account as acc
        on c.account_id=acc.account_id
    inner join users as u 
        on r.user_id=u.user_id
    left join first_entries as fe
        on r.user_id=fe.user_id 
        and r.contest_id=fe.contest_id
    left join status_type as s 
        on c.status_id = s.list_type_id

),

final as (

    select
    *,
    rank() over(
        partition by user_id, app_entered_on
        order by entry_date, entry_id
        ) as user_entry_number_app
    from user_entries
    
)


select * from final


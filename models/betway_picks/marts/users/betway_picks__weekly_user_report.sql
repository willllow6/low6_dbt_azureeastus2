with 

last_weeks_entries as (

    select *
    from {{ ref('betway_picks__entries') }}
    where date_trunc('week',contest_start_date) =  dateadd('week',-1,date_trunc('week',current_date())) -- entries from last week's contests

),

weekly_user_report as (

    select
        betway_SubscriberKey as SubscriberKey,
        betway_UserId as UserId,
        betway_CasinoId as CasinoId,
        date_trunc('week',contest_start_date) as week_commencing,
        region,
        count(*) as numnber_of_weekly_entries
    from last_weeks_entries
    group by 1,2,3,4,5

)

select * from last_weeks_entries
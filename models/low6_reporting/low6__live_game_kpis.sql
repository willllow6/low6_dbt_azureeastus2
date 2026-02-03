with

bet365_overunder as (

    select
        '1195' as app_id,
        count(*) as entries,
        count(case when cast(created_at as date) = current_date() - 1 then id else null end) as yesterday_entries,
        count(case when created_at >= current_date() - 8 and created_at < current_date() then id else null end) as last_7_days_entries,
        count(case when created_at >= current_date() - 29 and created_at < current_date() then id else null end) as last_28_days_entries,
        count(distinct user_id) as entrants,
        count(distinct case when cast(created_at as date) = current_date() - 1 then user_id else null end) as yesterday_entrants,
        count(distinct case when created_at >= current_date() - 8 and created_at < current_date() then user_id else null end) as last_7_days_entrants,
        count(distinct case when created_at >= current_date() - 29 and created_at < current_date() then user_id else null end) as last_28_days_entrants,
        count(distinct cast(created_at as date)) as contests,
        max(cast(created_at as date)) as last_entry_date
    from  {{ source('bet365_overunder', 'entries') }}
    group by 1

),

betway_picks as (

    select
        '1203' as app_id,
        count(distinct user_id || '-' || pickem_id) as entries,
        count(distinct case when cast(created_at as date) = current_date() - 1 then user_id || '-' || pickem_id else null end) as yesterday_entries,
        count(distinct case when created_at >= current_date() - 8 and created_at < current_date() then user_id || '-' || pickem_id else null end) as last_7_days_entries,
        count(distinct case when created_at >= current_date() - 29 and created_at < current_date() then user_id || '-' || pickem_id else null end) as last_28_days_entries,
        count(distinct user_id) as entrants,
        count(distinct case when cast(created_at as date) = current_date() - 1 then user_id else null end) as yesterday_entrants,
        count(distinct case when created_at >= current_date() - 8 and created_at < current_date() then user_id else null end) as last_7_days_entrants,
        count(distinct case when created_at >= current_date() - 29 and created_at < current_date() then user_id else null end) as last_28_days_entrants,
        count(distinct pickem_id) as contests,
        max(cast(created_at as date)) as last_entry_date
    from  {{ source('betway_picks', 'user_selections') }}
    group by 1

),

elf_collectyourelf as (

    select
        '1102' as app_id,
        count(distinct user_id || '-' || challenge_id) as entries,
        count(distinct case when cast(created_at as date) = current_date() - 1 then user_id || '-' || challenge_id else null end) as yesterday_entries,
        count(distinct case when created_at >= current_date() - 8 and created_at < current_date() then user_id || '-' || challenge_id else null end) as last_7_days_entries,
        count(distinct case when created_at >= current_date() - 29 and created_at < current_date() then user_id || '-' || challenge_id else null end) as last_28_days_entries,
        count(distinct user_id) as entrants,
        count(distinct case when cast(created_at as date) = current_date() - 1 then user_id else null end) as yesterday_entrants,
        count(distinct case when created_at >= current_date() - 8 and created_at < current_date() then user_id else null end) as last_7_days_entrants,
        count(distinct case when created_at >= current_date() - 29 and created_at < current_date() then user_id else null end) as last_28_days_entrants,
        count(distinct challenge_id) as contests,
        max(cast(created_at as date)) as last_entry_date
    from  {{ source('collectyourelf','user_challenge_progress') }}
    group by 1

),

fanstake_rivals as (

    select
        '1200' as app_id,
        count(distinct id) as entries,
        count(distinct case when cast(created_at as date) = current_date() - 1 then id else null end) as yesterday_entries,
        count(distinct case when created_at >= current_date() - 8 and created_at < current_date() then id else null end) as last_7_days_entries,
        count(distinct case when created_at >= current_date() - 29 and created_at < current_date() then id else null end) as last_28_days_entries,
        count(distinct user_id) as entrants,
        count(distinct case when cast(created_at as date) = current_date() - 1 then user_id else null end) as yesterday_entrants,
        count(distinct case when created_at >= current_date() - 8 and created_at < current_date() then user_id else null end) as last_7_days_entrants,
        count(distinct case when created_at >= current_date() - 29 and created_at < current_date() then user_id else null end) as last_28_days_entrants,
        count(distinct weekly_period_id) as contests,
        max(cast(created_at as date)) as last_entry_date
    from  {{ source('fanstake_rivals', 'user_weekly_lineups') }}
    group by 1

),

oilers_picks as (

    select
        'US51' as app_id,
        count(distinct userid || '-' || pickemid) as entries,
        count(distinct case when cast(createdat as date) = current_date() - 1 then userid || '-' || pickemid else null end) as yesterday_entries,
        count(distinct case when createdat >= current_date() - 8 and createdat < current_date() then userid || '-' || pickemid else null end) as last_7_days_entries,
        count(distinct case when createdat >= current_date() - 29 and createdat < current_date() then userid || '-' || pickemid else null end) as last_28_days_entries,
        count(distinct userid) as entrants,
        count(distinct case when cast(createdat as date) = current_date() - 1 then userid else null end) as yesterday_entrants,
        count(distinct case when createdat >= current_date() - 8 and createdat < current_date() then userid else null end) as last_7_days_entrants,
        count(distinct case when createdat >= current_date() - 29 and createdat < current_date() then userid else null end) as last_28_days_entrants,
        count(distinct pickemid) as contests,
        max(cast(createdat as date)) as last_entry_date
    from  {{ source('oilers_picks', 'userselections') }}
    group by 1

),

sac_kings as (

    select
        'US64' as app_id,
        count(distinct customerid || '-' || contestid) as entries,
        count(distinct case when cast(creationdate as date) = current_date() - 1 then customerid || '-' || contestid else null end) as yesterday_entries,
        count(distinct case when creationdate >= current_date() - 8 and creationdate < current_date() then customerid || '-' || contestid else null end) as last_7_days_entries,
        count(distinct case when creationdate >= current_date() - 29 and creationdate < current_date() then customerid || '-' || contestid else null end) as last_28_days_entries,
        count(distinct customerid) as entrants,
        count(distinct case when cast(creationdate as date) = current_date() - 1 then customerid else null end) as yesterday_entrants,
        count(distinct case when creationdate >= current_date() - 8 and creationdate < current_date() then customerid else null end) as last_7_days_entrants,
        count(distinct case when creationdate >= current_date() - 29 and creationdate < current_date() then customerid else null end) as last_28_days_entrants,
        count(distinct contestid) as contests,
        max(cast(creationdate as date)) as last_entry_date
    from {{ source('sackings_picks', 'cutsomerresponse') }}
    where  creationdate >= '2025-10-20'
    group by 1

),

unioned as (

    select *
    from bet365_overunder

    union all

    select *
    from betway_picks

    union all

    select *
    from elf_collectyourelf

    union all

    select *
    from fanstake_rivals

    union all

    select *
    from oilers_picks

    union all

    select *
    from sac_kings

)


select * from unioned
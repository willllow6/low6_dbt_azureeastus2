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

bet99_picks as (

    select
        'US4' as app_id,
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
    from  {{ source('bet99_picks', 'user_selections') }}
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

-- oilers_picks as (

--     select
--         'US51' as app_id,
--         count(distinct userid || '-' || pickemid) as entries,
--         count(distinct case when cast(createdat as date) = current_date() - 1 then userid || '-' || pickemid else null end) as yesterday_entries,
--         count(distinct case when createdat >= current_date() - 8 and createdat < current_date() then userid || '-' || pickemid else null end) as last_7_days_entries,
--         count(distinct case when createdat >= current_date() - 29 and createdat < current_date() then userid || '-' || pickemid else null end) as last_28_days_entries,
--         count(distinct userid) as entrants,
--         count(distinct case when cast(createdat as date) = current_date() - 1 then userid else null end) as yesterday_entrants,
--         count(distinct case when createdat >= current_date() - 8 and createdat < current_date() then userid else null end) as last_7_days_entrants,
--         count(distinct case when createdat >= current_date() - 29 and createdat < current_date() then userid else null end) as last_28_days_entrants,
--         count(distinct pickemid) as contests,
--         max(cast(createdat as date)) as last_entry_date
--     from  {{ source('oilers_picks', 'userselections') }}
--     group by 1

-- ),

-- sac_kings as (

--     select
--         'US64' as app_id,
--         count(distinct customerid || '-' || contestid) as entries,
--         count(distinct case when cast(creationdate as date) = current_date() - 1 then customerid || '-' || contestid else null end) as yesterday_entries,
--         count(distinct case when creationdate >= current_date() - 8 and creationdate < current_date() then customerid || '-' || contestid else null end) as last_7_days_entries,
--         count(distinct case when creationdate >= current_date() - 29 and creationdate < current_date() then customerid || '-' || contestid else null end) as last_28_days_entries,
--         count(distinct customerid) as entrants,
--         count(distinct case when cast(creationdate as date) = current_date() - 1 then customerid else null end) as yesterday_entrants,
--         count(distinct case when creationdate >= current_date() - 8 and creationdate < current_date() then customerid else null end) as last_7_days_entrants,
--         count(distinct case when creationdate >= current_date() - 29 and creationdate < current_date() then customerid else null end) as last_28_days_entrants,
--         count(distinct contestid) as contests,
--         max(cast(creationdate as date)) as last_entry_date
--     from {{ source('sackings_picks', 'cutsomerresponse') }}
--     where  creationdate >= '2025-10-20'
--     group by 1

-- ),

saracen_picks as (

    select
        'spkm' as app_id,
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
    from  {{ source('saracen_picks', 'user_selections') }}
    group by 1

),

saracen_bracket as (

    select
        'sbktwc' as app_id,
        count(*) as entries,
        count_if(cast(createdat as date) = current_date() - 1) as yesterday_entries,
        count_if(createdat >= current_date() - 8 and createdat < current_date()) as last_7_days_entries,
        count_if(createdat >= current_date() - 29 and createdat < current_date() ) as last_28_days_entries,
        count(distinct userid) as entrants,
        count(distinct case when cast(createdat as date) = current_date() - 1 then userid else null end) as yesterday_entrants,
        count(distinct case when createdat >= current_date() - 8 and createdat < current_date() then userid else null end) as last_7_days_entrants,
        count(distinct case when createdat >= current_date() - 29 and createdat < current_date() then userid else null end) as last_28_days_entrants,
        count(distinct competition) as contests,
        max(cast(createdat as date)) as last_entry_date
    from  {{ source('saracen_bracket', 'userselections') }}
    group by 1

),

cfl_fantasy as (

    select
        'cflfan' as app_id,
        count(distinct id) as entries,
        count(distinct case when cast(createdat as date) = current_date() - 1 then id else null end) as yesterday_entries,
        count(distinct case when createdat >= current_date() - 8 and createdat < current_date() then id else null end) as last_7_days_entries,
        count(distinct case when createdat >= current_date() - 29 and createdat < current_date() then id else null end) as last_28_days_entries,
        count(distinct userid) as entrants,
        count(distinct case when cast(createdat as date) = current_date() - 1 then userid else null end) as yesterday_entrants,
        count(distinct case when createdat >= current_date() - 8 and createdat < current_date() then userid else null end) as last_7_days_entrants,
        count(distinct case when createdat >= current_date() - 29 and createdat < current_date() then userid else null end) as last_28_days_entrants,
        count(distinct leagueid) as contests,
        max(cast(createdat as date)) as last_entry_date
    from  {{ source('cfl_fantasy', 'teams') }}
    where isAutoTeam = false
    group by 1

),

opap_s2w as (

    select
        'opaps2w' as app_id,
        count(distinct id) as entries,
        count(distinct case when cast(submitted_at as date) = current_date() - 1 then id else null end) as yesterday_entries,
        count(distinct case when submitted_at >= current_date() - 8 and submitted_at < current_date() then id else null end) as last_7_days_entries,
        count(distinct case when submitted_at >= current_date() - 29 and submitted_at < current_date() then id else null end) as last_28_days_entries,
        count(distinct user_id) as entrants,
        count(distinct case when cast(submitted_at as date) = current_date() - 1 then user_id else null end) as yesterday_entrants,
        count(distinct case when submitted_at >= current_date() - 8 and submitted_at < current_date() then user_id else null end) as last_7_days_entrants,
        count(distinct case when submitted_at >= current_date() - 29 and submitted_at < current_date() then user_id else null end) as last_28_days_entrants,
        count(distinct contest_id) as contests,
        max(cast(submitted_at as date)) as last_entry_date
    from  {{ source('opap_spintowin', 'entries') }}
    group by 1

),

gana_predictor as (

    select
        'ganapdct' as app_id,
        count(distinct id) as entries,
        count(distinct case when cast(created_at as date) = current_date() - 1 then id else null end) as yesterday_entries,
        count(distinct case when created_at >= current_date() - 8 and created_at < current_date() then id else null end) as last_7_days_entries,
        count(distinct case when created_at >= current_date() - 29 and created_at < current_date() then id else null end) as last_28_days_entries,
        count(distinct user_id) as entrants,
        count(distinct case when cast(created_at as date) = current_date() - 1 then user_id else null end) as yesterday_entrants,
        count(distinct case when created_at >= current_date() - 8 and created_at < current_date() then user_id else null end) as last_7_days_entrants,
        count(distinct case when created_at >= current_date() - 29 and created_at < current_date() then user_id else null end) as last_28_days_entrants,
        1 as contests,
        max(cast(created_at as date)) as last_entry_date
    from  {{ source('gana_gamezone', 'group_predictions') }}
    group by 1

),

gana_survivor as (

    select
        'ganasurv' as app_id,
        count(distinct id) as entries,
        count(distinct case when cast(created_at as date) = current_date() - 1 then id else null end) as yesterday_entries,
        count(distinct case when created_at >= current_date() - 8 and created_at < current_date() then id else null end) as last_7_days_entries,
        count(distinct case when created_at >= current_date() - 29 and created_at < current_date() then id else null end) as last_28_days_entries,
        count(distinct user_id) as entrants,
        count(distinct case when cast(created_at as date) = current_date() - 1 then user_id else null end) as yesterday_entrants,
        count(distinct case when created_at >= current_date() - 8 and created_at < current_date() then user_id else null end) as last_7_days_entrants,
        count(distinct case when created_at >= current_date() - 29 and created_at < current_date() then user_id else null end) as last_28_days_entrants,
        count(distinct survivor_match_id) as contests,
        max(cast(created_at as date)) as last_entry_date
    from  {{ source('gana_gamezone', 'survivor_predictions') }}
    group by 1

),

-- bet99_bracket as (

--     select
--         'b99bkt' as app_id,
--         count(distinct userselectionid) as entries,
--         count(distinct case when cast(createdat as date) = current_date() - 1 then userselectionid else null end) as yesterday_entries,
--         count(distinct case when createdat >= current_date() - 8 and createdat < current_date() then userselectionid else null end) as last_7_days_entries,
--         count(distinct case when createdat >= current_date() - 29 and createdat < current_date() then userselectionid else null end) as last_28_days_entries,
--         count(distinct userid) as entrants,
--         count(distinct case when cast(createdat as date) = current_date() - 1 then userid else null end) as yesterday_entrants,
--         count(distinct case when createdat >= current_date() - 8 and createdat < current_date() then userid else null end) as last_7_days_entrants,
--         count(distinct case when createdat >= current_date() - 29 and createdat < current_date() then userid else null end) as last_28_days_entrants,
--         1 as contests,
--         max(cast(createdat as date)) as last_entry_date
--     from  {{ source('bet99_bracket', 'userselections') }}
--     group by 1

-- ),

elf_blast as (

    select
        'US49' as app_id,
        count(session_id) as entries,
        count(case when session_date_utc = current_date() - 1 then session_id else null end) as yesterday_entries,
        count(case when session_date_utc >= current_date() - 8 and session_date_utc < current_date() then session_id else null end) as last_7_days_entries,
        count(case when session_date_utc >= current_date() - 29 and session_date_utc < current_date() then session_id else null end) as last_28_days_entries,
        count(distinct user_id) as entrants,
        count(distinct case when session_date_utc = current_date() - 1 then user_id else null end) as yesterday_entrants,
        count(distinct case when session_date_utc >= current_date() - 8 and session_date_utc < current_date() then user_id else null end) as last_7_days_entrants,
        count(distinct case when session_date_utc >= current_date() - 29 and session_date_utc < current_date() then user_id else null end) as last_28_days_entrants,
        1 as contests,
        max(session_date_utc) as last_entry_date
    from  {{ ref('INT_ELF_BLAST__SESSIONS') }}
    group by 1

),

elf_ski as (

    select
        'US54' as app_id,
        count(session_id) as entries,
        count(case when session_date_utc = current_date() - 1 then session_id else null end) as yesterday_entries,
        count(case when session_date_utc >= current_date() - 8 and session_date_utc < current_date() then session_id else null end) as last_7_days_entries,
        count(case when session_date_utc >= current_date() - 29 and session_date_utc < current_date() then session_id else null end) as last_28_days_entries,
        count(distinct user_id) as entrants,
        count(distinct case when session_date_utc = current_date() - 1 then user_id else null end) as yesterday_entrants,
        count(distinct case when session_date_utc >= current_date() - 8 and session_date_utc < current_date() then user_id else null end) as last_7_days_entrants,
        count(distinct case when session_date_utc >= current_date() - 29 and session_date_utc < current_date() then user_id else null end) as last_28_days_entrants,
        1 as contests,
        max(session_date_utc) as last_entry_date
    from  {{ ref('INT_ELF_SKI__SESSIONS') }}
    group by 1

),

nc_trivia as (

    select
        'US60' as app_id,
        count(distinct userid || '-' || contestid) as entries,
        count(distinct case when cast(startedat as date) = current_date() - 1 then userid || '-' || contestid else null end) as yesterday_entries,
        count(distinct case when startedat >= current_date() - 8 and startedat < current_date() then userid || '-' || contestid else null end) as last_7_days_entries,
        count(distinct case when startedat >= current_date() - 29 and startedat < current_date() then userid || '-' || contestid else null end) as last_28_days_entries,
        count(distinct userid) as entrants,
        count(distinct case when cast(startedat as date) = current_date() - 1 then userid else null end) as yesterday_entrants,
        count(distinct case when startedat >= current_date() - 8 and startedat < current_date() then userid else null end) as last_7_days_entrants,
        count(distinct case when startedat >= current_date() - 29 and startedat < current_date() then userid else null end) as last_28_days_entrants,
        count(distinct contestid) as contests,
        max(cast(startedat as date)) as last_entry_date
    from {{ source('nc_trivia', 'user_selections') }}
    group by 1

),

nc_matchup as (

    select
        '1206' as app_id,
        count(distinct id) as entries,
        count(distinct case when cast(createdat as date) = current_date() - 1 then id else null end) as yesterday_entries,
        count(distinct case when createdat >= current_date() - 8 and createdat < current_date() then id else null end) as last_7_days_entries,
        count(distinct case when createdat >= current_date() - 29 and createdat < current_date() then id else null end) as last_28_days_entries,
        count(distinct userid) as entrants,
        count(distinct case when cast(createdat as date) = current_date() - 1 then userid else null end) as yesterday_entrants,
        count(distinct case when createdat >= current_date() - 8 and createdat < current_date() then userid else null end) as last_7_days_entrants,
        count(distinct case when createdat >= current_date() - 29 and createdat < current_date() then userid else null end) as last_28_days_entrants,
        count(distinct gameid) as contests,
        max(cast(createdat as date)) as last_entry_date
    from  {{ source('newscorp_matchup', 'user_attempts') }}
    group by 1

),

b365fan_entries as (

    select
        'b365fan' as app_id,
        count(lineupid) as entries,
        count(distinct case when cast(createdat as date) = current_date() - 1 then lineupid else null end) as yesterday_entries,
        count(distinct case when createdat >= current_date() - 8 and createdat < current_date() then lineupid else null end) as last_7_days_entries,
        count(distinct case when createdat >= current_date() - 29 and createdat < current_date() then lineupid else null end) as last_28_days_entries,
        -- count(distinct userid) as entrants,
        -- count(distinct case when cast(createdat as date) = current_date() - 1 then userid else null end) as yesterday_entrants,
        -- count(distinct case when createdat >= current_date() - 8 and createdat < current_date() then userid else null end) as last_7_days_entrants,
        -- count(distinct case when createdat >= current_date() - 29 and createdat < current_date() then userid else null end) as last_28_days_entrants,
        count(distinct stageid) as contests,
        max(cast(createdat as date)) as last_entry_date
    from  {{ source('bet365_uf', 'LINEUPS') }}
    group by 1

),

b365fan_actives as (

    select
        'US63' as app_id,
        count(distinct userid) as entrants,
        count(distinct case when cast(createdat as date) = current_date() - 1 then userid else null end) as yesterday_entrants,
        count(distinct case when createdat >= current_date() - 8 and createdat < current_date() then userid else null end) as last_7_days_entrants,
        count(distinct case when createdat >= current_date() - 29 and createdat < current_date() then userid else null end) as last_28_days_entrants
    from  {{ source('bet365_uf', 'COIN_TRANSACTIONS') }}
    where transactiontype in ('4','5','7','8','9','12','15','16','17','19','21','22','23')
        
),

b365fan as(

    select
        b365fan_entries.app_id,
        b365fan_entries.entries,
        b365fan_entries.yesterday_entries,
        b365fan_entries.last_7_days_entries,
        b365fan_entries.last_28_days_entries,
        b365fan_actives.entrants,
        b365fan_actives.yesterday_entrants,
        b365fan_actives.last_7_days_entrants,
        b365fan_actives.last_28_days_entrants,
        b365fan_entries.contests,
        b365fan_entries.last_entry_date
    from b365fan_entries
    inner join b365fan_actives
        on b365fan_entries.app_id = b365fan_actives.app_id
        
),


unioned as (

    select *
    from bet365_overunder

    union all

    select *
    from bet99_picks

    union all

    select *
    from betway_picks

    union all

    select *
    from elf_collectyourelf

    union all

    select *
    from fanstake_rivals

    -- union all

    -- select *
    -- from oilers_picks

    -- union all

    -- select *
    -- from sac_kings

    -- union all 

    -- select *
    -- from saracen_picks

    -- union all

    -- select *
    -- from saracen_bracket

    union all
    
    select *
    from cfl_fantasy

    union all

    select *
    from opap_s2w

    union all
    
    select *
    from gana_predictor

    union all

    select *
    from gana_survivor

    -- union all
    
    -- select *
    -- from bet99_bracket

    union all
    
    select *
    from elf_blast

    union all
    
    select *
    from elf_ski

    union all
    
    select *
    from nc_trivia

    union all
    
    select *
    from nc_matchup

    union all

    select *
    from b365fan

)


select * from unioned
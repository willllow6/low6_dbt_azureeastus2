with

entries as (

    select * 
    from {{ ref('fct_saracen__entries') }}

),

-- users as (

--     select * 
--     from {{ ref('dim_saracen__users') }}
    
-- ),

contests as (

    select * 
    from {{ ref('dim_saracen__contests') }} 

),

join_tables as (

    select
        entries.entry_sk,
        -- entries.user_id,
        entries.sso_user_id,
        entries.contest_sk,

        -- users.username,
        -- users.registration_date_et,

        contests.contest_name,
        contests.contest_status,
        contests.contest_opens_at,
        contests.contest_opens_at_et,
        contests.contest_starts_at,
        contests.contest_starts_at_et,

        -- contests.region,

        entries.game_type,
        entries.user_entry_number,
        entries.user_game_entry_number,
        entries.entry_date,
        -- entries.entry_day,
        hour(entries.entered_at) as entry_hour,
        entries.entered_at,
        entries.entry_date_et,
        -- entries.entry_day_et,
        hour(entries.entered_at_et) as entry_hour_et,
        entries.entered_at_et

    from entries 
        -- inner join users
        --     on entries.user_id = users.user_id
        inner join contests 
            on entries.contest_sk = contests.contest_sk

)

select * from join_tables



with

pickem_entries as (

    select
        entry_sk,
        sso_user_id,
        -- user_id as pickem_user_id,
        -- null as bracket_user_id,
        contest_sk,
        game_type,
        entry_date,
        entry_date_et,
        entered_at,
        entered_at_et
    from {{ ref('int_saracen_picks__selections_to_entries') }}  as e
    where 1 = 0 -- picks on dev; remove when connected to prod

),

bracket_entries as (

    select
        entry_sk,
        sso_user_id,
        -- null as pickem_user_id,
        -- user_id as bracket_user_id,
        contest_sk,
        game_type,
        -- competition_name as contest_name,
        created_date as entry_date,
        created_date_et as entry_date_et,
        created_at as entered_at,
        created_at_et as entered_at_et
    from {{ ref('int_saracen_bracket__entries_unioned') }} 

)

select *
from pickem_entries

union all

select *
from bracket_entries
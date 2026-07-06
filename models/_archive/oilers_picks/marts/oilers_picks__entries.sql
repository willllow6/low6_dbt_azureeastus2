with

user_selections as (

    select * from {{ ref('stg_oilers_picks__selections') }}

),

users as (

    select * from {{ ref('stg_oilers_picks__users') }}
    
),

entry_scores as (

    select * from {{ ref('stg_oilers_picks__entry_scores') }}

),

pickems as (

    select * from {{ ref('stg_oilers_picks__pickems') }} where pickem_status != 'DRAFT'

),

tiebreak_outcomes as (

    select pickem_id, tiebreak_outcome from {{ ref('stg_oilers_picks__questions') }}  where is_tiebreak_question = 'True' 

),

aggregate_selections_to_entries as (

    select
        user_id || '-' || pickem_id as entry_id,
        user_id,
        pickem_id,
        max(tiebreak_prediction) as tiebreak_prediction,
        min(created_date) as entry_date,
        min(created_date_et) as entry_date_et,
        day(min(created_date_et)) as entry_day_et,
        min(created_at) as entered_at,
        min(created_at_et) as entered_at_et,
        rank() over(
            partition by user_id
            order by min(created_at) 
        ) as user_entry_number
    from user_selections 
    group by 1,2,3

),

join_tables as (

    select
        entries.entry_id,
        entries.user_id,
        entries.pickem_id,
        users.service_user_id,
        entry_scores.entry_score_id,

        users.email,
        users.low6_username,
        users.first_name,
        users.last_name,
        users.postal_code,
        users.mobile_number,
        users.has_consented_marketing,
        users.has_placed_sports_bet,
        users.created_date as user_created_date,
        users.created_date_et as user_created_date_et,

        pickems.pickem_title,
        pickems.pickem_status,
        pickems.pickem_prize,
        pickems.pickem_start_date,
        pickems.pickem_start_date_et,
        pickems.pickem_starts_at,

        entry_scores.entry_points,
        entries.tiebreak_prediction,
        tiebreak.tiebreak_outcome,
        round(abs(entries.tiebreak_prediction - tiebreak.tiebreak_outcome)) as tiebreak_error,
        entries.user_entry_number,

        case 
            when user_entry_number = 1
                then 'First Entry'
            else 'Subsequent Entry'
        end as user_entry_type,

        entries.entry_date,
        entries.entry_date_et,
        entries.entry_day_et,
        hour(entries.entered_at_et) as entry_hour_et,
        entries.entered_at,
        entries.entered_at_et

    from aggregate_selections_to_entries as entries 
        inner join users
            on entries.user_id = users.user_id
        inner join pickems 
            on entries.pickem_id = pickems.pickem_id
        left join entry_scores as entry_scores 
            on entries.user_id = entry_scores.user_id
            and entries.pickem_id = entry_scores.pickem_id
        left join tiebreak_outcomes as tiebreak 
            on entries.pickem_id = tiebreak.pickem_id

)

select * from join_tables



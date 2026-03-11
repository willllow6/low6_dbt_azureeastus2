with

users as (
  
    select * 
    from {{ ref('int_saracen__user_identity_spine') }} 

),

-- leagues as (

--     select *
--     from {{ ref('stg_saracen_picks__leagues') }}

-- ),

rfm as (

    select *
    from {{ ref('int_saracen__users_rfm') }}

),

joined as (

    select
        users.sso_user_id,
        users.pickem_user_id,
        users.bracket_user_id,
        users.pickem_username,
        users.bracket_username,
        users.pickem_created_date_et,
        users.bracket_created_date_et,
        users.pickem_created_at_et,
        users.bracket_created_at_et,
        rfm.last_entered_date,
        rfm.user_entries_count,
        rfm.days_since_last_played,
        rfm.frequency,
        rfm.recency,
        rfm.segment
    from users
    left join rfm 
        on users.sso_user_id = rfm.sso_user_id

)

select * from joined
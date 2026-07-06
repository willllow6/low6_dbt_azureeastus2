with

awards as (

    select *
    from {{ ref('fct_penn__awards') }}

),

users as (

    select
        user_id,
        penn_user_id,
        tier
    from {{ ref('dim_penn__users') }}

),

joined as (

    select
        a.award_id,
        a.entry_id,
        a.user_id,
        u.penn_user_id,
        u.tier as user_tier,
        a.contest_id,
        a.tenant_id,
        a.client_id,
        a.goal_id,
        a.player_external_id,
        a.event_external_id,
        a.outcome_id,
        a.promotion_id,
        a.prize_amount,
        a.currency,
        a.award_status,
        a.platform,
        a.delivered_at,
        a.delivered_date_et,
        a.created_at,
        a.created_date_et,
        a.updated_at
    from awards a
    inner join users u
        on a.user_id = u.user_id

)

select * from joined

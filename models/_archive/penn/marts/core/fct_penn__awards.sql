with

awards as (

    select *
    from {{ ref('stg_penn__awards') }}

),

entries as (

    select
        entry_id,
        user_id,
        contest_id,
        tenant_id
    from {{ ref('int_penn__entries') }}

),

joined as (

    select
        a.award_id,
        a.entry_id,
        e.user_id,
        e.contest_id,
        e.tenant_id,
        'penn' as client_id,
        a.penn_user_id,
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
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', a.delivered_at) as date) as delivered_date_et,
        a.created_at,
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', a.created_at) as date) as created_date_et,
        a.updated_at
    from awards a
    inner join entries e
        on a.entry_id = e.entry_id

)

select * from joined

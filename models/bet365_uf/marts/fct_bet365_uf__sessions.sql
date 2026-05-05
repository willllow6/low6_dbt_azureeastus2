with

events as (

    select user_id, event_type, coin_amount, created_at
    from {{ ref('fct_bet365_uf__events') }}

),

events_with_prev as (

    select
        user_id,
        event_type,
        coin_amount,
        created_at,
        lag(created_at) over (partition by user_id order by created_at) as prev_created_at
    from events

),

session_events as (

    select
        user_id,
        event_type,
        coin_amount,
        created_at,
        sum(
            case
                when prev_created_at is null then 1
                when timediff(second, prev_created_at, created_at) > 1800 then 1
                else 0
            end
        ) over (
            partition by user_id order by created_at
            rows between unbounded preceding and current row
        ) as session_number
    from events_with_prev

),

user_sessions as (

    select
        user_id || '-' || session_number as session_id,
        user_id,
        session_number as user_session_number,
        min(created_at) as starts_at,
        max(created_at) as ends_at,
        case
            when datediff(seconds, min(created_at), max(created_at)) = 0 then null
            else datediff(seconds, min(created_at), max(created_at))
        end as elapsed_time_seconds,
        count(*) as event_count,
        coalesce(sum(case when coin_amount > 0 then coin_amount end), 0) as coins_earned,
        coalesce(sum(case when coin_amount < 0 then coin_amount * -1 end), 0) as coins_spent,
        sum(case when event_type in ('Coin Purchase', 'All Access Pass Purchase') then 1 else 0 end) as iap_count,
        sum(case when event_type in ('Coin Purchase', 'All Access Pass Purchase') then 1 else 0 end) > 0 as has_iap,
        coalesce(sum(case when event_type = 'Coin Purchase' then coin_amount end), 0) as coins_purchased,
        sum(case when event_type = 'Card Purchase' then 1 else 0 end) as card_purchase_count,
        sum(case when event_type = 'Card Purchase' then 1 else 0 end) > 0 as has_card_purchase,
        sum(case when event_type = 'Pack Purchase' then 1 else 0 end) as pack_purchase_count,
        sum(case when event_type = 'Pack Purchase' then 1 else 0 end) > 0 as has_pack_purchase,
        sum(case when event_type = 'Card Training' then 1 else 0 end) as card_training_count,
        sum(case when event_type = 'Card Training' then 1 else 0 end) > 0 as has_card_training,
        sum(case when event_type = 'Contest Entered' then 1 else 0 end) as entry_count,
        sum(case when event_type = 'Contest Entered' then 1 else 0 end) > 0 as has_entered_contest
    from session_events
    group by user_id || '-' || session_number, user_id, session_number

)

select * from user_sessions

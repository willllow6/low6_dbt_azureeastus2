with

events as (

    select user_id, created_at
    from {{ ref('fct_bet365_uf__events') }}

),

user_days as (

    select distinct
        user_id,
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', created_at) as date) as active_date
    from events

),

user_active_day_number as (

    select
        user_id,
        active_date,
        row_number() over (partition by user_id order by active_date) as user_active_day_number
    from user_days

)

select * from user_active_day_number

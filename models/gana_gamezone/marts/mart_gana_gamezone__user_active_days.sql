with

predictor_activity as (

    select
        user_id,
        cast(convert_timezone('UTC', '{{ var("gana_gamezone_local_timezone") }}', entered_at) as date)     as date_day
    from {{ ref('fct_gana_gamezone__predictor_entries') }}

),

survivor_activity as (

    select
        user_id,
        cast(convert_timezone('UTC', '{{ var("gana_gamezone_local_timezone") }}', selected_at) as date)    as date_day
    from {{ ref('fct_gana_gamezone__survivor_entries') }}
    where selected_country_id is not null

),

all_activity as (

    select user_id, date_day from predictor_activity
    union
    select user_id, date_day from survivor_activity

),

users as (

    select * from {{ ref('dim_gana_gamezone__users') }}

),

joined as (

    select
        a.user_id,
        u.email,
        u.full_name,
        'gana'                                  as client_id,
        'gana'                                  as tenant_id,
        'Gana'                                  as tenant_name,
        a.date_day,
        p.user_id is not null                   as is_predictor_active,
        s.user_id is not null                   as is_survivor_active
    from all_activity as a
    left join users as u
        on a.user_id = u.user_id
    left join predictor_activity as p
        on a.user_id = p.user_id and a.date_day = p.date_day
    left join survivor_activity as s
        on a.user_id = s.user_id and a.date_day = s.date_day

)

select * from joined

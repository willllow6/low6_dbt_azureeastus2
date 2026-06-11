with

entries as (

    select * from {{ ref('fct_gana_gamezone__predictor_entries') }}

),

users as (

    select * from {{ ref('dim_gana_gamezone__users') }}

),

joined as (

    select
        e.entry_id,
        e.user_id,
        u.email,
        u.full_name,
        u.first_name,
        u.last_name,
        e.client_id,
        e.tenant_id,
        e.tenant_name,
        e.game_type,
        e.contest_id,
        e.entry_number,
        e.entered_at,
        cast(convert_timezone('UTC', '{{ var("gana_gamezone_local_timezone") }}', e.entered_at) as date)   as entered_date,
        u.registered_at,
        u.clerk_user_id
    from entries as e
    left join users as u
        on e.user_id = u.user_id

)

select * from joined

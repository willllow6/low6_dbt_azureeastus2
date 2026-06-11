with

entries as (

    select * from {{ ref('fct_gana_gamezone__survivor_entries') }}

),

users as (

    select * from {{ ref('dim_gana_gamezone__users') }}

),

contests as (

    select * from {{ ref('dim_gana_gamezone__survivor_contests') }}

),

countries as (

    select * from {{ ref('dim_gana_gamezone__countries') }}

),

joined as (

    select
        e.entry_id,
        e.user_id,
        u.email,
        u.full_name,
        e.client_id,
        e.tenant_id,
        e.tenant_name,
        e.game_type,
        e.contest_id,
        c.contest_name,
        c.competing_teams,
        c.day,
        c.ends_at,
        e.selected_country_id,
        co.country_name                 as selected_country_name,
        co.country_code                 as selected_country_code,
        e.is_correct,
        e.current_streak,
        e.best_streak,
        e.selected_at,
        cast(convert_timezone('UTC', '{{ var("gana_gamezone_local_timezone") }}', e.selected_at) as date)   as selected_date
    from entries as e
    left join users as u
        on e.user_id = u.user_id
    left join contests as c
        on e.contest_id = c.contest_id
    left join countries as co
        on e.selected_country_id = co.country_id

)

select * from joined

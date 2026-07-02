with

selections as (

    select * from {{ ref('fct_gana_gamezone__bracket_selections') }}

),

users as (

    select * from {{ ref('dim_gana_gamezone__users') }}

),

matches as (

    select * from {{ ref('dim_gana_gamezone__bracket_matches') }}

),

countries as (

    select * from {{ ref('dim_gana_gamezone__countries') }}

),

joined as (

    select
        s.selection_id,
        s.user_id,
        u.email,
        u.full_name,
        s.client_id,
        s.tenant_id,
        s.tenant_name,
        s.game_type,
        s.contest_id,
        s.match_id,
        m.round,
        m.bracket_position,
        m.competing_teams,
        s.selected_country_id,
        co.country_name                 as selected_country_name,
        co.country_code                 as selected_country_code,
        s.is_correct,
        m.correct_country_id,
        m.correct_country_name,
        s.selected_at,
        cast(convert_timezone('UTC', '{{ var("gana_gamezone_local_timezone") }}', s.selected_at) as date)   as selected_date
    from selections as s
    left join users as u
        on s.user_id = u.user_id
    left join matches as m
        on s.match_id = m.match_id
    left join countries as co
        on s.selected_country_id = co.country_id

)

select * from joined

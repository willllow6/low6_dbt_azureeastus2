with

selections as (

    select * from {{ ref('fct_gana_gamezone__predictor_selections') }}

),

group_contests as (

    select * from {{ ref('dim_gana_gamezone__group_contests') }}

),

countries as (

    select * from {{ ref('dim_gana_gamezone__countries') }}

),

joined as (

    select
        s.selection_id,
        s.entry_id,
        s.user_id,
        s.client_id,
        s.tenant_id,
        s.tenant_name,
        s.game_type,
        s.contest_id,
        s.group_id,
        gc.group_name,
        s.predicted_position,
        s.selected_country_id,
        s.is_predicted_to_progress,
        c.country_name                  as selected_country_name,
        c.country_code                  as selected_country_code,
        c.odds                          as selected_country_odds,
        s.selected_at,
        cast(convert_timezone('UTC', '{{ var("gana_gamezone_local_timezone") }}', s.selected_at) as date)   as selected_date
    from selections as s
    left join group_contests as gc
        on s.group_id = gc.group_id
    left join countries as c
        on s.selected_country_id = c.country_id

)

select * from joined

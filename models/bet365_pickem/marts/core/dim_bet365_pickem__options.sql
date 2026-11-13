with

options as (

    select *
    from {{ ref('int_bet365_pickem__options_joined_to_competitors') }}

)

select
    option_id,
    question_id,
    competitor_id,

    client_id,
    game_type,

    option_key,
    option_label,
    competitor_name,
    option_text,
    pricing_strategy,
    manual_price_set_by,

    option_position,
    manual_price_decimal,

    is_active,

    manual_price_set_at,
    removed_at

from options

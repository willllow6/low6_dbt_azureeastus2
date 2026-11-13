with

options as (

    select *
    from {{ ref('stg_bet365_pickem__options') }}

),

competitors as (

    select *
    from {{ ref('stg_bet365_pickem__competitors') }}

),

-- a merged competitor is shown under the competitor it was merged into
resolved_competitors as (

    select
        competitors.competitor_id,
        coalesce(merged_into.competitor_name, competitors.competitor_name) as competitor_name
    from competitors
    left join competitors as merged_into
        on competitors.merged_into_competitor_id = merged_into.competitor_id

)

select
    options.option_id,
    options.question_id,
    options.competitor_id,

    options.client_id,
    options.game_type,

    options.option_key,
    options.option_label,
    resolved_competitors.competitor_name,
    case
        when resolved_competitors.competitor_name is null
            then options.option_label
        when options.option_key = 'win'
            then resolved_competitors.competitor_name
        when options.option_key in ('ko_tko_dq', 'submission', 'decision')
            then resolved_competitors.competitor_name || ' by ' || options.option_label
        else resolved_competitors.competitor_name || ' - ' || options.option_label
    end as option_text,
    options.pricing_strategy,
    options.manual_price_set_by,

    options.option_position,
    options.manual_price_decimal,

    options.is_active,

    options.manual_price_set_at,
    options.removed_at

from options
left join resolved_competitors
    on options.competitor_id = resolved_competitors.competitor_id

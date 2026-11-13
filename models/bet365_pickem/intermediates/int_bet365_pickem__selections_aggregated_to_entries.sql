with

selections as (

    select *
    from {{ ref('stg_bet365_pickem__selections') }}

),

aggregated as (

    select
        entry_id,

        count(*) as selections,
        avg(price_decimal) as avg_price_decimal,
        sum(potential_returns) as potential_returns,

        max(selected_at) as last_selected_at

    from selections
    group by entry_id

)

select
    entry_id,

    selections,
    avg_price_decimal,
    potential_returns,

    last_selected_at

from aggregated

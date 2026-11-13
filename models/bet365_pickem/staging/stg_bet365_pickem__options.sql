with

source as (

    select *
    from {{ source('bet365_pickem', 'question_options') }}

),

renamed as (

    select

        ----------  ids
        id::varchar as option_id,
        question_id::varchar as question_id,
        competitor_id::varchar as competitor_id,
        'bet365' as client_id,

        ---------- strings
        'pickem' as game_type,
        key as option_key,
        -- display label for the key alone; dim_bet365_pickem__options adds the competitor
        case
            when key = 'ko_tko_dq' then 'KO/TKO/DQ'
            when key regexp 'round_[0-9]+' then 'Round ' || regexp_substr(key, '[0-9]+')
            when key regexp 'at_least_[0-9]+' then regexp_substr(key, '[0-9]+') || '+'
            else initcap(replace(key, '_', ' '))
        end as option_label,
        pricing_strategy,
        manual_price_set_by,

        ---------- numerics
        position as option_position,
        manual_price_decimal,

        ---------- booleans
        removed_at is null as is_active,

        ---------- timestamps
        manual_price_set_at::timestamp_ntz as manual_price_set_at,
        removed_at::timestamp_ntz as removed_at

    from source

)

select * from renamed

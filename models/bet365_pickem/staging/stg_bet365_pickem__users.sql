with

source as (

    select *
    from {{ source('bet365_pickem', 'players') }}

),

renamed as (

    -- wallet_private_token_enc and wallet_context are wallet credentials: never select them.
    select

        ----------  ids
        id::varchar as user_id,
        gaming_id::varchar as external_user_id,
        'bet365' as client_id,
        country_code || coalesce('-' || nullif(state_code, ''), '') as tenant_id,

        ---------- strings
        country_code || coalesce('-' || nullif(state_code, ''), '') as tenant_name,
        'pickem' as game_type,
        'sso' as registration_type,
        country_code,
        nullif(state_code, '') as state_code,
        display_name,
        currency,
        language,
        price_format,

        ---------- timestamps
        first_launched_at::timestamp_ntz as registered_at,
        last_launched_at::timestamp_ntz as last_launched_at

    from source

)

select * from renamed

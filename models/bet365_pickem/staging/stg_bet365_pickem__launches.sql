with

source as (

    select *
    from {{ source('bet365_pickem', 'player_launches') }}

),

renamed as (

    -- launched_at and captured_at are written by the ADF "Append player launches"
    -- step as timestamp_ntz in UTC, so no timezone handling is needed here.
    select

        ----------  ids
        {{ dbt_utils.generate_surrogate_key(['player_id', 'launched_at']) }} as launch_id,
        player_id::varchar as user_id,
        'bet365' as client_id,
        country_code || coalesce('-' || nullif(state_code, ''), '') as tenant_id,

        ---------- strings
        country_code || coalesce('-' || nullif(state_code, ''), '') as tenant_name,
        'pickem' as game_type,
        country_code,
        nullif(state_code, '') as state_code,

        ---------- timestamps
        launched_at,
        captured_at

    from source

)

select * from renamed

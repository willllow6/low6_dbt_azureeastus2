with

source as (

    select *
    from {{ source('opap_spintowin', 'reel_options') }}

),

renamed as (

    select

        ----------  ids
        id as reel_option_id,
        reel_id,
        contest_fixture_id,

        ---------- strings
        external_outcome_id,
        external_market_id,
        coalesce(display_name_override, outcome_name) as outcome_name,
        market_name,

        ---------- numerics
        position,

        ---------- booleans
        enabled as is_enabled

        ---------- dates

        ---------- timestamps

    from source

)

select * from renamed

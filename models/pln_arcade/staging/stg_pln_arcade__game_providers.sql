with

source as (

    select *
    from {{ source('pln_arcade','game_providers') }}

),

renamed as (

    select
        gameproviderid as game_provider_id,
        gameprovidername as game_provider_name,
        setting as game_provider_settings
    from source

)

select * from renamed
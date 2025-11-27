with

source as (

    select *
    from {{ source('pln_arcade','game_providers') }}

),

renamed as (

    select
        game_provider_id,
        game_provider_name,
        game_code,
        setting as game_provider_settings

    from source

)

select * from renamed
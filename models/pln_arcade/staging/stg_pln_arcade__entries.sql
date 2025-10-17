with

source as (

    select *
    from {{ source('pln_arcade','entries') }}

),

renamed as (

    select
        entry_id,
        game_id,
        user_id,
        transaction_id,
        played as is_played,
        cast(created_at as date) as created_date,
        cast(convert_timezone('UTC','America/New_York',created_at) as date) as created_date_et,
        created_at,
        convert_timezone('UTC','America/New_York',created_at) as created_at_et
    from source
)

select * from renamed
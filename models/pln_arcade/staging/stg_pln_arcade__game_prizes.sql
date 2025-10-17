with

source as (

    select *
    from {{ source('pln_arcade','game_prizes') }}

),

renamed as (

    select
        game_prize_id,
        game_id,
        entry_id,
        transaction_id,
        prize_tier,
        prize_type,
        amount as prize_amount,
        cast(createdat as date) as created_date,
        cast(convert_timezone('UTC','America/New_York',createdat) as date) as created_date_et,
        createdat as created_at,
        convert_timezone('UTC','America/New_York',createdat) as created_at_et
    from source
    
)

select * from renamed
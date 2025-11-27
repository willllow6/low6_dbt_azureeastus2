with

source as (

    select *
    from {{ source('pln_arcade','coin_transactions') }}

),

renamed as (

    select
        coin_transaction_id,
        user_id,
        amount as coin_amount,
        case when amount < 0 then 'debit' else 'credit' end as transaction_direction,
        transaction_type,
        reference as transaction_reference,
        cast(created_at as date) as created_date,
        cast(convert_timezone('UTC','America/New_York',created_at) as date) as created_date_et,
        created_at,
        convert_timezone('UTC','America/New_York',created_at) as created_at_et
    from source

)

select * from renamed
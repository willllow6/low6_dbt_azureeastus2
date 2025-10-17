with

source as (

    select *
    from {{ source('pln_arcade','coin_transactions') }}

),

renamed as (

    select
        cointransactionid as coin_transaction_id,
        userid as user_id,
        amount as coin_amount,
        case when amount < 0 then 'debit' else 'credit' end as transaction_direction,
        transactiontype as transaction_type,
        reference as transaction_reference,
        cast(createdat as date) as created_date,
        cast(convert_timezone('UTC','America/New_York',createdat) as date) as created_date_et,
        createdat as created_at,
        convert_timezone('UTC','America/New_York',createdat) as created_at_et
    from source

)

select * from renamed
with

source as (

    select *
    from {{ source('pln_arcade','store_purchases') }}

),

renamed as (

    select
        storepurchaseid as store_purchase_id,
        userid as user_id,
        store as store_id,
        case
            when store = 0 
                then 'Apple'
            else 'Google'
        end as app_store,
        price,
        productid as product_id,
        purchaseid as purchase_id,
        cast(createdat as date) as created_date,
        cast(convert_timezone('UTC','America/New_York',createdat) as date) as created_date_et,
        createdat as created_at,
        convert_timezone('UTC','America/New_York',createdat) as created_at_et,
        cast(purchasedate as date) as purchased_date,
        cast(convert_timezone('UTC','America/New_York',purchasedate) as date) as purchased_date_et,
        purchasedate as purchased_at,
        convert_timezone('UTC','America/New_York',purchasedate) as purchased_at_et
    from source
)

select * from renamed
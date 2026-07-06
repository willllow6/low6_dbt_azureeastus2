with

source as (

    select *
    from {{ source('pln_arcade','store_purchases') }}

),

renamed as (

    select
        store_purchase_id,
        user_id,
        store as store_id,
        case
            when store = 0 
                then 'Apple'
            else 'Google'
        end as app_store,
        price,
        product_id,
        purchase_id,
        cast(created_at as date) as created_date,
        cast(convert_timezone('UTC','America/New_York',created_at) as date) as created_date_et,
        created_at,
        convert_timezone('UTC','America/New_York',created_at) as created_at_et,
        cast(purchase_date as date) as purchased_date,
        cast(convert_timezone('UTC','America/New_York',purchase_date) as date) as purchased_date_et,
        purchase_date as purchased_at,
        convert_timezone('UTC','America/New_York',purchase_date) as purchased_at_et
    from source
)

select * from renamed
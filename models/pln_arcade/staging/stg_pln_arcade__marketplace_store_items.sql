with

source as (

    select *
    from {{ source('pln_arcade','marketplace_store_items') }}

),

renamed as (

    select
        marketplacestoreitemid as marketplace_store_item_id,
        store as store_id,
        case
            when store = 0 
                then 'Apple'
            else 'Google'
        end as app_store,
        productid as product_id,
        name as item_name,
        amount as coin_amount,
        price as item_price,
        pricelabel as item_price_label
    from source
    
)

select * from renamed
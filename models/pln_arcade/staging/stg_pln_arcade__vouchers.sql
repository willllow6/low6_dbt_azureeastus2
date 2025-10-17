with

source as (

    select *
    from {{ source('pln_arcade','vouchers') }}

),

renamed as (

    select
        voucher_id,
        user_id,
        game_id,
        amount as voucher_amount,
        status as voucher_status,
        code as voucher_code,
        created_at
    from source
)

select * from renamed
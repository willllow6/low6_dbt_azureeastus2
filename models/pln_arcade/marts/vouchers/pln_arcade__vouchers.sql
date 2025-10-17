with

vouchers as (

    select *
    from {{ ref('stg_pln_arcade__vouchers') }}

),

users as (

    select *
    from {{ ref('stg_pln_arcade__users') }}

),

games as (

    select *
    from {{ ref('stg_pln_arcade__game_providers') }}

),

joined as (

    select
        vouchers.voucher_id,
        vouchers.user_id,
        vouchers.game_id,
        users.username,
        users.email,
        games.game_provider_name as game_name,
        vouchers.voucher_amount,
        vouchers.voucher_status,
        vouchers.voucher_code,
        vouchers.created_at
    from vouchers
    left join users
        on vouchers.user_id = users.user_id
    left join games
        on vouchers.game_id = games.game_provider_id

)

select * from joined
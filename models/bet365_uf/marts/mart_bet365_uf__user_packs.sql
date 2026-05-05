with

user_packs as (

    select * 
    from {{ ref('stg_bet365_uf__packs') }}

),

users as (

    select 
        user_id, 
        username, 
        is_tester 
    from {{ ref('stg_bet365_uf__users') }}
),

joined as (

    select
        user_packs.pack_id,
        user_packs.tenant_id,
        user_packs.user_id,
        user_packs.client_id,
        users.username,
        users.is_tester,
        user_packs.pack_name,
        user_packs.pack_type,
        user_packs.is_new,
        user_packs.pack_assigned_date,
        user_packs.pack_assigned_at,
        user_packs.pack_created_at
    from user_packs
    left join users
        on user_packs.user_id = users.user_id

)

select * from joined

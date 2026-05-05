with

cards as (
    select * from {{ ref('dim_bet365_uf__cards') }}
),

users as (
    select user_id, username, is_tester from {{ ref('stg_bet365_uf__users') }}
),

tenants as (
    select tenant_id, tenant_name from {{ ref('stg_bet365_uf__tenants') }}
),

joined as (

    select
        cards.card_id,
        cards.user_id,
        cards.scoreable_id,
        cards.pack_id,
        cards.tenant_id,
        cards.client_id,
        cards.card_rating,
        cards.is_squad,
        cards.card_acquired_date,
        cards.card_acquired_at,
        cards.scoreable_name,
        cards.scoreable_team_name,
        cards.scoreable_rating,
        cards.scoreable_position,
        cards.is_active,
        cards.is_injured,
        cards.card_acquisition_pack_name,
        users.username,
        users.is_tester,
        tenants.tenant_name
    from cards
    left join users
        on cards.user_id = users.user_id
    left join tenants
        on cards.tenant_id = tenants.tenant_id

)

select * from joined

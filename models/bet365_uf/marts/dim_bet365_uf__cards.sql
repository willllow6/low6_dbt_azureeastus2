with

cards as (
    select * from {{ ref('stg_bet365_uf__cards') }}
),

scoreables as (
    select * from {{ ref('stg_bet365_uf__scoreables') }}
),

packs as (
    select * from {{ ref('stg_bet365_uf__packs') }}
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
        cards.card_created_at,
        scoreables.scoreable_name,
        scoreables.scoreable_team_name,
        scoreables.scoreable_rating,
        scoreables.scoreable_position,
        scoreables.is_active,
        scoreables.is_injured,
        packs.pack_name as card_acquisition_pack_name
    from cards
    left join scoreables
        on cards.scoreable_id = scoreables.scoreable_id
    left join packs
        on cards.pack_id = packs.pack_id

)

select * from joined

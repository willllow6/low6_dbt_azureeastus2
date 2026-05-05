with

user_cards as (
    select *
    from {{ ref('mart_bet365_uf__user_cards') }}
    where is_tester = false
),

user_collections as (

    select
        user_id,
        username,
        sum(case when scoreable_rating = 'All-Star' then 1 else 0 end) as allstar_card_count,
        sum(case when scoreable_rating = 'Gold' then 1 else 0 end) as gold_card_count,
        sum(case when scoreable_rating = 'Silver' then 1 else 0 end) as silver_card_count,
        sum(case when scoreable_rating = 'Base' then 1 else 0 end) as base_card_count,
        count(scoreable_id) as card_count
    from user_cards
    group by 1, 2

),

collection_ranking as (

    select
        *,
        row_number() over (
            order by allstar_card_count desc, gold_card_count desc
        ) as collection_ranking
    from user_collections

)

select * from collection_ranking

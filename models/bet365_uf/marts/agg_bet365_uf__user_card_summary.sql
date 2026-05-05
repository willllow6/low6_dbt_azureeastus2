with

user_cards as (
    select * from {{ ref('mart_bet365_uf__user_cards') }}
),

agg_user_cards as (

    select
        tenant_name,
        scoreable_name,
        scoreable_team_name,
        scoreable_position,
        scoreable_rating,
        is_active,
        is_injured,
        card_acquisition_pack_name,
        count(*) as cards_owned,
        count(distinct user_id) as card_owners,
        case
            when grouping(scoreable_position) = 1 and grouping(scoreable_rating) = 1 then 'GRAND_TOTAL'
            when grouping(scoreable_position) = 0 and grouping(scoreable_rating) = 1 then 'POSITION_TOTAL'
            when grouping(scoreable_position) = 1 and grouping(scoreable_rating) = 0 then 'RATING_TOTAL'
            else 'DETAIL'
        end as grouping_level
    from user_cards
    group by grouping sets (
        (tenant_name, scoreable_name, scoreable_team_name, scoreable_position, scoreable_rating, is_active, is_injured, card_acquisition_pack_name),
        (scoreable_position),
        (scoreable_rating),
        ()
    )

)

select
    coalesce(tenant_name, 'TOTAL') as tenant_name,
    coalesce(scoreable_name, 'TOTAL') as scoreable_name,
    coalesce(scoreable_team_name, 'TOTAL') as scoreable_team_name,
    coalesce(scoreable_position, 'TOTAL') as scoreable_position,
    coalesce(scoreable_rating, 'TOTAL') as scoreable_rating,
    coalesce(to_varchar(is_active), 'TOTAL') as is_active,
    coalesce(to_varchar(is_injured), 'TOTAL') as is_injured,
    coalesce(card_acquisition_pack_name, 'TOTAL') as card_acquisition_pack_name,
    cards_owned,
    card_owners,
    grouping_level
from agg_user_cards

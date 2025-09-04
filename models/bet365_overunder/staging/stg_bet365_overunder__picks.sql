with

source as (

    select * 
    from {{ source('bet365_overunder','picks') }}

),

renamed as (

    select

        id as pick_id,
        entry_id,
        prop_id,
        
        initcap(selected_option) as picked_option,
        parse_json(snapshot) as pick_details_json,
        -- pick_details_json:fid::string                 as fixture_option_id,
        -- pick_details_json:fixtureId::string           as fixture_id,
        -- pick_details_json:propId::string              as prop_id,
        -- pick_details_json:marketId::string            as market_id,
        pick_details_json:playerName::string          as player_name,
        pick_details_json:position::string            as player_position,
        pick_details_json:team::string                as team_abbr,
        pick_details_json:opponent::string            as opponent_abbr,
        pick_details_json:marketLabel::string         as market_name,
        pick_details_json:handicap::float             as market_handicap,
        pick_details_json:odds::string                as odds_fractional,
        pick_details_json:oddsDecimal::float          as odds_decimal,
        pick_details_json:oddsAmerican::string        as odds_american,

        case
            when is_correct = 1 
                then true 
            when is_correct = 0
                then false
            else null
        end as is_correct,

        created_at as picked_at
    
    from source

)

select * from renamed
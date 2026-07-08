with

puzzle_items as (

    select * from {{ ref('stg_nc_matchup__puzzle_items') }}
    
),

categories as (

    select * from {{ ref('stg_nc_matchup__categories') }}
    
),

games as (

    select * from {{ ref('stg_nc_matchup__games') }}
    
),

category_items as (

    select
        c.category_id,
        p.puzzle_item_id,
        c.category_name,
        c.category_difficulty_level,
        p.puzzle_item_name
    from categories as c 
        inner join puzzle_items as p 
            on c.category_id = p.category_id
            
),

game_categories as (

    select 
        game_id, 
        trim(value,'[]')::number as category_id,
        game_title,
        game_status,
        sport_code
    from games, lateral split_to_table(category_ids, ',')
    
),

game_category_items as (

    select
        gc.game_id,
        gc.category_id,
        ci.puzzle_item_id,
        gc.game_title,
        gc.game_status,
        gc.sport_code,
        ci.category_name,
        ci.category_difficulty_level,
        ci.puzzle_item_name
    from game_categories as gc 
        inner join category_items as ci 
            on gc.category_id = ci.category_id
        
)

select * from game_category_items order by game_id, category_id, puzzle_item_id
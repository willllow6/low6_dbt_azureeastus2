with

line_attempts as (

    select * from {{ ref('stg_nc_matchup__line_attempts') }}
        
),

puzzle_items as (

    select * from {{ ref('nc_matchup__puzzle_items') }}
    
),

line_attempt_selections as (

    select 
        line_attempt_id, 
        user_id,
        game_id,
        trim(value) as selection,
    from line_attempts, lateral split_to_table(line_attempt_selections, ',')

),

selection_category as (

    select
        s.line_attempt_id,
        s.user_id,
        s.game_id,
        p.category_id,
        p.puzzle_item_id,
        s.selection,
        p.category_name
    from line_attempt_selections as s 
        left join puzzle_items as p
            on s.selection = p.puzzle_item_name
            and s.game_id = p.game_id
)

select * from selection_category
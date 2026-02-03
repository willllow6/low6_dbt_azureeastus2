with 

source as (

    select *
    from {{ source('fanstake_rivals', 'lineup_players') }}

), 

renamed as (

    select
        id as lineup_player_id, -- primary key
        lineup_id, -- foreign key
        athlete_id, -- foreign key
        position,
        salary_cost,
        is_portal_pick, -- flag
        slot,
        points,
        metadata, -- variant/json
        created_at,
        updated_at -- last modified timestamp
    from source

)

select * from renamed
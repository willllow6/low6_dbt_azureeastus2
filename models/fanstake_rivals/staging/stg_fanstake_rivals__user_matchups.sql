with 

source as (

    select *
    from {{ source('fanstake_rivals', 'user_matchups') }}

), 

renamed as (

    select
        id as matchup_id, -- primary key
        weekly_period_id, -- foreign key
        user1_id,
        user2_id,
        user1_lineup_id,
        user2_lineup_id,
        user1_score,
        user2_score,
        winner_id,
        -- matchup_type,
        status,
        metadata, -- variant/json
        cast(created_at as date) as created_date,
        cast(convert_timezone('UTC', 'America/New_York', created_at) as date) as created_date_et,
        created_at,
        updated_at -- last modified timestamp
    from source

)


select * from renamed
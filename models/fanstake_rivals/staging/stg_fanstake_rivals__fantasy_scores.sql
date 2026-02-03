with 

source as (

    select *
    from {{ source('fanstake_rivals', 'fantasy_scores') }}

), 

renamed as (

    select
        id as fantasy_score_id, -- primary key
        lineup_id, -- foreign key to lineups
        weekly_period_id, -- foreign key to weekly periods
        total_points,
        athlete_scores, -- variant/json
        calculated_at,
        created_at
    from source

)

select * from renamed
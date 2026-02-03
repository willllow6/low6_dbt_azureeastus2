with 

source as (

    select *
    from {{ source('fanstake_rivals', 'teams') }}

), 

renamed as (

    select
        id as team_id, -- primary key
        conference_id, -- foreign key
        fanstake_id,
        name as team_name,
        short_name,
        sport as team_sport,
        league as team_league,
        logo,
        metadata, -- variant/json
        created_at,
        updated_at -- last modified timestamp
    from source

)

select * from renamed
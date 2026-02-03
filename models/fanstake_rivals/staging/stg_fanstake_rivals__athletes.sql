with 

source as (

    select *
    from {{ source('fanstake_rivals', 'athletes') }}

), 

renamed as (

    select
        id as athlete_id, -- primary key
        team_id, -- foreign key to teams
        first_name,
        last_name,
        position,
        sport,
        headshot,
        fanstake_id,
        is_active, -- active flag
        salary_cost,
        metadata, -- json/variant info
        created_at,
        updated_at -- last modified timestamp
    from source

)


select * from renamed
with 

source as (

    select *
    from {{ source('fanstake_rivals', 'user_weekly_lineups') }}

), 

renamed as (

    select
        id as lineup_id, -- primary key
        user_id, -- foreign key
        weekly_period_id, -- foreign key
        name as lineup_name,
        total_salary,
        remaining_salary,
        total_points,
        status,
        rank,
        parse_json(metadata):pickCount::number as lineup_selections,
        coalesce(try_parse_json(metadata):isAILineup::boolean,false) as is_ai_generated,
        parse_json(metadata):generatedFor::string as generated_for_user_id,
        hour(created_at) as created_hour,
        hour(convert_timezone('UTC', 'America/New_York', created_at)) as created_hour_et,
        cast(created_at as date) as created_date,
        cast(convert_timezone('UTC', 'America/New_York', created_at) as date) as created_date_et,
        created_at,
        updated_at -- last modified timestamp
    from source
)


select * from renamed
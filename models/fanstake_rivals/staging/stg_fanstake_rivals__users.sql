with 

source as (

    select *
    from {{ source('fanstake_rivals', 'users') }}

), 

renamed as (

    select
        id as user_id, -- primary key
        team_id, -- foreign key
        fanstake_id,
        username,
        display_name,
        avatar,
        is_active,
        trim(regexp_replace(roles, '[\{\}"]', '')) AS user_role,
        refresh_token,
        last_login,
        metadata, -- variant/json
        cast(created_at as date) as registration_date,
        cast(convert_timezone('UTC', 'America/New_York', created_at) as date) as registration_date_et,
        team_updated_at,
        created_at as registered_at,
        updated_at -- last modified timestamp
    from source

)


select * from renamed
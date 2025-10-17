with

source as (

    select *
    from {{ source('betway_picks', 'leagues') }}

),

renamed as (

    select

        ----------  ids
        id as league_id,
        -- owner_user_id as league_owner_user_id,
        -- league_team_id as league_team_id,

        ---------- strings
        title as league_title,
        name_en as league_name,
        description_en as league_description,
        code as league_code,
        league_type,

        ---------- numerics

        ---------- booleans

        ---------- dates
        cast(created_at as date) as league_created_date,
        cast(convert_timezone('UTC','America/New_York',created_at) as date) as league_created_date_et, 

        ---------- timestamps
        created_at as league_created_at,
        convert_timezone('UTC','America/New_York', created_at) as league_created_at_et


    from source


)

select * from renamed

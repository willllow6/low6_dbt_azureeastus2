with

source as (

    select *
    from {{ source('saracen_picks', 'league_users') }}

),

renamed as (

    select

        ----------  ids
        id as friends_league_member_id,
        user_id,
        league_id,

        ---------- strings

        ---------- numerics
        score,

        ---------- booleans

        ---------- dates
        cast(created_at as date) as joined_date,
        cast(convert_timezone('UTC','America/New_York',created_at) as date) as joined_date_et, 

        ---------- timestamps
        created_at as joined_at,
        convert_timezone('UTC','America/New_York', created_at) as joined_at_et


    from source


)

select * from renamed
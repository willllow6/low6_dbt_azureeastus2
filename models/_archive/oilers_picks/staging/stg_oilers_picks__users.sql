with

source as (

    select *
    from {{ source('oilers_picks', 'users') }}

),

renamed as (

    select

        ----------  ids
        userid as user_id,
        serviceuserid as service_user_id,
        favouriteteamid as favorite_team_id,

        ---------- strings
        email,
        username as low6_username,
        firstname as first_name,
        lastname as last_name,
        postalcode as postal_code,

        ---------- numerics
        mobilenumber as mobile_number,

        ---------- booleans
        optincommunication as has_consented_marketing,
        placedsportsbetbefore as has_placed_sports_bet,

        ---------- dates
        cast(createdat as date) as created_date,
        cast(
            convert_timezone('UTC', 'America/New_York', createdat) as date
        ) as created_date_et,

        ---------- timestamps
        createdat as created_at,
        convert_timezone('UTC', 'America/New_York', createdat) as created_at_et

    from source

)

select * from renamed

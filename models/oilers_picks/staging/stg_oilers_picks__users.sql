with

source as (

    select *
    from {{ source('oilers_picks', 'users') }}

),

renamed as (

    select

        ----------  ids
        userid::varchar as user_id,
        'oilers' as client_id,
        'oilers' as tenant_id,
        serviceuserid as service_user_id,
        ssouserid as external_user_id,
        favouriteteamid as favorite_team_id,

        ---------- strings
        'Edmonton Oilers' as tenant_name,
        'pickem' as game_type,
        case
            when ssouserid is not null
                then 'sso'
            else 'form'
        end as registration_type,
        username,
        email,
        firstname as first_name,
        lastname as last_name,
        postalcode as postal_code,
        mobilenumber as mobile_number,

        ---------- booleans
        optincommunication as marketing_consent,
        placedsportsbetbefore as has_placed_sports_bet,

        ---------- timestamps
        createdat as registered_at

    from source

)

select * from renamed

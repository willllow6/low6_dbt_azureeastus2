with

source as (

    select *
    from {{ source('oilers_picks', 'leagues') }}

),

renamed as (

    select

        ----------  ids
        leagueid::varchar as friend_league_id,
        owneruserid::varchar as owner_user_id,
        leagueteamid as league_team_id,
        'oilers' as client_id,
        'oilers' as tenant_id,

        ---------- strings
        'Edmonton Oilers' as tenant_name,
        'pickem' as game_type,
        title as friend_league_name,
        code as friend_league_code,
        leaguetype as friend_league_type,

        ---------- timestamps
        createdat as created_at

    from source

)

select * from renamed

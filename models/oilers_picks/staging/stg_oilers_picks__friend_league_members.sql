with

source as (

    select *
    from {{ source('oilers_picks', 'leagueusers') }}

),

renamed as (

    select

        ----------  ids
        leagueuserid::varchar as friend_league_member_id,
        leagueid::varchar as friend_league_id,
        userid::varchar as user_id,
        'oilers' as client_id,
        'oilers' as tenant_id,

        ---------- strings
        'Edmonton Oilers' as tenant_name,
        'pickem' as game_type,

        ---------- timestamps
        createdat as joined_at

    from source

)

select * from renamed

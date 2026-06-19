with

source as (

    select *
    from {{ source('saracen_bracket_nbafinals2026', 'leagueusers') }}

),

renamed as (

    select

        ----------  ids
        leagueuserid as league_member_id,
        leagueid as league_id,
        userid as user_id,

        ---------- strings
        'nba_finals_2026' as tournament_name,

        ---------- numerics

        ---------- booleans

        ---------- dates
        cast(createdat as date) as league_joined_date,

        ---------- timestamps
        createdat as league_joined_at

    from source

)

select * from renamed

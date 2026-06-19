with

source as (

    select *
    from {{ source('saracen_bracket_nbafinals2026', 'leagues') }}

),

renamed as (

    select

        ----------  ids
        leagueid as league_id,
        owneruserid as league_owner_user_id,

        ---------- strings
        'nba_finals_2026' as tournament_name,
        title as league_name,
        leaguetype as league_type,
        code as league_code,

        ---------- numerics

        ---------- booleans

        ---------- dates
        cast(createdat as date) as league_created_date,

        ---------- timestamps
        createdat as league_created_at

    from source

)

select * from renamed

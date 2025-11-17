with

source as (

    select *
    from {{ source('oilers_picks', 'leagueusers') }}

),

renamed as (

    select

        ----------  ids
        leagueuserid as membership_id,
        leagueid as league_id,
        userid as user_id,

        ---------- strings

        ---------- numerics

        ---------- booleans

        ---------- dates
        cast(createdat as date) as league_joined_date,
        cast(
            convert_timezone('UTC', 'America/New_York', createdat) as date
        ) as league_joined_date_et,

        ---------- timestamps
        createdat as league_joined_at,
        convert_timezone(
            'UTC', 'America/New_York', createdat
        ) as league_joined_at_et

    from source

)

select * from renamed

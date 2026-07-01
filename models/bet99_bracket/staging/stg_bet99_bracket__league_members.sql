with

source as (

    select *
    from {{ source('bet99_bracket', 'leagueusers') }}

),

renamed as (

    select

        ----------  ids
        leagueuserid as league_member_id,
        leagueid as league_id,
        userid as user_id,

        ---------- strings
        '{{ var("bet99_bracket_current_tournament") }}' as tournament_name,

        ---------- numerics

        ---------- booleans

        ---------- dates
        cast(createdat as date) as league_joined_date,
        cast(convert_timezone('UTC', 'America/New_York', createdat::timestamp_ntz) as date) as league_joined_date_et,

        ---------- timestamps
        createdat as league_joined_at

    from source


)

select * from renamed

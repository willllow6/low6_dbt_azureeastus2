with

source as (

    select *
    from {{ source('oilers_picks', 'leagues') }}

),

renamed as (

    select

        ----------  ids
        leagueid as league_id,
        owneruserid as league_owner_user_id,
        leagueteamid as league_team_id,

        ---------- strings
        title as league_name,
        code as league_code,
        leaguetype as league_type,

        ---------- numerics

        ---------- booleans

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

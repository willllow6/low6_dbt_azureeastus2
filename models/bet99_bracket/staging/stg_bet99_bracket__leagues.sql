with

source as (

    select *
    from {{ source('bet99_bracket', 'leagues') }}

),

renamed as (

    select

        ----------  ids
        leagueid as league_id,
        owneruserid as league_owner_user_id,

        ---------- strings
        '{{ var("bet99_bracket_current_tournament") }}' as tournament_name,
        title as league_name,
        leaguetype as league_type,
        code as league_code,

        ---------- numerics

        ---------- booleans

        ---------- dates
        cast(createdat as date) as league_created_date,
        cast(convert_timezone('UTC', 'America/New_York', createdat::timestamp_ntz) as date) as league_created_date_et,

        ---------- timestamps
        createdat as league_created_at

    from source


)

select * from renamed

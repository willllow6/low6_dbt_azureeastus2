with

source as (

    select *
    from {{ source('nc_trivia', 'leagues') }}

),

renamed as (

    select

        ----------  ids
        leagueid as league_id,

        ---------- strings
        name as league_name,
        description as league_description,
        membershipcode as league_code,

        ---------- numerics

        ---------- booleans
        isdailyleaderboardenabled as is_daily_leaderboard_enabled,
        isweeklyleaderboardenabled as is_weekly_leaderboard_enabled,
        ismonthlyleaderboardenabled as is_monthly_leaderboard_enabled,
        isoverallleaderboardenabled as is_overall_leaderboard_enabled,

        ---------- dates
        cast(createdat as date) as league_created_date,

        ---------- timestamps
        createdat as created_at,
        updatedat as updated_at

    from source

)

select * from renamed
with

source as (

    select *
    from {{ source('saracen_bracket_nbafinals2026', 'userselections') }}

),

users as (

    select *
    from {{ source('saracen_bracket_nbafinals2026', 'users') }}

),

renamed as (

    select

        ----------  ids
        source.userselectionid as entry_id,
        'BKT_' || source.userselectionid as entry_sk,
        source.userid as user_id,
        users.ssouserid as sso_user_id,
        'BKT_' || source.competition as contest_sk,

        ---------- strings
        'bracket' as game_type,
        'nba_finals_2026' as tournament_name,
        source.name as user_bracket_name,
        source.competition as competition_name,

        ---------- numerics
        source.points,
        source.frpoints as first_round_points,
        source.srpoints as second_round_points,
        source.s16points as sweet_16_points,
        source.eepoints as elite_eight_points,
        source.ffpoints as final_four_points,
        source.chpoints as championship_points,
        source.answer as tie_breaker_selection,

        ---------- booleans

        ---------- dates
        cast(source.createdat as date) as created_date,
        cast(convert_timezone('UTC','America/New_York',source.createdat::timestamp_ntz) as date) as created_date_et,

        ---------- timestamps
        source.createdat::timestamp_ntz as created_at,
        convert_timezone('UTC','America/New_York',source.createdat::timestamp_ntz) as created_at_et

    from source
    left join users
        on source.userid = users.userid

)

select * from renamed

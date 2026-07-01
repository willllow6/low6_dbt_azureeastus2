with

source as (

    select *
    from {{ source('bet99_bracket', 'brackets') }}

),

renamed as (

    select

        ----------  ids
        bracketid as contest_id,
        'BKT_' || competition as contest_sk,

        ---------- strings
        '{{ var("bet99_bracket_current_tournament") }}' as tournament_name,
        competition as contest_name,
        case
            when sysdate() < openselectionsutc or openselectionsutc is null
                then 'DRAFT'
            when sysdate() >= openselectionsutc and sysdate() < closeselectionsutc
                then 'ACTIVE'
            when sysdate() >= closeselectionsutc
                then 'COMPLETE'
        end as contest_status,

        ---------- numerics
        tiebreakeranswer as tie_breaker_answer,

        ---------- booleans
        allowmultiplebracket as allow_multiple_brackets,

        ---------- dates

        ---------- timestamps
        createdat as created_at,
        openselectionsutc::timestamp_ntz as contest_opens_at,
        convert_timezone('UTC','America/New_York',openselectionsutc::timestamp_ntz) as contest_opens_at_et,
        closeselectionsutc::timestamp_ntz as contest_starts_at,
        convert_timezone('UTC','America/New_York',closeselectionsutc::timestamp_ntz) as contest_starts_at_et

    from source


)

select * from renamed

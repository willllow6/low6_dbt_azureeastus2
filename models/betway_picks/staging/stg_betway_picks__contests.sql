with

source as (

    select *
    from {{ source('betway_picks', 'pickems') }}

),

renamed as (

    select

        ----------  ids
        id as contest_id,
        league_id,

        ---------- strings
        title_en as contest_title,
        pickem_status as contest_status,
        prize_text_en as contest_prize,

        ---------- numerics

        ---------- booleans
        scored as is_scored,

        ---------- dates
        cast(start_utc as date) as contest_start_date,
        cast(convert_timezone('UTC','America/New_York', start_utc) as date) as contest_start_date_et,

        ---------- timestamps
        open_pickem_utc as contest_opens_at,
        start_utc as contest_starts_at,
        convert_timezone('UTC','America/New_York', open_pickem_utc) as contest_opens_at_et,
        convert_timezone('UTC','America/New_York', start_utc) as contest_starts_at_et,
        created_at,
        updated_at
        

    from source

)

select * from renamed

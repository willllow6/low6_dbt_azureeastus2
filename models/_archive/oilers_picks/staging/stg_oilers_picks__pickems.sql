with

source as (

    select *
    from {{ source('oilers_picks', 'pickems') }}

),

renamed as (

    select

        ----------  ids
        pickemid as pickem_id,

        ---------- strings
        titleen as pickem_title,
        pickemstatus as pickem_status,
        prizetexten as pickem_prize,

        ---------- numerics

        ---------- booleans

        ---------- dates
        cast(createdat as date) as created_date,
        cast(startutc as date) as pickem_start_date,
        cast(
            convert_timezone('UTC', 'America/New_York', createdat) as date
        ) as created_date_et,
        cast(
            convert_timezone('UTC', 'America/New_York', startutc) as date
        ) as pickem_start_date_et,

        ---------- timestamps
        createdat as created_at,
        startutc as pickem_starts_at,
        convert_timezone('UTC', 'America/New_York', createdat) as created_at_et,
        convert_timezone(
            'UTC', 'America/New_York', startutc
        ) as pickem_starts_at_et

    from source

)

select * from renamed

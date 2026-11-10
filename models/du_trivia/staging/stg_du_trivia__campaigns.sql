with

source as (

    select *
    from {{ source('du_trivia', 'campaigns') }}

),

renamed as (

    select

        ----------  ids
        id::varchar as campaign_id,
        'du' as client_id,
        'du' as tenant_id,

        ---------- strings
        'DU' as tenant_name,
        'trivia' as game_type,
        name as campaign_name,
        status as campaign_status,
        dailyunlocktime as daily_unlock_time,
        timezone as time_zone,

        ---------- numerics
        version as campaign_version,
        questionsperday as questions_per_day,

        ---------- semi-structured
        content as campaign_content,

        ---------- dates
        startdate as campaign_start_date,
        enddate as campaign_end_date,

        ---------- timestamps
        publishedat::timestamp_ntz as published_at,
        archivedat::timestamp_ntz as archived_at,
        createdat::timestamp_ntz as created_at,
        updatedat::timestamp_ntz as updated_at

    from source

)

select * from renamed

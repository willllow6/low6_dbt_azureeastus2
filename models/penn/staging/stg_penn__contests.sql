with

source as (

    select *
    from {{ source('penn', 'pickems') }}

),

renamed as (

    select

        ---------- ids
        id as contest_id,
        friendly_id,

        ---------- strings
        title_en as contest_title,
        pickem_status as contest_status,
        prize_text_en as contest_prize,
        sport,
        'penn' as client_id,
        'penn' as tenant_id,

        ---------- booleans
        scored as is_scored,
        pickem_status not in ('completed', 'cancelled') as is_active,

        ---------- game type
        'daily_reveal' as game_type,

        ---------- dates
        cast(coalesce(start_utc, open_pickem_utc) as date) as contest_start_date,

        ---------- timestamps
        open_pickem_utc as contest_opens_at,
        coalesce(start_utc, open_pickem_utc) as contest_starts_at,
        created_at,
        updated_at

    from source

)

select * from renamed

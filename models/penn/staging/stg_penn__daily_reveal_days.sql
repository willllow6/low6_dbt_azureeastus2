with

source as (

    select *
    from {{ source('penn', 'daily_reveal_days') }}

),

renamed as (

    select

        ---------- ids
        id as daily_reveal_day_id,
        pickem_id as contest_id,

        ---------- strings
        pack_id,
        status,
        'penn' as client_id,
        'penn' as tenant_id,

        ---------- numerics
        day_number,

        ---------- booleans
        status not in ('completed', 'cancelled') as is_active,

        ---------- game type
        'daily_reveal' as game_type,

        ---------- dates
        game_date,

        ---------- timestamps
        cast(reveal_open_at as timestamp_ntz) as reveal_open_at,
        cast(pick_cutoff_at as timestamp_ntz) as pick_cutoff_at,
        created_at,
        updated_at

    from source

)

select * from renamed

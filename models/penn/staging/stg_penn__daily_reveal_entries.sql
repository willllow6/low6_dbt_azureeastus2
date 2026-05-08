with

source as (

    select *
    from {{ source('penn', 'daily_reveal_entries') }}

),

renamed as (

    select

        ---------- ids
        id as daily_reveal_entry_id,
        day_id as daily_reveal_day_id,
        user_id,

        ---------- strings
        pack_id,
        status,
        'penn' as client_id,
        'penn' as tenant_id,

        ---------- numerics
        total_hits,
        case when total_hits >= 1 then 1 else 0 end as points,

        ---------- booleans
        status not in ('completed', 'cancelled') as is_active,

        ---------- game type
        'daily_reveal' as game_type,

        ---------- dates
        cast(created_at as date) as created_date,

        ---------- timestamps
        created_at,
        updated_at

    from source

)

select * from renamed

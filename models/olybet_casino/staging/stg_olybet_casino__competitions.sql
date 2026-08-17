with

source as (

    select *
    from {{ source('olybet_casino', 'competition') }}

),

renamed as (

    select

        ----------  ids
        id::varchar as contest_id,
        game_id::varchar as game_id,

        ---------- strings
        name as contest_name,
        description as contest_description,
        status as contest_status,
        image_url as contest_image_url,

        ---------- client / canonical fields
        'olybet'        as client_id,
        'olybet'        as tenant_id,
        'Olybet'        as tenant_name,
        'instant_win'   as game_type,

        ---------- numerics
        max_tickets,
        ticket_min,
        ticket_default,
        ticket_max,
        user_max_entries,
        tickets_issued,

        ---------- booleans
        -- source has no documented status enum, so is_active is derived from
        -- timestamps rather than a known 'live'/'closed' value
        deleted_at is null
            and (expires_at is null or expires_at > sysdate())  as is_active,

        ---------- dates
        cast(live_at as date) as contest_start_date,

        ---------- timestamps
        live_at::timestamp_ntz    as contest_starts_at,
        expires_at::timestamp_ntz as contest_ends_at,
        created_at::timestamp_ntz as created_at,
        deleted_at::timestamp_ntz as deleted_at,
        greatest(
            coalesce(created_at,  '1900-01-01'::timestamp_ntz),
            coalesce(live_at,     '1900-01-01'::timestamp_ntz),
            coalesce(expires_at,  '1900-01-01'::timestamp_ntz),
            coalesce(deleted_at,  '1900-01-01'::timestamp_ntz)
        )::timestamp_ntz as updated_at

    from source

)

select * from renamed

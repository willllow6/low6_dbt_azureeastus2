with

source as (

    select *
    from {{ source('bet365_pickem', 'contests') }}

),

renamed as (

    select

        ----------  ids
        id::varchar as contest_id,
        event_id::varchar as event_id,
        values_version_id::varchar as values_version_id,
        definitions_version_id::varchar as definitions_version_id,
        'bet365' as client_id,

        ---------- strings
        'pickem' as game_type,
        name as contest_name,
        -- the source has no status column: status is the latest lifecycle step reached
        case
            when cancelled_at is not null then 'cancelled'
            when closed_at is not null then 'closed'
            when paid_at is not null then 'paid'
            when paying_at is not null then 'paying'
            when hold_started_at is not null then 'on_hold'
            when settled_at is not null then 'settled'
            when opened_at is not null then 'open'
            when published_at is not null then 'published'
            else 'draft'
        end as contest_status,
        ruleset,
        event_class,
        created_by,

        ---------- numerics
        revision,

        ---------- booleans
        -- published or open: not yet settled, closed or cancelled
        published_at is not null
            and settled_at is null
            and hold_started_at is null
            and paying_at is null
            and paid_at is null
            and closed_at is null
            and cancelled_at is null as is_active,

        ---------- timestamps
        created_at::timestamp_ntz as created_at,
        published_at::timestamp_ntz as published_at,
        opens_at::timestamp_ntz as opens_at,
        opened_at::timestamp_ntz as opened_at,
        settled_at::timestamp_ntz as settled_at,
        hold_started_at::timestamp_ntz as hold_started_at,
        hold_ends_at::timestamp_ntz as hold_ends_at,
        paying_at::timestamp_ntz as paying_at,
        paid_at::timestamp_ntz as paid_at,
        closed_at::timestamp_ntz as closed_at,
        cancelled_at::timestamp_ntz as cancelled_at

    from source

)

select * from renamed

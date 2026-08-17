with

source as (

    select *
    from {{ source('olybet_casino', 'competition_entry') }}

),

renamed as (

    select

        ----------  ids
        id::varchar as entry_id,
        competition_id::varchar as contest_id,
        user_id,

        ---------- booleans
        deleted_at is null as is_active,

        ---------- dates
        cast(created_at as date) as entry_date,

        ---------- timestamps
        created_at::timestamp_ntz as entered_at,
        deleted_at::timestamp_ntz as deleted_at,
        greatest(
            coalesce(created_at, '1900-01-01'::timestamp_ntz),
            coalesce(deleted_at, '1900-01-01'::timestamp_ntz)
        )::timestamp_ntz as updated_at

    from source

)

select * from renamed

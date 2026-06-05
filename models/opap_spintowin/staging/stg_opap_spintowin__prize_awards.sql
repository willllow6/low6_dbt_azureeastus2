with

source as (

    select *
    from {{ source('opap_spintowin', 'prize_awards') }}

),

renamed as (

    select

        ----------  ids
        id as prize_award_id,
        entry_id,
        user_id,
        prize_tier_id,

        ---------- strings
        status as raw_status,
        case
            when status = 'completed' then 'PROCESSED'
            when status = 'error'     then 'FAILED'
            else                           'PENDING'
        end as payment_status,
        delivery_ref as wallet_reference_id,
        error_message,
        correlation_id,

        ---------- booleans

        ---------- dates
        cast(created_at as date) as created_date,

        ---------- timestamps
        processed_at::timestamp_ntz as payment_processed_at,
        completed_at::timestamp_ntz         as completed_at,
        confirmed_at::timestamp_ntz         as confirmed_at,
        processed_at::timestamp_ntz         as processed_at,
        created_at::timestamp_ntz           as created_at,
        greatest(
            coalesce(processed_at,  '1900-01-01'::timestamp_ntz),
            coalesce(completed_at,  '1900-01-01'::timestamp_ntz),
            coalesce(confirmed_at,  '1900-01-01'::timestamp_ntz)
        )::timestamp_ntz                    as updated_at

    from source

)

select * from renamed

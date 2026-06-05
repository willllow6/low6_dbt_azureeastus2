with

source as (

    select *
    from {{ source('opap_spintowin', 'entries') }}

),

renamed as (

    select

        ----------  ids
        id as entry_id,
        user_id,
        contest_id,
        prize_tier_id,

        ---------- strings
        case
            when settled_at is null                              then 'ACTIVE'
            when prize_tier_id is not null                       then 'WINNER'
            else                                                      'ELIMINATED'
        end as entry_status,

        ---------- numerics
        total_correct as points,

        ---------- booleans
        settled_at is null as is_active,

        ---------- dates
        cast(submitted_at as date) as entry_date,

        ---------- timestamps
        submitted_at::timestamp_ntz as entered_at,
        settled_at::timestamp_ntz   as settled_at,
        greatest(
            coalesce(submitted_at,  '1900-01-01'::timestamp_ntz),
            coalesce(settled_at,  '1900-01-01'::timestamp_ntz)
        )::timestamp_ntz                    as updated_at

    from source

)

select * from renamed

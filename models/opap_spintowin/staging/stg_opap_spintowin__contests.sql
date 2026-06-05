with

source as (

    select *
    from {{ source('opap_spintowin', 'contests') }}

),

renamed as (

    select

        ----------  ids
        id as contest_id,

        ---------- strings
        name:en::string as contest_title,
        status as contest_status,
        adapter_config:campaignId::string as campaign_id,
        adapter_config:round::string as round,

        ---------- client / canonical fields
        'opap'          as client_id,
        'opap'          as tenant_id,
        'OPAP'          as tenant_name,
        'spin_to_win'   as game_type,

        ---------- booleans
        status not in ('draft', 'closed', 'settled') as is_active,

        ---------- dates
        cast(closes_at as date) as contest_start_date,

        ---------- timestamps
        opens_at::timestamp_ntz  as contest_opens_at,
        closes_at::timestamp_ntz  as contest_starts_at,
        status_changed_at::timestamp_ntz  as status_changed_at,
        created_at::timestamp_ntz as created_at,
        greatest(
            coalesce(created_at,  '1900-01-01'::timestamp_ntz),
            coalesce(opens_at,  '1900-01-01'::timestamp_ntz),
            coalesce(closes_at,  '1900-01-01'::timestamp_ntz),
            coalesce(status_changed_at,  '1900-01-01'::timestamp_ntz)
        )::timestamp_ntz                    as updated_at

    from source

)

select * from renamed

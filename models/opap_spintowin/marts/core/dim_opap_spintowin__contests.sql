with

contests as (

    select *
    from {{ ref('stg_opap_spintowin__contests') }}

)

select
    contest_id,
    client_id,
    tenant_id,
    tenant_name,
    game_type,
    contest_title,
    contest_status,
    campaign_id,
    round,
    is_active,
    contest_opens_at,
    convert_timezone('UTC', '{{ var("opap_spintowin_local_timezone") }}', contest_opens_at)::timestamp_ntz as contest_opens_at_et,
    contest_start_date,
    cast(convert_timezone('UTC', '{{ var("opap_spintowin_local_timezone") }}', contest_starts_at) as date) as contest_start_date_et,
    contest_starts_at,
    convert_timezone('UTC', '{{ var("opap_spintowin_local_timezone") }}', contest_starts_at)::timestamp_ntz as contest_starts_at_et,
    created_at,
    updated_at
from contests

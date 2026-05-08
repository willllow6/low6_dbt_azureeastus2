with

contests as (

    select *
    from {{ ref('stg_penn__contests') }}

)

select
    contest_id,
    client_id,
    tenant_id,
    contest_title,
    game_type,
    contest_status,
    contest_prize,
    is_scored,
    is_active,
    contest_start_date,
    cast(convert_timezone('UTC', '{{ var("local_timezone") }}', contest_starts_at) as date) as contest_start_date_et,
    contest_opens_at,
    convert_timezone('UTC', '{{ var("local_timezone") }}', contest_opens_at)::timestamp_ntz as contest_opens_at_et,
    contest_starts_at,
    convert_timezone('UTC', '{{ var("local_timezone") }}', contest_starts_at)::timestamp_ntz as contest_starts_at_et,
    updated_at
from contests

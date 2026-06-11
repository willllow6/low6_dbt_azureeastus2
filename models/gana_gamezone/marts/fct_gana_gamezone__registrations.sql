with

users as (

    select * from {{ ref('stg_gana_gamezone__users') }}

)

select
    user_id,
    client_id,
    tenant_id,
    tenant_name,
    registration_type,
    registered_at,
    created_at,
    updated_at
from users

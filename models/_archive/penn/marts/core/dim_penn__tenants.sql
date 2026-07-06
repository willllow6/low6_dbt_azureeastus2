with

tenants as (

    select *
    from {{ ref('stg_penn__tenants') }}

)

select
    tenant_id,
    tenant_code,
    tenant_name,
    title,
    client_id
from tenants

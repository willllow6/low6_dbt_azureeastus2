with

users as (

    select *
    from {{ ref('stg_bet365_pickem__users') }}

),

entries as (

    select *
    from {{ ref('stg_bet365_pickem__entries') }}

),

tenant_rows as (

    select
        tenant_id,
        tenant_name,
        country_code,
        state_code,
        client_id
    from users

    union

    select
        tenant_id,
        tenant_name,
        country_code,
        state_code,
        client_id
    from entries

)

select
    tenant_id,
    tenant_name,
    country_code,
    state_code,
    client_id

from tenant_rows

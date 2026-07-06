-- DAU is based on daily player reveals, as users log in each day to reveal their selection.
with

reveals as (

    select
        user_id,
        client_id,
        tenant_id,
        game_type,
        cast(revealed_at_et as date) as reveal_date
    from {{ ref('fct_penn__selections') }}
    where status = 'revealed'
        or revealed_at is not null

),

users as (

    select
        user_id,
        registration_date_et as registration_date
    from {{ ref('dim_penn__users') }}

),

tenants as (

    select
        tenant_id,
        tenant_name
    from {{ ref('dim_penn__tenants') }}

),

daily_active as (

    select
        r.reveal_date as date_day,
        r.client_id,
        r.tenant_id,
        t.tenant_name,
        r.game_type,
        count(distinct r.user_id) as dau,
        count(distinct case
            when u.registration_date < r.reveal_date
            then r.user_id
        end) as returning_users
    from reveals as r
    left join users as u
        on r.user_id = u.user_id
    left join tenants as t
        on r.tenant_id = t.tenant_id
    group by
        r.reveal_date,
        r.client_id,
        r.tenant_id,
        t.tenant_name,
        r.game_type

)

select
    date_day,
    client_id,
    tenant_id,
    tenant_name,
    game_type,
    dau,
    0 as new_registrations,
    returning_users
from daily_active

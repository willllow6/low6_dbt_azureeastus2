with

reveals as (

    select
        user_id,
        tenant_id,
        client_id,
        game_type,
        cast(revealed_at_et as date) as date_day,
        count(selection_id) as reveals_count,
        count(distinct contest_id) as contests_active
    from {{ ref('fct_penn__selections') }}
    where status = 'revealed'
        or revealed_at is not null
    group by
        user_id,
        tenant_id,
        client_id,
        game_type,
        cast(revealed_at_et as date)

),

users as (

    select
        user_id,
        penn_user_id,
        registration_date_et
    from {{ ref('dim_penn__users') }}

),

tenants as (

    select
        tenant_id,
        tenant_name
    from {{ ref('dim_penn__tenants') }}

)

select
    r.date_day,
    r.user_id,
    u.penn_user_id,
    r.client_id,
    r.tenant_id,
    t.tenant_name,
    r.game_type,
    r.reveals_count,
    r.contests_active,
    datediff('day', u.registration_date_et, r.date_day) as days_since_registration
from reveals as r
left join users as u
    on r.user_id = u.user_id
left join tenants as t
    on r.tenant_id = t.tenant_id

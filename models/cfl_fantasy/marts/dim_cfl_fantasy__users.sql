with users as (
    select * from {{ ref('stg_cfl_fantasy__users') }}
),

entry_counts as (
    select
        user_id,
        count(*) as total_entries,
        min(entered_at) as first_entered_at
    from {{ ref('fct_cfl_fantasy__entries') }}
    group by 1
),

enriched as (
    select
        u.user_id,
        u.client_id,
        u.tenant_id,
        u.tenant_name,
        u.game_type,
        u.external_user_id,
        u.display_name,
        u.locale,
        u.registered_at,
        u.registered_date,
        u.registration_type,
        u.has_display_name_filled,
        u.is_verified,
        u.is_user,
        u.created_at,
        u.updated_at,
        coalesce(ec.total_entries, 0) as total_entries,
        ec.first_entered_at,
        case when ec.total_entries is not null then true else false end as has_entered

    from users u
    left join entry_counts ec on u.user_id = ec.user_id
)

select * from enriched

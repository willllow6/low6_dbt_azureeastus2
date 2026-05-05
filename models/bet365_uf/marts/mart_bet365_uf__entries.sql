with

entries as (
    select * from {{ ref('fct_bet365_uf__entries') }}
),

contests as (
    select * from {{ ref('dim_bet365_uf__contests') }}
),

tenants as (
    select * from {{ ref('stg_bet365_uf__tenants') }}
),

users as (
    select user_id, is_tester from {{ ref('stg_bet365_uf__users') }}
),

joined as (

    select
        entries.entry_id,
        entries.contest_id,
        entries.tenant_id,
        entries.user_id,
        entries.client_id,
        entries.game_type,
        entries.entry_score,
        entries.entry_date,
        entries.entered_at,
        entries.prize_paid_at,
        entries.entry_number,
        entries.tenant_entry_number,
        tenants.tenant_name,
        contests.contest_name,
        contests.contest_status,
        contests.contest_identifier,
        contests.starts_at as contest_starts_at,
        contests.ends_at as contest_ends_at,
        contests.contest_start_date,
        contests.contest_end_date,
        contests.prize_pool,
        contests.is_completed,
        contests.is_prizes_paid,
        contests.game_week_id,
        contests.game_week_name,
        contests.game_week_description,
        contests.game_week_start_date,
        contests.game_week_end_date,
        contests.game_week_starts_at,
        contests.game_week_ends_at,
        contests.competition_id,
        contests.competition_name,
        contests.is_international_competition,
        users.is_tester
    from entries
    inner join contests
        on entries.contest_id = contests.contest_id
    left join tenants
        on entries.tenant_id = tenants.tenant_id
    left join users
        on entries.user_id = users.user_id

)

select * from joined

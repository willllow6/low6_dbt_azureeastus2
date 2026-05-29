with

contests as (

    select
        contest_id,
        tournament_id,
        tournament_name,
        contest_name,
        slug,
        sequence,
        reveal_type,
        reveal_count,
        contest_status,
        is_active,
        game_type,
        client_id,
        tenant_id,
        tenant_name,
        contest_starts_at,
        contest_starts_at_et,
        contest_start_date_et,
        contest_ends_at,
        contest_ends_at_et
    from {{ ref('dim_penn__contests') }}

),

entry_metrics as (

    select
        contest_id,
        count(entry_id) as total_entries,
        count(distinct user_id) as unique_entrants
    from {{ ref('fct_penn__entries') }}
    group by contest_id

),

selection_metrics as (

    -- Exclude slots whose reveal window has not yet opened.
    -- convert_timezone('UTC', ...) normalises reveal_start to UTC before comparing
    -- to sysdate() (which Snowflake always returns in UTC), guarding against
    -- account-level or session timezone settings on timestamp_tz source values.
    select
        contest_id,
        count(selection_id) as total_slots,
        count(case when status = 'revealed' then selection_id end) as revealed_selections,
        count(case when status = 'missed'   then selection_id end) as missed_selections,
        count(case when status = 'pending'  then selection_id end) as pending_selections
    from {{ ref('fct_penn__selections') }}
    where convert_timezone('UTC', reveal_start)::timestamp_ntz <= sysdate()
    group by contest_id

),

award_metrics as (

    select
        contest_id,
        count(award_id) as total_awards,
        sum(prize_amount) as total_prize_amount
    from {{ ref('fct_penn__awards') }}
    where award_status = 'delivered'
    group by contest_id

),

joined as (

    select
        c.contest_id,
        c.tournament_id,
        c.tournament_name,
        c.contest_name,
        c.slug,
        c.sequence,
        c.reveal_type,
        c.reveal_count,
        c.contest_status,
        c.is_active,
        c.game_type,
        c.client_id,
        c.tenant_id,
        c.tenant_name,
        c.contest_starts_at,
        c.contest_starts_at_et,
        c.contest_start_date_et,
        c.contest_ends_at,
        c.contest_ends_at_et,

        coalesce(em.total_entries, 0) as total_entries,
        coalesce(em.unique_entrants, 0) as unique_entrants,
        coalesce(sm.total_slots, 0) as total_slots,
        coalesce(sm.revealed_selections, 0) as revealed_selections,
        coalesce(sm.missed_selections, 0) as missed_selections,
        coalesce(sm.pending_selections, 0) as pending_selections,
        coalesce(am.total_awards, 0) as total_awards,
        coalesce(am.total_prize_amount, 0) as total_prize_amount,

        round(
            coalesce(sm.revealed_selections, 0) / nullif(coalesce(em.total_entries, 0), 0),
            2
        ) as avg_reveals_per_entry

    from contests as c
    left join entry_metrics as em
        on c.contest_id = em.contest_id
    left join selection_metrics as sm
        on c.contest_id = sm.contest_id
    left join award_metrics as am
        on c.contest_id = am.contest_id

)

select * from joined

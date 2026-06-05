with

entries as (

    select *
    from {{ ref('stg_opap_spintowin__entries') }}

),

contests as (

    select
        contest_id,
        contest_title,
        contest_status,
        contest_start_date,
        contest_starts_at,
        contest_opens_at,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        updated_at as contest_updated_at
    from {{ ref('stg_opap_spintowin__contests') }}

),

prize_tiers as (

    select
        prize_tier_id,
        prize_type,
        prize_amount
    from {{ ref('stg_opap_spintowin__prize_tiers') }}

),

joined as (

    select
        e.entry_id,
        e.user_id,
        e.contest_id,
        c.client_id,
        c.tenant_id,
        c.tenant_name,
        c.game_type,
        c.contest_title,
        c.contest_status,
        c.contest_start_date,
        c.contest_starts_at,
        c.contest_opens_at,
        e.points,
        e.prize_tier_id,
        pt.prize_type,
        pt.prize_amount,
        null::integer                       as tiebreaker_prediction,
        null::integer                       as tiebreaker_outcome,
        null::integer                       as tiebreaker_error,
        e.entry_status,
        e.is_active,
        e.entry_date,
        e.entered_at,
        e.settled_at,
        greatest(
            coalesce(e.updated_at,           '1900-01-01'::timestamp_ntz),
            coalesce(c.contest_updated_at,   '1900-01-01'::timestamp_ntz)
        ) as updated_at
    from entries e
    left join contests c
        on e.contest_id = c.contest_id
    left join prize_tiers pt
        on e.prize_tier_id = pt.prize_tier_id

),

with_entry_number as (

    select
        *,
        rank() over (
            partition by user_id
            order by entered_at
        ) as user_entry_number
    from joined

)

select * from with_entry_number

with

user_rounds as (

    select *
    from {{ ref('stg_penn__user_rounds') }}

),

rounds as (

    select
        round_id,
        tournament_id,
        contest_name,
        sequence
    from {{ ref('stg_penn__rounds') }}

),

tournaments as (

    select
        tournament_id,
        tenant_id
    from {{ ref('stg_penn__tournaments') }}

),

joined as (

    select
        ur.user_round_id as entry_id,
        ur.user_id,
        r.round_id as contest_id,
        r.tournament_id,
        t.tenant_id,
        'penn' as client_id,
        'squads' as game_type,
        ur.user_tier,
        ur.status,
        ur.locked_at,
        ur.prize_outcome_id,
        ur.prize_revealed_at,
        ur.per_goal_amount,
        ur.per_goal_currency,
        ur.entry_date,
        ur.entered_at,
        ur.updated_at
    from user_rounds ur
    inner join rounds r
        on ur.round_id = r.round_id
    inner join tournaments t
        on r.tournament_id = t.tournament_id

)

select * from joined

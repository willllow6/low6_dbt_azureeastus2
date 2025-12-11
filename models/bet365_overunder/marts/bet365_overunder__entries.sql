with

entries as (

    select *
    from {{ ref('stg_bet365_overunder__entries') }}

),

users as (

    select *
    from {{ ref('bet365_overunder__users') }}

),

picks as (

    select *
    from {{ ref('stg_bet365_overunder__picks') }}

),

entry_picks as (

    select
        entry_id,
        sum(case when sport_name = 'NBA' then 1 else 0 end) as nba_picks,
        sum(case when sport_name = 'NFL' then 1 else 0 end) as nfl_picks,
        sum(case when sport_name = 'NHL' then 1 else 0 end) as nhl_picks,

        case
            when nba_picks > 0 and nfl_picks = 0 and nhl_picks = 0 then 'NBA only'
            when nba_picks = 0 and nfl_picks > 0 and nhl_picks = 0 then 'NFL only'
            when nba_picks = 0 and nfl_picks = 0 and nhl_picks > 0 then 'NHL only'
            when nba_picks > 0 and nfl_picks > 0 and nhl_picks = 0 then 'NBA & NFL'
            when nba_picks > 0 and nfl_picks = 0 and nhl_picks > 0 then 'NBA & NHL'
            when nba_picks = 0 and nfl_picks > 0 and nhl_picks > 0 then 'NFL & NHL'
            when nba_picks > 0 and nfl_picks > 0 and nhl_picks > 0 then 'NBA, NFL & NHL'
            else 'No sport selected'
        end as sport_combination
        from picks
    group by 1

),

ranked_entries as (

    select
        *,
        row_number() over (partition by user_id order by entered_at) as user_entry_number
    from entries

),

first_wins as (

    select
        user_id,
        entry_id,
        entered_at
    from ranked_entries
    where prize_amount > 0
    qualify row_number() over (
                partition by user_id
                order by entered_at
            ) = 1
),

joined as (

    select
        ranked_entries.entry_id,
        ranked_entries.user_id,
        users.gaming_id,

        case 
            when ranked_entries.user_entry_number = 1
                then 'First Entry'
            else 'Repeat Entry'
        end as entry_type,
        users.country,
        users.state_province,
        users.segment_group,
        entry_picks.sport_combination,

        ranked_entries.user_entry_number,
        ranked_entries.contest_date_et,
        ranked_entries.entered_picks,
        ranked_entries.scored_picks,
        ranked_entries.correct_picks,
        entry_picks.nba_picks,
        entry_picks.nfl_picks,
        entry_picks.nhl_picks,
        ranked_entries.potential_prize_amount,
        ranked_entries.prize_amount,
        users.currency_code,

        ranked_entries.is_winner,
        case
            when first_wins.entry_id is not null
                then true
            else false
        end as is_first_win,

        ranked_entries.entry_hour,
        ranked_entries.entry_hour_et,
        ranked_entries.entry_date,
        ranked_entries.entry_date_et,

        ranked_entries.entered_at,
        ranked_entries.entered_at_et,
        ranked_entries.locked_at,
        ranked_entries.locked_at_et,
        ranked_entries.settled_at,
        ranked_entries.settled_at_et

    from ranked_entries
    inner join users
        on ranked_entries.user_id = users.user_id
    left join first_wins
        on ranked_entries.entry_id = first_wins.entry_id
    left join entry_picks
        on ranked_entries.entry_id = entry_picks.entry_id

)

select * from joined
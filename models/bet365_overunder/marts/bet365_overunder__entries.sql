with

entries as (

    select *
    from {{ ref('stg_bet365_overunder__entries') }}

),

users as (

    select *
    from {{ ref('bet365_overunder__users') }}

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

        ranked_entries.user_entry_number,
        ranked_entries.entered_picks,
        ranked_entries.scored_picks,
        ranked_entries.correct_picks,
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
        ranked_entries.entry_date,

        ranked_entries.entered_at,
        ranked_entries.locked_at,
        ranked_entries.settled_at

    from ranked_entries
    inner join users
        on ranked_entries.user_id = users.user_id
    left join first_wins
        on ranked_entries.entry_id = first_wins.entry_id

)

select * from joined
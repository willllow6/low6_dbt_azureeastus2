with

entries as (

    select *
    from {{ ref('fct_bet365_overunder__entries') }}

),

calculate_rfm_metrics as (

    select
        user_id,
        client_id,
        tenant_id,
        max(country) as country,
        max(entry_date) as last_entry_date,
        count(*) as entries
    from entries
    group by 1,2,3

),

create_segments as (

    select
        user_id,
        client_id,
        tenant_id,
        country,
        last_entry_date,
        entries,
        datediff('day', last_entry_date, current_date()) as days_since_last_played,

        case
            when entries = 1
                then 'Low'
            else 'High'
        end as frequency,

        case
            when days_since_last_played <= 7
                then 'High'
            when days_since_last_played <= 21
                then 'Med'
            when days_since_last_played > 21
                then 'Low'
        end as recency,

        case
            when frequency = 'Low' and recency = 'High'
                then 'New Users'
            when frequency = 'Low' and recency = 'Med'
                then 'Recently Tried'
            when frequency = 'Low' and recency = 'Low'
                then 'One Hitter Quitter'
            when frequency = 'High' and recency = 'High'
                then 'Active Players'
            when frequency = 'High' and recency = 'Med'
                then 'At Risk'
            when frequency = 'High' and recency = 'Low'
                then 'Hibernating'
        end as segment

    from calculate_rfm_metrics

)

select * from create_segments

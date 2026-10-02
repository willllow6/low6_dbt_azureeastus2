{#
    RFM segmentation CTEs, for use inside a model's `with` list.
    entries_cte must name a CTE with one row per entry and columns
    user_id, entry_date_et. Produces a CTE named user_rfm_segments with one
    row per user.

    Clock-dependent (days since last entry is measured against getdate(), or
    var reporting_as_of_date when set), so only call it from rpt_ or shared_
    models.
#}
{% macro user_rfm_segments_ctes(entries_cte) %}

{%- set as_of_date = var('reporting_as_of_date', none) -%}
{%- set as_of = "cast('" ~ as_of_date ~ "' as date)" if as_of_date else "getdate()" -%}

rfm_metrics as (

    select
        user_id,
        max(entry_date_et) as last_entry_date,
        count(*) as entries,
        datediff('day', max(entry_date_et), {{ as_of }}) as days_since_last_entry
    from {{ entries_cte }}
    group by user_id

),

rfm_scores as (

    select
        user_id,
        last_entry_date,
        entries,
        days_since_last_entry,

        case
            when entries = 1
                then 'Low'
            else 'High'
        end as frequency,

        case
            when days_since_last_entry <= 7
                then 'High'
            when days_since_last_entry <= 21
                then 'Med'
            when days_since_last_entry > 21
                then 'Low'
        end as recency

    from rfm_metrics

),

user_rfm_segments as (

    select
        user_id,
        last_entry_date,
        entries,
        days_since_last_entry,
        frequency,
        recency,

        case
            when frequency = 'Low' and recency = 'High'
                then 'New Users'
            when frequency = 'Low' and recency = 'Med'
                then 'Recently Tried'
            when frequency = 'Low' and recency = 'Low'
                then 'One Hitter Quitter'
            when frequency = 'High' and recency = 'High'
                then 'Active Users'
            when frequency = 'High' and recency = 'Med'
                then 'At Risk'
            when frequency = 'High' and recency = 'Low'
                then 'Hibernating'
        end as segment

    from rfm_scores

)

{% endmacro %}

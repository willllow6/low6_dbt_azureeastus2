{#
    Standard rpt_{domain}__contest_performance_summary.
    Grain: contest_id x tenant_id. No money and no win metrics.

    contests    dim_{domain}__contests
    entries     fct_{domain}__entries
    payouts     fct_{domain}__payouts, or none when the domain has no payouts
                (paid_out_entries is then 0)
    extra_attributes
                contest columns appended after the standard columns; must be
                1:1 with the contest
    extra_metrics
                {name: aggregate expression over entries}, appended last
#}
{% macro rpt_contest_performance_summary(
    contests,
    entries,
    payouts=none,
    extra_attributes=[],
    extra_metrics={}
) %}

with

contests as (

    select *
    from {{ contests }}

),

entries as (

    select *
    from {{ entries }}

),

{% if payouts is not none -%}
payouts as (

    select *
    from {{ payouts }}

),

paid_out_entries as (

    select distinct entry_id
    from payouts

),
{%- endif %}

entry_metrics as (

    select
        entries.contest_id,
        entries.tenant_id,
        count(*) as total_entries,
        count(distinct entries.user_id) as unique_entrants,
        count_if(entries.user_entry_number = 1) as first_time_entrants,
        {% if payouts is not none -%}
        count(paid_out_entries.entry_id) as paid_out_entries
        {%- else -%}
        0 as paid_out_entries
        {%- endif %}
        {%- for name, expression in extra_metrics.items() %},
        {{ expression }} as {{ name }}
        {%- endfor %}
    from entries
    {% if payouts is not none -%}
    left join paid_out_entries
        on entries.entry_id = paid_out_entries.entry_id
    {%- endif %}
    group by entries.contest_id, entries.tenant_id

)

select
    contests.contest_id,
    contests.client_id,
    contests.tenant_id,
    contests.tenant_name,
    contests.game_type,

    contests.contest_name,
    contests.contest_status,
    contests.starts_at,
    contests.ends_at,

    coalesce(entry_metrics.total_entries, 0) as total_entries,
    coalesce(entry_metrics.unique_entrants, 0) as unique_entrants,
    coalesce(entry_metrics.first_time_entrants, 0) as first_time_entrants,
    coalesce(entry_metrics.paid_out_entries, 0) as paid_out_entries
    {%- for attribute in extra_attributes %},
    contests.{{ attribute }}
    {%- endfor %}
    {%- for name in extra_metrics %},
    entry_metrics.{{ name }}
    {%- endfor %}

from contests
left join entry_metrics
    on contests.contest_id = entry_metrics.contest_id
    and contests.tenant_id = entry_metrics.tenant_id

{% endmacro %}

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
    tenants_from_entries
                true when contests are not tied to a tenant (contest tenant_id
                is null) and the tenants come from the entries instead, e.g.
                bet365_pickem's country-state jurisdictions. A contest then has
                one row per tenant it was entered from, and one row with null
                tenant_id while it has no entries.
#}
{% macro rpt_contest_performance_summary(
    contests,
    entries,
    payouts=none,
    extra_attributes=[],
    extra_metrics={},
    tenants_from_entries=false
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

{% if tenants_from_entries -%}
contest_tenants as (

    select
        contest_id,
        tenant_id,
        max(tenant_name) as tenant_name
    from entries
    group by contest_id, tenant_id

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
    {% if tenants_from_entries -%}
    coalesce(contest_tenants.tenant_id, contests.tenant_id) as tenant_id,
    coalesce(contest_tenants.tenant_name, contests.tenant_name) as tenant_name,
    {%- else -%}
    contests.tenant_id,
    contests.tenant_name,
    {%- endif %}
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
{% if tenants_from_entries -%}
left join contest_tenants
    on contests.contest_id = contest_tenants.contest_id
left join entry_metrics
    on contest_tenants.contest_id = entry_metrics.contest_id
    and contest_tenants.tenant_id = entry_metrics.tenant_id
{%- else -%}
left join entry_metrics
    on contests.contest_id = entry_metrics.contest_id
    and contests.tenant_id = entry_metrics.tenant_id
{%- endif %}

{% endmacro %}

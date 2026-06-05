with

users as (

    select
        user_id,
        registration_date_et    as date_day
    from {{ ref('dim_opap_spintowin__users') }}

),

entries as (

    select
        user_id,
        count(*)    as total_entries
    from {{ ref('fct_opap_spintowin__entries') }}
    group by 1

),

cohort as (

    select
        u.date_day,
        'opap'                                                                          as client_id,
        'opap'                                                                          as tenant_id,
        'OPAP'                                                                          as tenant_name,
        'spin_to_win'                                                                   as game_type,
        count(u.user_id)                                                                as registered,
        count(case when coalesce(e.total_entries, 0) >= 1 then u.user_id end)           as activated,
        count(case when coalesce(e.total_entries, 0) >= 2 then u.user_id end)           as repeat_entrants
    from users u
    left join entries e
        on u.user_id = e.user_id
    group by 1, 2, 3, 4, 5

),

with_rates as (

    select
        date_day,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        registered,
        activated,
        case
            when registered > 0 then round(activated / registered::float, 4)
            else null
        end                                     as activation_rate,
        repeat_entrants,
        case
            when activated > 0 then round(repeat_entrants / activated::float, 4)
            else null
        end                                     as repeat_rate
    from cohort

)

select * from with_rates

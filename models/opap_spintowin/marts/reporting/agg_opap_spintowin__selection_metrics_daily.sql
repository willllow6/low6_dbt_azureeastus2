with

selections as (

    select *
    from {{ ref('fct_opap_spintowin__selections') }}

),

daily as (

    select
        cast(created_at as date)                                                as date_day,
        client_id,
        tenant_id,
        'OPAP'                                                                  as tenant_name,
        game_type,
        contest_id,
        count(*)                                                                as total_selections,
        count(case when selection_status = 'WIN' then 1 end)                    as correct_selections,
        case
            when count(case when selection_status != 'PENDING' then 1 end) > 0
                then round(
                    count(case when selection_status = 'WIN' then 1 end)
                    / count(case when selection_status != 'PENDING' then 1 end)::float,
                    4
                )
            else null
        end                                                                     as accuracy_rate
    from selections
    group by 1, 2, 3, 4, 5, 6

)

select * from daily

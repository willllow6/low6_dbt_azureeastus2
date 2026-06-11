with

survivor_contests as (

    select * from {{ ref('stg_gana_gamezone__survivor_contests') }}

),

countries as (

    select * from {{ ref('stg_gana_gamezone__countries') }}

),

enriched as (

    select
        sc.contest_id,
        sc.client_id,
        sc.tenant_id,
        sc.tenant_name,
        sc.game_type,
        sc.contest_name,
        sc.contest_status,
        sc.question_type,
        sc.day,
        sc.is_active,
        sc.is_completed,
        sc.free_bet_prize,
        sc.answers,
        c1.country_name || ' vs ' || c2.country_name   as competing_teams,
        sc.correct_country_id,
        correct.country_name                            as correct_country_name,
        correct.country_code                            as correct_country_code,
        sc.ends_at,
        sc.created_at,
        sc.updated_at
    from survivor_contests as sc
    left join countries as c1
        on sc.answers[0]::varchar = c1.country_id
    left join countries as c2
        on sc.answers[1]::varchar = c2.country_id
    left join countries as correct
        on sc.correct_country_id = correct.country_id

)

select * from enriched

with

bracket_matches as (

    select * from {{ ref('stg_gana_gamezone__bracket_matches') }}

),

countries as (

    select * from {{ ref('stg_gana_gamezone__countries') }}

),

enriched as (

    select
        bm.match_id,
        bm.round,
        bm.bracket_position,
        bm.is_active,
        bm.countries,
        c1.country_name || ' vs ' || c2.country_name   as competing_teams,
        bm.correct_country_id,
        correct.country_name                            as correct_country_name,
        correct.country_code                            as correct_country_code,
        bm.created_at,
        bm.updated_at
    from bracket_matches as bm
    left join countries as c1
        on bm.countries[0]:country_id::varchar = c1.country_id
    left join countries as c2
        on bm.countries[1]:country_id::varchar = c2.country_id
    left join countries as correct
        on bm.correct_country_id = correct.country_id

)

select * from enriched

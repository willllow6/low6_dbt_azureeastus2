with

bracket_selections as (

    select * from {{ ref('stg_gana_gamezone__bracket_selections') }}

),

bracket_matches as (

    select * from {{ ref('stg_gana_gamezone__bracket_matches') }}

),

latest_per_match as (

    select
        selection_id,
        user_id,
        match_id,
        contest_id,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        selected_country_id,
        selected_at,
        updated_at
    from bracket_selections
    qualify row_number() over (partition by user_id, match_id order by selected_at desc) = 1

),

enriched as (

    select
        ls.selection_id,
        ls.user_id,
        ls.match_id,
        ls.contest_id,
        ls.client_id,
        ls.tenant_id,
        ls.tenant_name,
        ls.game_type,
        ls.selected_country_id,
        case
            when bm.correct_country_id is not null
            then ls.selected_country_id = bm.correct_country_id
            else null
        end                                 as is_correct,
        ls.selected_at,
        ls.updated_at
    from latest_per_match as ls
    left join bracket_matches as bm
        on ls.match_id = bm.match_id

)

select * from enriched

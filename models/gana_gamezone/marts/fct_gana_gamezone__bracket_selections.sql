with

bracket_selections as (

    select * from {{ ref('int_gana_gamezone__bracket_selections') }}

),

final as (

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
        is_correct,
        row_number() over (
            partition by user_id order by selected_at
        )                                   as selection_number,
        selected_at,
        updated_at
    from bracket_selections

)

select * from final

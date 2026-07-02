with

bracket_selections as (

    select * from {{ ref('int_gana_gamezone__bracket_selections') }}

),

entries as (

    select
        user_id || '-gana_bracket'  as entry_id,
        user_id,
        'gana_bracket'              as contest_id,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        count(*)                    as total_selections,
        min(selected_at)            as entered_at,
        max(updated_at)             as updated_at
    from bracket_selections
    group by 1, 2, 3, 4, 5, 6, 7

)

select * from entries

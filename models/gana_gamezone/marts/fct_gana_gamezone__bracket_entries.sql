with

bracket_entries as (

    select * from {{ ref('int_gana_gamezone__bracket_entries') }}

),

final as (

    select
        entry_id,
        user_id,
        contest_id,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        total_selections,
        row_number() over (
            partition by user_id order by entered_at
        )                                   as entry_number,
        entered_at,
        updated_at
    from bracket_entries

)

select * from final

with

group_predictions as (

    select * from {{ ref('stg_gana_gamezone__group_predictions') }}

),

entries as (

    select
        entry_id,
        user_id,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        contest_id,
        row_number() over (
            partition by user_id
            order by entered_at
        )                                               as entry_number,
        entered_at,
        updated_at
    from group_predictions

)

select * from entries

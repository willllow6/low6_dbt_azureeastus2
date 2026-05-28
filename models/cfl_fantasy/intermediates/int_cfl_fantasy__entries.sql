with teams as (
    select * from {{ ref('stg_cfl_fantasy__teams') }}
),

entries_ranked as (
    select
        entry_id,
        contest_id,
        user_id,
        client_id,
        tenant_id,
        tenant_name,
        game_type,
        team_name,
        image_id,
        waiver_priority,
        is_commissioner,
        is_auto_draft,
        is_auto_team,
        pre_draft_queue,
        entered_at,
        updated_at,

        row_number() over (
            partition by user_id
            order by entered_at
        ) as entry_number

    from teams
)

select * from entries_ranked

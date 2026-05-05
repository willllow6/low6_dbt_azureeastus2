with

source as (

    select *
    from {{ source('bet365_uf', 'LINEUP_SLOTS') }}

),

renamed as (

    select

        ----------  ids
        LineupSlotId as selection_id,
        LineupId as entry_id,
        TenantId as tenant_id,
        UserId as user_id,
        CardId as card_id,
        ScoreableId as scoreable_id,
        'bet365' as client_id,

        ---------- strings
        'fantasy' as game_type,
        MatchStatus as match_status,

        ---------- numerics
        Position as selection_position,
        Modifier as selection_modifier,
        Score as selection_score,
        ScoreBreakdown as selection_score_breakdown,

        ---------- booleans

        ---------- dates
        cast(CreatedAt as date) as selected_date,

        ---------- timestamps
        ScoreUpdatedAt as score_updated_at,
        UpdatedAt as updated_at,
        CreatedAt as selected_at

    from source

)

select * from renamed

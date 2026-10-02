with

selections as (

    select *
    from {{ ref('stg_oilers_picks__selections') }}

),

aggregated as (

    select
        -- entry_id keeps the legacy '{user_id}-{contest_id}' format because
        -- shared_oilers_picks__entries has always exposed it to the client.
        user_id || '-' || contest_id as entry_id,
        user_id,
        contest_id,

        client_id,
        tenant_id,
        tenant_name,
        game_type,

        count(*) as selections,
        max(tiebreak_prediction) as tiebreak_prediction,

        min(selected_at) as entered_at

    from selections
    group by user_id, contest_id, client_id, tenant_id, tenant_name, game_type

)

select
    entry_id,
    user_id,
    contest_id,

    client_id,
    tenant_id,
    tenant_name,
    game_type,

    selections,
    tiebreak_prediction,
    row_number() over (
        partition by user_id
        order by entered_at, contest_id
    ) as user_entry_number,

    entered_at

from aggregated

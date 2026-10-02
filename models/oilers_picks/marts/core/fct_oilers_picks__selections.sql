with

selections as (

    select *
    from {{ ref('stg_oilers_picks__selections') }}

),

options as (

    select *
    from {{ ref('stg_oilers_picks__options') }}

)

select
    selections.selection_id,
    selections.user_id || '-' || selections.contest_id as entry_id,
    selections.user_id,
    selections.contest_id,
    selections.question_id,
    selections.option_id,

    selections.client_id,
    selections.tenant_id,
    selections.tenant_name,
    selections.game_type,

    selections.tiebreak_prediction,
    options.is_correct,

    selections.selected_at,
    convert_timezone('UTC', '{{ var("local_timezone") }}', selections.selected_at) as selected_at_et

from selections
left join options
    on selections.option_id = options.option_id

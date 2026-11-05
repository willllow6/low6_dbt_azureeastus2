with

selections as (

    select *
    from {{ ref('stg_du_trivia__selections') }}

),

aggregated as (

    select
        entry_id,

        count(*) as selections,
        count_if(is_correct) as correct_selections,
        sum(elapsed_seconds) as elapsed_seconds

    from selections
    group by entry_id

)

select
    entry_id,

    selections,
    correct_selections,
    elapsed_seconds

from aggregated

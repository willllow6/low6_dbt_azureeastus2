with

source as (

    select *
    from {{ source('oilers_picks', 'userpickempoints') }}

),

renamed as (

    select

        ----------  ids
        userpickempointsid as entry_score_id,
        userid::varchar as user_id,
        pickemid::varchar as contest_id,
        'oilers' as client_id,
        'oilers' as tenant_id,

        ---------- strings
        'Edmonton Oilers' as tenant_name,
        'pickem' as game_type,

        ---------- numerics
        points as entry_points,
        tiebreakdiff as tiebreak_diff,

        ---------- timestamps
        createdat as scored_at

    from source

)

select * from renamed

with

source as (

    select *
    from {{ source('bet365_pickem', 'questions') }}

),

renamed as (

    select

        ----------  ids
        id::varchar as question_id,
        contest_fixture_id::varchar as contest_fixture_id,
        'bet365' as client_id,

        ---------- strings
        'pickem' as game_type,
        definition_key,
        type as question_type,
        copy_key,
        copy_override,
        -- copy_key is a translation key ('questions.fight_winner'), not display
        -- text: label the known definitions, and proper-case any new ones
        coalesce(
            copy_override,
            case definition_key
                when 'fight_winner' then 'Fight Winner'
                when 'method_of_victory' then 'Method of Victory'
                when 'round_of_victory' then 'Round of Victory'
                when 'sig_strikes' then 'Significant Strikes'
                else replace(initcap(replace(definition_key, '_', ' ')), ' Of ', ' of ')
            end
        ) as question_text,
        params as question_params,

        ---------- numerics
        position as question_position,

        ---------- booleans
        removed_at is null as is_active,

        ---------- timestamps
        created_at::timestamp_ntz as created_at,
        removed_at::timestamp_ntz as removed_at

    from source

)

select * from renamed

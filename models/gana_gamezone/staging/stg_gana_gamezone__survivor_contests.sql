with

source as (

    select * from {{ source('gana_gamezone', 'survivor_matches') }}

),

renamed as (

    select

        ---------- ids
        id                                  as contest_id,

        ---------- strings
        'gana'                              as client_id,
        'gana'                              as tenant_id,
        'Gana'                              as tenant_name,
        'survivor'                          as game_type,
        question                            as contest_name,
        question_type,
        status                              as contest_status,
        free_bet_prize,

        ---------- numerics
        day,

        ---------- booleans
        not is_completed                    as is_active,
        is_completed,

        ---------- semi-structured
        answers,
        correct_answer::varchar             as correct_country_id,

        ---------- timestamps
        cast(deadline as timestamp_ntz)     as ends_at,
        cast(created_at as timestamp_ntz)   as created_at,
        cast(updated_at as timestamp_ntz)   as updated_at

    from source

)

select * from renamed

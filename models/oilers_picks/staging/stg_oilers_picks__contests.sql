with

source as (

    select *
    from {{ source('oilers_picks', 'pickems') }}

),

renamed as (

    select

        ----------  ids
        pickemid::varchar as contest_id,
        'oilers' as client_id,
        'oilers' as tenant_id,

        ---------- strings
        'Edmonton Oilers' as tenant_name,
        'pickem' as game_type,
        titleen as contest_name,
        pickemstatus as contest_status,
        prizetexten as prize_text,

        ---------- booleans
        pickemstatus not in ('DRAFT', 'COMPLETE') as is_active,

        ---------- timestamps
        createdat as created_at,
        allowselectionstartutc as selections_open_at,
        startutc as starts_at,
        updatedat as updated_at

    from source

)

select * from renamed

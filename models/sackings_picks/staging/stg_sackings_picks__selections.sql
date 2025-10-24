with

source as (

    select *
    from {{ source('sackings_picks', 'cutsomerresponse') }}

),

renamed as (

    select
        id as user_selection_id,
        customerid as user_id,
        questionheaderid as question_id,
        contestid as contest_id,
        responseid as selection_id,
        responsetext as selection_text,
        isuserselected as is_user_selected,
        sequence,
        isactive as is_active,
        creationdate as selected_at,
        cast(creationdate as date) as selection_date,
        convert_timezone('UTC','America/New_York',creationdate) as selected_at_et,
        cast(selected_at_et as date) as selection_date_et,
        modifieddate as modified_at,
        cast(modifieddate as date) as modified_date,
        'sackings_picks' as source
    from source
    
)

select * from renamed
with

source as (

    select *
    from {{ source('opap_spintowin', 'picks') }}

),

renamed as (

    select

        ----------  ids
        id as pick_id,
        entry_id,
        reel_option_id,

        ---------- strings
        result,
        submission_state as selection_status,

        ---------- booleans

        ---------- dates

        ---------- timestamps
        scored_at::timestamp_ntz  as scored_at

    from source

)

select * from renamed

with

source as (

    select *
    from {{ source('opap_spintowin', 'contest_reels') }}

),

renamed as (

    select

        ----------  ids
        id as reel_id,
        contest_id,

        ---------- strings
        label as reel_label,

        ---------- numerics
        reel_number

        ---------- booleans

        ---------- dates

        ---------- timestamps

    from source

)

select * from renamed

with

source as (

    select *
    from {{ source('opap_spintowin', 'prize_tiers') }}

),

renamed as (

    select

        ----------  ids
        id as prize_tier_id,
        contest_id,

        ---------- strings
        prize_type,
        display_value as prize_display_value,
        segment_ref,

        ---------- numerics
        correct_count,
        coalesce(adapter_config:amount::number, adapter_config:value::number) as prize_amount,

        ---------- booleans

        ---------- dates

        ---------- timestamps
        created_at::timestamp_ntz as created_at,
        updated_at::timestamp_ntz as updated_at

    from source

)

select * from renamed

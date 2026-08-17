with

prizes as (

    select *
    from {{ ref('stg_olybet_casino__prizes') }}

)

select
    prize_id,
    prize_name,
    prize_description,
    prize_image_url,
    prize_type,
    prize_value,
    is_deleted,
    prize_expires_at,
    created_at,
    deleted_at
from prizes

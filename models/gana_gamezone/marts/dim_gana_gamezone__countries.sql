with

countries as (

    select * from {{ ref('stg_gana_gamezone__countries') }}

)

select
    country_id,
    country_name,
    country_code,
    odds,
    created_at,
    updated_at
from countries

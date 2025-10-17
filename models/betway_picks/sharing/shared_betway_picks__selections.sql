{{ config(
    materialized='view',
    secure=true,
    post_hook="{{ share_view('analytics_betway_picks', 'shared_betway_picks__selections', 'betway_share') }}"
) }}

with

selections as (

    select * from {{ ref('betway_picks__selections') }}

)

select * from selections
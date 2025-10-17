{{ config(
    materialized='view',
    secure=true,
    post_hook="{{ share_view('analytics_betway_picks', 'shared_betway_picks__entries', 'betway_share') }}"
) }}

with

entries as (

    select * from {{ ref('betway_picks__entries') }}

)

select * from entries
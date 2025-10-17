{{ config(
    materialized='view',
    secure=true,
    post_hook="{{ share_view('analytics_betway_picks', 'shared_betway_picks__users', 'betway_share') }}"
) }}

with

users as (

    select * from {{ ref('stg_betway_picks__users') }}

)

select * from users
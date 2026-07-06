with

entry_days as (

    select
        distinct
        user_id,
        created_date_et
    from {{ ref('stg_pln_arcade__entries') }}

)

select * from entry_days
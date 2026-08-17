with

entries as (

    select *
    from {{ ref('stg_olybet_casino__competition_entries') }}

),

-- Proxy registration: no users source table exists for this domain, so
-- registered_at is approximated as each user's first competition entry.
-- registration_type is unknowable and left null downstream.
first_entries as (

    select
        user_id,
        min(entered_at) as registered_at,
        cast(min(entered_at) as date) as registration_date
    from entries
    group by 1

)

select * from first_entries

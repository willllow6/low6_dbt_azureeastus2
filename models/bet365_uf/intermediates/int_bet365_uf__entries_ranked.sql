with

entries as (
    select * from {{ ref('stg_bet365_uf__entries') }}
),

ranked as (

    select
        *,
        row_number() over (
            partition by user_id
            order by entered_at
        ) as entry_number,
        row_number() over (
            partition by user_id, tenant_id
            order by entered_at
        ) as tenant_entry_number
    from entries

)

select * from ranked

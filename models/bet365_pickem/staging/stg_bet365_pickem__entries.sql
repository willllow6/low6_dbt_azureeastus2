with

source as (

    select *
    from {{ source('bet365_pickem', 'entries') }}

),

renamed as (

    select

        ----------  ids
        id::varchar as entry_id,
        player_id::varchar as user_id,
        contest_id::varchar as contest_id,
        boosted_contest_fixture_id::varchar as boosted_contest_fixture_id,
        'bet365' as client_id,
        country_code || coalesce('-' || nullif(state_code, ''), '') as tenant_id,

        ---------- strings
        country_code || coalesce('-' || nullif(state_code, ''), '') as tenant_name,
        'pickem' as game_type,
        country_code,
        nullif(state_code, '') as state_code,
        currency,
        case
            when refused_at is not null then 'refused'
            when counted_at is not null then 'counted'
            when submitted_at is not null then 'submitted'
            else 'draft'
        end as entry_status,
        refused_reason,

        ---------- booleans
        -- The one place the rule for what counts as an entry lives: submitted and not refused.
        submitted_at is not null and refused_at is null as is_valid,

        ---------- timestamps
        created_at::timestamp_ntz as created_at,
        submitted_at::timestamp_ntz as entered_at,
        counted_at::timestamp_ntz as counted_at,
        refused_at::timestamp_ntz as refused_at

    from source

)

select * from renamed

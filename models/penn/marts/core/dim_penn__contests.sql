with

rounds as (

    select *
    from {{ ref('stg_penn__rounds') }}

),

tournaments as (

    select
        tournament_id,
        tournament_name,
        tenant_id,
        starts_at,
        ends_at
    from {{ ref('stg_penn__tournaments') }}

),

tenants as (

    select
        tenant_id,
        tenant_name
    from {{ ref('stg_penn__tenants') }}

),

joined as (

    select
        r.round_id as contest_id,
        r.tournament_id,
        t.tournament_name,
        r.contest_name,
        r.slug,
        r.sequence,
        r.reveal_type,
        r.reveal_count,
        r.contest_status,
        r.is_active,
        r.game_type,
        'penn' as client_id,
        t.tenant_id,
        tn.tenant_name,
        cast(convert_timezone('UTC', '{{ var("local_timezone") }}', t.starts_at) as date) as contest_start_date_et,
        t.starts_at as contest_starts_at,
        convert_timezone('UTC', '{{ var("local_timezone") }}', t.starts_at)::timestamp_ntz as contest_starts_at_et,
        t.ends_at as contest_ends_at,
        convert_timezone('UTC', '{{ var("local_timezone") }}', t.ends_at)::timestamp_ntz as contest_ends_at_et,
        r.updated_at
    from rounds r
    inner join tournaments t
        on r.tournament_id = t.tournament_id
    inner join tenants tn
        on t.tenant_id = tn.tenant_id

)

select * from joined

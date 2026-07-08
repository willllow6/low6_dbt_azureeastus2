with

entries as (

    select * from {{ ref('STG_ELF_BLAST__ENTRIES') }}

),

users as (

    select * from {{ ref('INT_ELF_BLAST__USERS') }}

),

levels as (

    select * from {{ ref('STG_ELF_BLAST__LEVELS') }}

),

joined as (

    select
        e.entry_id,
        u.user_id,
        e.player_id,
        u.sso_user_id,
        u.username,
        u.email,
        u.tenant,
        u.is_user_active,
        u.is_user_deleted,
        u.has_completed_tutorial,
        e.level_id,
        e.entry_starts_at_utc,
        e.entry_ends_at_utc,
        e.entry_date_utc,
        e.entry_hour_utc,
        e.entry_starts_at_et,
        e.entry_date_et,
        e.entry_hour_et,
        e.entry_elapsed_time_seconds,
        l.level_number,
        l.level_name
    from entries as e 
        left join users as u 
            on e.player_id = u.player_id
        left join levels as l
            on e.level_id = l.level_id 

),

ranked as (

    select
        *,
        row_number() over (partition by user_id order by entry_starts_at_utc) as user_entry_number
    from joined

)

select * from ranked
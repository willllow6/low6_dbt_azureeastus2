with

entries as (

    select * from {{ source('ELF_BLAST', 'EFPGM_ENTRIES') }} 
    where year(game_start_time) != '2036'

),

final as (

    select
        id as entry_id,
        player_ref_id as sso_user_id,
        game_start_time as entry_starts_at_utc,
        game_end_time as entry_ends_at_utc,
        cast(entry_starts_at_utc as date) as entry_date_utc,
        hour(entry_starts_at_utc) as entry_hour_utc,
        convert_timezone('UTC','America/New_York',entry_starts_at_utc) as entry_starts_at_et,
        cast(entry_starts_at_et as date) as entry_date_et,
        hour(entry_starts_at_et) as entry_hour_et,
        datediff(seconds,entry_starts_at_utc,entry_ends_at_utc) as entry_elapsed_time_seconds
    from entries

)

select * from final
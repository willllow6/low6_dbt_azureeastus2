with

entries as (

    select * from {{ source('ELF_BLAST', 'GAME_PLAYED_ENTRIES') }} 
    -- where _fivetran_deleted = False 

),

final as (

    select
        id as entry_id,
        fk_player_id as player_id,
        fk_level_id as level_id,
        game_start_time as entry_starts_at_utc,
        game_end_time as entry_ends_at_utc,
        cast(entry_starts_at_utc as date) as entry_date_utc,
        hour(entry_starts_at_utc) as entry_hour_utc,
        convert_timezone('UTC','America/New_York',entry_starts_at_utc) as entry_starts_at_et,
        cast(entry_starts_at_et as date) as entry_date_et,
        hour(entry_starts_at_et) as entry_hour_et,
        case
            when datediff(seconds,entry_starts_at_utc,entry_ends_at_utc) > 550
                then null
            else datediff(seconds,entry_starts_at_utc,entry_ends_at_utc)
        end as entry_elapsed_time_seconds
    from entries

)

select * from final
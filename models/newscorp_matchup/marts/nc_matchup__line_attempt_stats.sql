with

entries as (
  
     select * from {{ ref('nc_matchup__line_attempts') }} 

),

final as (

    select
        line_attempt_date_aet,
        count(*) as line_attempts
    from entries
    group by 1

)

select * from final
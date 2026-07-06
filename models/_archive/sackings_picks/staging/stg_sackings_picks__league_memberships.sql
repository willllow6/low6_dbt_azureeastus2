with 

source as (

    select *
    from {{ source('sackings_picks', 'leaguedetail') }}

),

renamed as (

    select
        id as member_id,
        leagueheaderid as league_id,
        memberid as user_id,
        isactive as is_active,
        isadmin as is_admin,
        createdby as created_by,
        creationdate as created_at,
        cast(creationdate as date) as created_date,
        modifiedby as modified_by,
        modifieddate as modified_at,
        cast(modifieddate as date) as modified_date,
        'sackings_picks' as source
    from source
    
)

select * from renamed
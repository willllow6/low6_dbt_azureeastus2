with

source as (

    select *
    from {{ source('nc_trivia', 'league_members') }}

),

renamed as (

    select

        ----------  ids
        leagueid as league_id,
        userid as user_id,

        ---------- strings
        role as league_role,

        ---------- numerics

        ---------- booleans

        ---------- dates
        cast(joinedat as date) as league_joined_date,

        ---------- timestamps
        joinedat as league_joined_at

    from source

)

select * from renamed
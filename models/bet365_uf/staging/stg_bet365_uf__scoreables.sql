with

source as (

    select *
    from {{ source('bet365_uf', 'SCOREABLES') }}

),

teams as (

    select
        ScoreableId as scoreable_id,
        initcap(FirstName) as scoreable_name
    from source
    where TeamScoreableId is null

),

renamed as (

    select

        ----------  ids
        source.ScoreableId as scoreable_id,
        source.TenantId as tenant_id,
        source.TeamScoreableId as team_scoreable_id,
        source.NationalTeamScoreableId as national_team_scoreable_id,
        source.ExternalId as scoreable_external_id,
        source.SeasonId as season_id,
        'bet365' as client_id,

        ---------- strings
        source.ExternalReference as scoreable_external_reference,
        initcap(source.FirstName) as scoreable_first_name,
        source.LastName as scoreable_last_name,
        case
            when source.TeamScoreableId is null then initcap(source.FirstName)
            when source.FirstName is null then source.LastName
            else trim(source.FirstName || ' ' || source.LastName)
        end as scoreable_name,
        coalesce(teams.scoreable_name, initcap(source.FirstName)) as scoreable_team_name,
        source.ScoreableType as scoreable_type,
        source.Position as position,
        case
            when source.Position = 0 then 'Team'
            when source.Position = 1 then 'Defender'
            when source.Position = 2 then 'Midfielder'
            when source.Position = 3 then 'Forward'
            when source.Position = 17 then 'Ruck'
            when source.Position = 18 then 'Full Back'
            when source.Position = 19 then 'Middle Forward'
            when source.Position = 21 then 'Half'
            when source.Position = 22 then 'Hooker'
            when source.Position = 23 then 'Middle Forward'
            when source.Position = 24 then 'Edge Forward'
            else ''
        end as scoreable_position,
        source.Country as scoreable_country,
        source.ShortCode as scoreable_short_code,
        source.InjuryStatus as injury_status,

        ---------- numerics
        source.Rating as rating,
        case
            when source.Rating = 0 or source.Rating is null then 'Base'
            when source.Rating = 1 then 'Silver'
            when source.Rating = 2 then 'Gold'
            when source.Rating = 3 then 'All-Star'
        end as scoreable_rating,
        source.JerseyNumber as scoreable_shirt_number,
        source.Height as scoreable_height,
        source.AvgPointsPerGame as scoreable_average_points_per_game,

        ---------- booleans
        source.IsActive as is_active,
        source.IsInjured as is_injured,
        source.CanGenerate as can_generate,

        ---------- dates
        source.DOB as scoreable_date_of_birth,
        cast(source.CreatedAt as date) as scoreable_created_date,

        ---------- timestamps
        source.PointsBreakDown as scoreable_points_break_down,
        source.CreatedAt as scoreable_created_at

    from source
    left join teams
        on source.TeamScoreableId = teams.scoreable_id

)

select * from renamed

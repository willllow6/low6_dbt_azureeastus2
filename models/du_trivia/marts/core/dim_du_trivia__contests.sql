with

contests as (

    select *
    from {{ ref('stg_du_trivia__contests') }}

),

campaigns as (

    select *
    from {{ ref('stg_du_trivia__campaigns') }}

),

questions as (

    select *
    from {{ ref('stg_du_trivia__questions') }}

),

contest_questions as (

    select
        contest_id,
        count(*) as questions,
        sum(question_points) as max_points
    from questions
    group by contest_id

),

contest_starts as (

    -- A contest unlocks at the campaign's daily unlock time, in the campaign's
    -- timezone. The source stores only the date.
    select
        contests.contest_id,
        convert_timezone(
            coalesce(campaigns.time_zone, '{{ var("du_trivia_local_timezone") }}'),
            'UTC',
            timestamp_ntz_from_parts(
                contests.contest_date,
                to_time(coalesce(campaigns.daily_unlock_time, '00:00'), 'HH24:MI')
            )
        ) as starts_at
    from contests
    left join campaigns
        on contests.campaign_id = campaigns.campaign_id

)

select
    contests.contest_id,
    contests.campaign_id,
    contests.category_id,

    contests.client_id,
    contests.tenant_id,
    contests.tenant_name,
    contests.game_type,

    contests.contest_name,
    contests.contest_name_ar,
    contests.contest_description,
    contests.contest_description_ar,
    campaigns.campaign_status as contest_status,
    contests.contest_status_code,
    campaigns.campaign_name,

    coalesce(contest_questions.questions, 0) as questions,
    coalesce(contest_questions.max_points, 0) as max_points,
    campaigns.questions_per_day,

    campaigns.campaign_status = 'published' as is_active,

    contests.contest_date,
    campaigns.campaign_start_date,
    campaigns.campaign_end_date,

    contest_starts.starts_at,
    convert_timezone('UTC', '{{ var("du_trivia_local_timezone") }}', contest_starts.starts_at) as starts_at_gst,
    -- the source has no contest end time
    cast(null as timestamp_ntz) as ends_at,
    contests.created_at,
    contests.updated_at

from contests
left join campaigns
    on contests.campaign_id = campaigns.campaign_id
left join contest_questions
    on contests.contest_id = contest_questions.contest_id
left join contest_starts
    on contests.contest_id = contest_starts.contest_id

{{ rpt_growth_metrics_daily(
     users=ref('dim_du_trivia__users'),
     entries=ref('fct_du_trivia__entries'),
     activity=ref('fct_du_trivia__user_activity_daily'),
     start_date=var('du_trivia_start_date'),
     local_timezone=var('du_trivia_local_timezone'),
     date_suffix='gst') }}

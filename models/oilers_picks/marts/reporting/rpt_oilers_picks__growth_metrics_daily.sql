{{ rpt_growth_metrics_daily(
     users=ref('dim_oilers_picks__users'),
     entries=ref('fct_oilers_picks__entries'),
     activity=ref('fct_oilers_picks__user_activity_daily'),
     start_date=var('oilers_picks_start_date')) }}

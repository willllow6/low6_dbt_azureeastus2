{{ rpt_growth_metrics_daily(
     users=ref('dim_bet365_pickem__users'),
     entries=ref('fct_bet365_pickem__entries'),
     activity=ref('fct_bet365_pickem__user_activity_daily'),
     start_date=var('bet365_pickem_start_date')) }}

{{ rpt_contest_performance_summary(
     contests=ref('dim_bet365_pickem__contests'),
     entries=ref('fct_bet365_pickem__entries'),
     tenants_from_entries=true,
     extra_attributes=['locks_at', 'starts_at_et', 'locks_at_et']) }}

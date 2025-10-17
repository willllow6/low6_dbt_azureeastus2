-- macros/create_share.sql
{% macro create_share(share_name, accounts) %}
  {% if target.name == "prod" %}
    --create share and grant usage to low6_database
    {% set sql %}
      use role accountadmin;
      CREATE SHARE IF NOT EXISTS {{ share_name }};
      GRANT USAGE ON DATABASE {{ target.database }} TO SHARE {{ share_name }};
      
      --Add individual counts to the share
      {% for account in accounts %}
        ALTER SHARE {{ share_name }} ADD ACCOUNTS = {{ account }};
      {% endfor %}
      
    {% endset %}
    use role compute_role;
    {% set table = run_query(sql) %}
    
  {% endif %}

{% endmacro %}
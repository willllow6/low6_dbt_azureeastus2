{% macro share_view(view_schema, view_name, share_name) %}
  {% if target.name == "prod" %}

    {% set sql %}
      use role accountadmin;
      GRANT USAGE ON SCHEMA {{ target.database }}.{{ view_schema }} TO SHARE {{ share_name }};
      GRANT SELECT ON TABLE {{ target.database }}.{{ view_schema }}.{{ view_name }} TO SHARE {{ share_name }};
    {% endset %}
    use role compute_role;
    {% set table = run_query(sql) %}

  {% endif %}

{% endmacro %}
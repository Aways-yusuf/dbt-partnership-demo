{# Cross-database macros so the same dbt project runs on BigQuery (default) and AlloyDB (Postgres).
   Use these in models instead of raw safe_cast / timestamp() / parse_date. #}

{% macro cross_db_safe_cast_int(expression) %}
  {%- if target.type == 'bigquery' -%}
    safe_cast({{ expression }} as int64)
  {%- else -%}
    (case when {{ expression }} is null or trim(cast({{ expression }} as text)) = 'NULL' then null else cast({{ expression }} as integer) end)
  {%- endif -%}
{% endmacro %}

{% macro cross_db_safe_cast_int_coalesce(expression, default_val=0) %}
  {%- if target.type == 'bigquery' -%}
    coalesce(safe_cast({{ expression }} as int64), {{ default_val }})
  {%- else -%}
    (case when {{ expression }} is null or trim(cast({{ expression }} as text)) = 'NULL' or trim(cast({{ expression }} as text)) = '' then {{ default_val }} else cast({{ expression }} as integer) end)
  {%- endif -%}
{% endmacro %}

{% macro cross_db_cast_timestamp(expression) %}
  {# BigQuery: source may be datetime string; Postgres/AlloyDB: often already timestamp. #}
  {%- if target.type == 'bigquery' -%}
    safe_cast(substr(cast({{ expression }} as string), 1, 26) as timestamp)
  {%- else -%}
    ({{ expression }})::timestamp
  {%- endif -%}
{% endmacro %}

{% macro cross_db_timestamp_max() %}
  {%- if target.type == 'bigquery' -%}
    timestamp('9999-12-31 23:59:59.999999')
  {%- else -%}
    '9999-12-31 23:59:59.999999'::timestamp
  {%- endif -%}
{% endmacro %}

{% macro cross_db_timestamp_min() %}
  {%- if target.type == 'bigquery' -%}
    timestamp('1900-01-01')
  {%- else -%}
    '1900-01-01'::timestamp
  {%- endif -%}
{% endmacro %}

{% macro cross_db_parse_date(format, expression) %}
  {%- if target.type == 'bigquery' -%}
    safe.parse_date({{ format }}, substr(cast({{ expression }} as string), 1, 10))
  {%- else -%}
    ({{ expression }})::date
  {%- endif -%}
{% endmacro %}

{% macro cross_db_null_int() %}
  {%- if target.type == 'bigquery' -%}
    cast(null as int64)
  {%- else -%}
    cast(null as integer)
  {%- endif -%}
{% endmacro %}

{% macro cross_db_null_string() %}
  {%- if target.type == 'bigquery' -%}
    cast(null as string)
  {%- else -%}
    cast(null as text)
  {%- endif -%}
{% endmacro %}

{# Postgres: current_timestamp (no parens). BigQuery: current_timestamp() #}
{% macro cross_db_current_timestamp() %}
  {%- if target.type == 'bigquery' -%}
    current_timestamp()
  {%- else -%}
    current_timestamp
  {%- endif -%}
{% endmacro %}

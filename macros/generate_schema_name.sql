-- Use custom schema as the BigQuery dataset name (no prefix/suffix).
-- So +schema: dbt_staging → dataset "dbt_staging", +schema: dbt_target → dataset "dbt_target".
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

{# Use +schema from dbt_project (dbt_staging for staging/intermediate, dbt_target for dimensions/facts).
   Only fall back to target.schema when no custom schema is set. #}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is not none and custom_schema_name | trim != '' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}
    {%- endif -%}

{%- endmacro %}
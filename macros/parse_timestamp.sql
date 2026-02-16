{% macro parse_timestamp(column_name) %}
    coalesce(
        safe.parse_timestamp('%Y-%m-%d %H:%M:%E*S%Ez', cast({{ column_name }} as string)),
        safe.parse_timestamp('%Y-%m-%d %H:%M:%E*S', cast({{ column_name }} as string)),
        safe_cast({{ column_name }} as timestamp)
    )
{% endmacro %}


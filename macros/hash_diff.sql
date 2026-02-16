{% macro hash_diff(columns) %}
    to_hex(md5(concat(
        {%- for col in columns -%}
            coalesce(cast({{ col }} as string), '')
            {%- if not loop.last -%} || '||' || {%- endif -%}
        {%- endfor -%}
    )))
{% endmacro %}


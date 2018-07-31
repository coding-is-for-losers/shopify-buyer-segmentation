{% macro table_exists(schema, client, table, column) -%}

    {%- call statement('table_exists', fetch_result=True) %}

        select
            count({{ column }}) as value
        from `{{ target.project }}.{{ schema }}{{ client }}.{{ table }}`
        limit 1

    {%- endcall -%}

    {%- set value_list = load_result('table_exists') -%}

    {%- if value_list and value_list['data'] -%}
        {%- set values = value_list['data'] | map(attribute=0) | list %}
        {{ return(values) }}
    {%- else -%}
        {{ return([]) }}
    {%- endif -%}

{%- endmacro %}
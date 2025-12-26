{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {# No custom schema? Use the default from profiles.yml #}
        {{ default_schema }}
    {%- else -%}
        {# Custom schema provided? Use that combined with the environment suffix #}
        {# Logic: If target is 'prod', just use the custom name. If 'dev', append _dev #}
        
        {%- if target.name == 'prod' -%}
            {{ custom_schema_name | upper }}_PROD
        {%- else -%}
            {{ custom_schema_name | upper }}_DEV
        {%- endif -%}

    {%- endif -%}

{%- endmacro %}
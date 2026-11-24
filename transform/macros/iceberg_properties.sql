{#- Iceberg table properties for the Trino target; other adapters ignore them. -#}
{% macro iceberg_properties(partitioning=none) -%}
    {%- set props = {"format": "'PARQUET'", "format_version": "2"} -%}
    {%- if partitioning -%}
        {%- do props.update({"partitioning": "ARRAY[" ~ (partitioning | map("tojson") | join(", ") | replace('"', "'")) ~ "]"}) -%}
    {%- endif -%}
    {{ return(props if target.type == "trino" else {}) }}
{%- endmacro %}

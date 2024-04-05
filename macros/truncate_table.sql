{% macro truncate_tbl(schm_nm,tbl_name) %}
truncate table if exists {{schm_nm}}.{{tbl_name }};
{% endmacro %}
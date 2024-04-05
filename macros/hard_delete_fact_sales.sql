{% macro hard_delete_fact_sales(schm_nm,tbl_nm) %}
delete from {{schm_nm}}.{{tbl_nm}} where salesorderid =43797;
{% endmacro %}
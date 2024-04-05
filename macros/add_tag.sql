{% macro add_tag(schm_nm,tbl_name) %}
  alter table {{schm_nm}}.{{tbl_name }}
   set tag general_cost = 'public';
{% endmacro %}
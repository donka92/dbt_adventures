{% macro apply_rap(schema_nm,view_name,rap_flag, rap_policy,rap_column) %}
{% if rap_flag == True %}
    ALTER VIEW IF EXISTS {{schema_nm}}.{{view_name}} ADD ROW ACCESS POLICY {{rap_policy}} ON (rap_column)
{% endif %}
{% endmacro %}
select CUSTOMER_KEY, count(*)
from {{ref('dim_customers')}}
group by CUSTOMER_KEY
having count(*)>1
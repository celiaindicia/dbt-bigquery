select last_name, count(1) as number_of_customers
    from dbt-tutorial.jaffle_shop.customers
group by last_name 

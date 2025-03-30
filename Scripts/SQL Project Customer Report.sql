USE DataWarehouseAnalytics;

-- Building cutomer report
/* 
Purpose: This report consolidates key customer metrics and behaviors.

Highlights:

1. Gather essential field such as names, ages and transaction details
2. Segments custo ers into categories(VIP, Regular, New) and age group
3. Aggregates customer-level metrics
   - Total orders
   - Total sales
   - Total quantity purchased
   - Total products
   - Lifespan (in months)
4. Calculates valuable KPIs:
   - Recency(months since last order)
   - Average order value
   - Average monthly spend
 */
 
CREATE VIEW gold.report_cutomers AS 
 -- Base query: Retrive core columns from tables
 WITH base_query AS(
 SELECT 
 s.order_number, s.product_key, s.order_date,
 s.sales_amount, s.quantity, c.customer_key,
 c.customer_number, CONCAT(c.first_name,' ', c.last_name) AS customer_name,
 TIMESTAMPDIFF(YEAR,c.birthdate,CURDATE())AS age
 FROM gold.fact_sales s
 LEFT JOIN gold.dim_customers c
 ON s.customer_key = c.customer_key
 WHERE order_date IS NOT NULL
 ),
 customer_aggregation AS(
 SELECT 
 customer_key, customer_number, customer_name, age,
 COUNT(DISTINCT order_number) AS total_orders,
 SUM(sales_amount) As total_sales,
 SUM(quantity) AS total_quantity,
 COUNT(DISTINCT product_key) AS total_products,
 MAX(order_date) AS last_order_date,
 TIMESTAMPDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
 FROM base_query WHERE customer_key IS NOT NULL
 GROUP BY customer_key, customer_number, customer_name, age
 )
 SELECT 
 customer_key, customer_number, customer_name, age,
 CASE
     WHEN age < 20 THEN 'under 20'
     WHEN age BETWEEN 20 AND 29 THEN '20-29'
     WHEN age BETWEEN 30 AND 39 THEN '30-39'
     WHEN age BETWEEN 40 AND 49 THEN '40-49'
     ELSE '50 and Above'
END age_group,
lifespan,
CASE 
      WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
	  WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
	  ELSE 'New'
END customer_segment,
total_orders, total_sales,
total_quantity, total_products, last_order_date,
TIMESTAMPDIFF(MONTH, last_order_date, CURDATE()) AS recency,
CASE
    WHEN total_orders = 0 THEN 0
    ELSE total_sales/total_orders
END avg_order_value,
CASE
    WHEN lifespan = 0 THEN total_sales
    ELSE total_sales/lifespan
END avg_monthly_spend
FROM customer_aggregation;

SELECT * FROM gold.report_cutomers
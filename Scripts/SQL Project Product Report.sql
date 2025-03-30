USE DataWarehouseAnalytics;

-- Building product report
/* 
Purpose: This report consolidates key product metrics and behaviors.

Highlights:
1. Gather essential fields such as product name, category, subcategory, and cost
2. Segments products by revenue to identify High-peformer, Mid-Range, or Low-pefromer
3. Aggregate product level metrics:
   - total orders
   - total sales
   - total quantity sold
   - total customers (unique)
   - lifespan(in months)
4. Calculates valuable KPIs:
   - recency(months since last sale)
   - average order revenue
   - average monthly revenue
*/

CREATE VIEW gold.report_products AS 
WITH base_query AS(
SELECT 
s.order_number, s.customer_key, s.order_date, s.sales_amount, s.quantity,
p.product_key, p.product_name, p.category, p.subcategory, p.cost
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON s.product_key = p.product_key
WHERE order_date IS NOT NULL
),
product_aggregation AS(
SELECT 
order_number, order_date, sales_amount, product_key, 
product_name, category, subcategory, cost,
COUNT(DISTINCT order_number) AS total_orders,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity_sold,
COUNT(DISTINCT customer_key) AS total_customers,
MAX(order_date) AS last_sale_date,
TIMESTAMPDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan,
ROUND(AVG(sales_amount/NULLIF(quantity,0)),2) AS avg_selling_price
FROM base_query
GROUP BY product_key, 
product_name, category, subcategory, cost
)
SELECT 
product_key, product_name, category, subcategory, cost,
last_sale_date,
TIMESTAMPDIFF(MONTH,last_sale_date,CURDATE()) AS recency_in_month,
CASE
    WHEN total_sales > 50000 THEN 'High-performer'
    WHEN total_sales >=10000 THEN 'Mid-Range'
    ELSE 'Lower-performer'
END product_segment,

CASE
    WHEN total_orders = 0 THEN 0
    ELSE ROUND(total_sales / total_orders,2)
END avg_order_revenue,

CASE
    WHEN lifespan = 0 THEN total_sales
    ELSE ROUND(total_sales/lifespan,2)
END avg_monthly_revenue
FROM product_aggregation;

SELECT * FROM gold.report_products;
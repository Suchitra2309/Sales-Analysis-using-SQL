-- Create the 'DataWarehouseAnalytics' database
CREATE DATABASE DataWarehouseAnalytics;
USE DataWarehouseAnalytics;

-- Create Schemas
CREATE SCHEMA gold;

CREATE TABLE gold.dim_customers(
	customer_key int,
	customer_id int,
	customer_number nvarchar(50),
	first_name nvarchar(50),
	last_name nvarchar(50),
	country nvarchar(50),
	marital_status nvarchar(50),
	gender nvarchar(50),
	birthdate date,
	create_date date
);


CREATE TABLE gold.dim_products(
	product_key int ,
	product_id int ,
	product_number nvarchar(50) ,
	product_name nvarchar(50) ,
	category_id nvarchar(50) ,
	category nvarchar(50) ,
	subcategory nvarchar(50) ,
	maintenance nvarchar(50) ,
	cost int,
	product_line nvarchar(50),
	start_date date 
);


CREATE TABLE gold.fact_sales(
	order_number nvarchar(50),
	product_key int,
	customer_key int,
	order_date date,
	shipping_date date,
	due_date date,
	sales_amount int,
	quantity tinyint,
	price int 
);


SELECT * FROM gold.dim_customers;

SELECT * FROM gold.dim_products;

SELECT * FROM gold.fact_sales;

-- calculate total sale for each year

SELECT
YEAR(order_date) AS order_year,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_customer,
SUM(quantity) AS total_quantity
FROM gold.fact_sales
GROUP BY YEAR(order_date)
ORDER BY YEAR(order_date);

-- calculate total sales for each month

SELECT
MONTH(order_date) AS order_month,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_customer,
SUM(quantity) AS total_quantity
FROM gold.fact_sales
GROUP BY MONTH(order_date)
ORDER BY MONTH(order_date);

-- calculate total sales for each year and month

SELECT
YEAR(order_date) AS order_year,
MONTH(order_date) AS order_month,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_customer,
SUM(quantity) AS total_quantity
FROM gold.fact_sales
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date);

-- calculate the total sales per month and the running total of sales and moving average of price over time 
SELECT order_year, order_month,
total_sales,
SUM(total_sales) OVER(PARTITION BY order_year ORDER BY order_year,order_month) AS running_total_sales,
ROUND(AVG(average_price) OVER(PARTITION BY order_year ORDER BY order_year,order_month) )AS moving_average_price
FROM(
SELECT
YEAR(order_date) AS order_year,
MONTH(order_date) AS order_month,
SUM(sales_amount) AS total_sales,
AVG(price) AS average_price,
COUNT(DISTINCT customer_key) AS total_customer,
SUM(quantity) AS total_quantity
FROM gold.fact_sales
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date)
) t;

/* Analyze the yearly performance of products by comparing each product's sales to both its 
average sales performance and the previous year's sales*/

WITH yearly_product_sales AS(
SELECT YEAR(s.order_date) AS order_year,
p.product_name,
SUM(s.sales_amount) AS current_sales
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON s.product_key = p.product_key
GROUP BY YEAR(s.order_date), p.product_name
)
SELECT order_year, product_name,
current_sales,
ROUND(AVG(current_sales) OVER(PARTITION BY product_name)) AS average_sales,
current_sales - ROUND(AVG(current_sales) OVER(PARTITION BY product_name)) AS diff_avg,
CASE
WHEN current_sales - ROUND(AVG(current_sales) OVER(PARTITION BY product_name)) < 0 THEN 'Below Avg'
WHEN current_sales - ROUND(AVG(current_sales) OVER(PARTITION BY product_name)) > 0  THEN 'Above Avg'
ELSE 'Avg'
END AS change_avg,
LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS py_sales,
current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS diff_py_sales,
CASE
WHEN current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
WHEN current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
ELSE 'No change'
END AS py_change
FROM yearly_product_sales;

-- which categories contribute the most to overall sales?

WITH category_sales AS(
SELECT category,
SUM(sales_amount) AS total_sales
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON s.product_key = p.product_key
GROUP BY category
)
SELECT category, total_sales,
SUM(total_sales) OVER() AS overall_sales,
CONCAT(ROUND((total_sales / SUM(total_sales) OVER())*100,2),'%') AS percentage_of_total
FROM category_sales
ORDER BY total_sales DESC;

-- segment products into cost range and count how many products fall into each segment

WITH product_segment AS(
SELECT product_key, product_name, cost,
CASE
    WHEN cost < 100 THEN 'Below 100'
    WHEN cost BETWEEN 100 AND 500 THEN '100-500'
    WHEN cost BETWEEN 500 AND 1000 then '500-1000'
    ELSE 'Above 1000'
END cost_range
FROM gold.dim_products
)

SELECT cost_range,
COUNT(product_key) AS total_products
FROM product_segment
GROUP BY cost_range
ORDER BY total_products DESC;

/* Group customers into three segments based on their spending behavior.
   VIP: At least 12 months of history and spending more than 5000
   Regular: At least 12 months of history but spending 5000 or less
   NEW: Lifespan less than 12 months
And find the total number of customers by each group. */

WITH customer_spending AS(
SELECT c.customer_key,
SUM(s.sales_amount) AS total_spending,
MIN(s.order_date) AS first_order,
MAX(s.order_date) As last_order,
TIMESTAMPDIFF(MONTH,MIN(s.order_date),MAX(s.order_date)) AS lifespan
FROM gold.dim_customers c
LEFT JOIN gold.fact_sales s
ON c.customer_key = s.customer_key
GROUP BY c.customer_key
)
SELECT customer_segment,
COUNT(customer_key) AS total_customers
FROM(
     SELECT customer_key, total_spending, lifespan,
     CASE
		WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
        WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
        ELSE 'New'
     END customer_segment
     FROM customer_spending) t 
     GROUP BY customer_segment
     ORDER BY total_customers DESC;



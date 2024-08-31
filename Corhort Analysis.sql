-- COHORT ANALYSIS
WITH table_month AS (
    SELECT customer_id, order_id, transaction_date,
            MIN(MONTH(transaction_date)) OVER(PARTITION BY customer_id ORDER BY transaction_date) AS [month],
            MONTH(transaction_date) -  MIN(MONTH(transaction_date)) OVER(PARTITION BY customer_id ORDER BY transaction_date) AS month_n
    FROM payment_history_17 as his_17 
    JOIN product 
        ON product.product_number = his_17.product_id
    WHERE message_id = 1 AND sub_category = 'electricity'
)
 , table_retained  AS(
    SELECT [month], month_n, COUNT(DISTINCT customer_id) AS reatained_customers
    FROM table_month
    GROUP BY [month], month_n
)
, table_retaintion AS(
SELECT *,
        MAX(reatained_customers) OVER(PARTITION BY [month] ORDER BY month_n) AS original_customers,
      CAST(reatained_customers AS DECIMAL)/MAX(reatained_customers) OVER(PARTITION BY [month] ORDER BY month_n)  AS pct
FROM table_retained
)
SELECT  [month], original_customers, "0","1","2","3","4","5","6","7","8","9","10","11"
FROM(
    SELECT [month], month_n, original_customers, CAST(pct AS DECIMAL(10,2)) AS pct
    FROM table_retaintion
) AS source
PIVOT(
    SUM(pct)
    FOR month_n IN ("0","1","2","3","4","5","6","7","8","9","10","11")
) as pivot_table
ORDER BY [month]
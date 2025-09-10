import duckdb

# Percorsi dei file Parquet
orders_path = 'archive/parquet/orders.parquet'
order_items_path = 'archive/parquet/order_items.parquet'
products_path = 'archive/parquet/products.parquet'
product_reviews_path = 'archive/parquet/product_reviews.parquet'
customers_path = 'archive/parquet/customers.parquet'

# Query DuckDB che legge direttamente dai file Parquet
query = f"""
    WITH product_sales AS (
        SELECT
            o.shipping_country AS country,
            oi.product_id,
            p.product_name,
            SUM(o.total_amount) AS total_sales,
            SUM(oi.quantity) AS total_quantity
        FROM read_parquet('{orders_path}') o
        JOIN read_parquet('{order_items_path}') oi ON o.order_id = oi.order_id
        JOIN read_parquet('{products_path}') p ON oi.product_id = p.product_id
        GROUP BY country, oi.product_id, p.product_name
    ),
    product_ratings AS (
        SELECT
            product_id,
            AVG(rating) AS avg_rating
        FROM read_parquet('{product_reviews_path}')
        GROUP BY product_id
    ),
    product_customers AS (
        SELECT
            oi.product_id,
            COUNT(DISTINCT o.customer_id) AS customer_count,
            100.0 * SUM(CASE WHEN c.gender = 'Male' THEN 1 ELSE 0 END) / COUNT(DISTINCT o.customer_id) AS male_percent,
            100.0 * SUM(CASE WHEN c.gender = 'Female' THEN 1 ELSE 0 END) / COUNT(DISTINCT o.customer_id) AS female_percent
        FROM read_parquet('{orders_path}') o
        JOIN read_parquet('{order_items_path}') oi ON o.order_id = oi.order_id
        JOIN read_parquet('{customers_path}') c ON o.customer_id = c.customer_id
        GROUP BY oi.product_id
    ),
    ranked_sales AS (
        SELECT ps.*, pr.avg_rating, pc.customer_count, pc.male_percent, pc.female_percent,
            RANK() OVER (PARTITION BY ps.country ORDER BY ps.total_sales DESC) AS rank
        FROM product_sales ps
        LEFT JOIN product_ratings pr ON ps.product_id = pr.product_id
        LEFT JOIN product_customers pc ON ps.product_id = pc.product_id
    ),
    top_countries AS (
        SELECT country, MAX(total_sales) AS max_sales
        FROM product_sales
        GROUP BY country
        ORDER BY max_sales DESC
        LIMIT 10
    )
    SELECT rs.country, rs.product_name, rs.total_sales, rs.total_quantity, rs.avg_rating,
        rs.customer_count, rs.male_percent, rs.female_percent
    FROM ranked_sales rs
    JOIN top_countries tc ON rs.country = tc.country
    WHERE rs.rank = 1
    ORDER BY rs.total_sales DESC;
"""

# Esecuzione della query
result = duckdb.query(query).to_df()

# Visualizzazione dei risultati
print(result)

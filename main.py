import duckdb
import os

def setup_ducklake():
    """Initialize DuckLake extension and create/attach database"""
    conn = duckdb.connect()
    
    # Install DuckLake extension
    conn.execute("INSTALL ducklake;")
    
    # Create or attach to DuckLake database
    ducklake_path = 'ecommerce_analytics.ducklake'
    data_path = 'lakehouse/'
    
    conn.execute(f"ATTACH 'ducklake:{ducklake_path}' AS ecommerce_db (DATA_PATH '{data_path}');")
    conn.execute("USE ecommerce_db;")
    
    return conn

def create_tables_if_not_exist(conn):
    """Create DuckLake tables from existing Parquet files if they don't exist"""
    
    # Check if tables already exist
    tables_exist = conn.execute("""
        SELECT COUNT(*) as count FROM information_schema.tables 
        WHERE table_name IN ('orders', 'order_items', 'products', 'product_reviews', 'customers')
    """).fetchone()[0]
    
    if tables_exist < 5:
        print("Creating DuckLake tables from Parquet files...")
        
        # Create tables by reading from Parquet files
        parquet_files = {
            'customers': 'archive/parquet/customers.parquet',
            'products': 'archive/parquet/products.parquet', 
            'orders': 'archive/parquet/orders.parquet',
            'order_items': 'archive/parquet/order_items.parquet',
            'product_reviews': 'archive/parquet/product_reviews.parquet'
        }
        
        for table_name, parquet_path in parquet_files.items():
            if os.path.exists(parquet_path):
                print(f"Creating table {table_name}...")
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} AS 
                    SELECT * FROM read_parquet('{parquet_path}')
                """)
            else:
                print(f"Warning: Parquet file {parquet_path} not found")
    
    return conn

def run_analytics_query(conn):
    """Run the main analytics query using DuckLake tables"""
    
    query = """
        WITH product_sales AS (
            SELECT
                o.shipping_country AS country,
                oi.product_id,
                p.product_name,
                SUM(o.total_amount) AS total_sales,
                SUM(oi.quantity) AS total_quantity
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            JOIN products p ON oi.product_id = p.product_id
            GROUP BY country, oi.product_id, p.product_name
        ),
        product_ratings AS (
            SELECT
                product_id,
                AVG(rating) AS avg_rating
            FROM product_reviews
            GROUP BY product_id
        ),
        product_customers AS (
            SELECT
                oi.product_id,
                COUNT(DISTINCT o.customer_id) AS customer_count,
                100.0 * SUM(CASE WHEN c.gender = 'Male' THEN 1 ELSE 0 END) / COUNT(DISTINCT o.customer_id) AS male_percent,
                100.0 * SUM(CASE WHEN c.gender = 'Female' THEN 1 ELSE 0 END) / COUNT(DISTINCT o.customer_id) AS female_percent
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            JOIN customers c ON o.customer_id = c.customer_id
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
    
    return conn.execute(query).df()

def main():
    """Main execution function"""
    try:
        # Setup DuckLake
        conn = setup_ducklake()
        print("DuckLake database initialized successfully")
        
        # Create tables if they don't exist
        conn = create_tables_if_not_exist(conn)
        
        # Run analytics query
        print("Running analytics query...")
        result = run_analytics_query(conn)
        
        # Display results
        print("\nTop-selling products by country:")
        print(result)
        
        # Close connection
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()

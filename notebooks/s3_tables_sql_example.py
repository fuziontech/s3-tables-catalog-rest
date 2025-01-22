import os
import json
import requests
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def setup_dependencies():
    """Download required JARs if they don't exist"""
    if not os.path.exists('jars'):
        os.makedirs('jars')

    jars = [
        'https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar',
        'https://repo1.maven.org/maven2/software/amazon/s3tables/s3-tables-catalog-for-iceberg-runtime/0.1.3/s3-tables-catalog-for-iceberg-runtime-0.1.3.jar',
        'https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.26/bundle-2.29.26.jar',
        'https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.29.26/url-connection-client-2.29.26.jar'
    ]

    for jar in jars:
        jar_name = os.path.basename(jar)
        if not os.path.exists(f'jars/{jar_name}'):
            print(f'Downloading {jar_name}...')
            os.system(f'wget -P jars {jar}')

def init_spark():
    """Initialize Spark session with required configurations"""
    # Get AWS credentials from environment
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_REGION', 'us-east-1')
    warehouse_location = os.getenv('AWS_S3_WAREHOUSE')

    if not all([aws_access_key, aws_secret_key, warehouse_location]):
        raise ValueError("AWS credentials and warehouse location must be set in .env file")

    jars = [
        'jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar',
        'jars/s3-tables-catalog-for-iceberg-runtime-0.1.3.jar',
        'jars/bundle-2.29.26.jar',
        'jars/url-connection-client-2.29.26.jar'
    ]

    return SparkSession.builder \
        .appName("S3TablesCatalogSQLExample") \
        .config("spark.jars", ",".join(jars)) \
        .config("spark.sql.catalog.default", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.default.catalog-impl", "org.apache.iceberg.rest.RESTCatalog") \
        .config("spark.sql.catalog.default.uri", "http://localhost:8080") \
        .config("spark.sql.catalog.default.warehouse", warehouse_location) \
        .config("spark.sql.defaultCatalog", "default") \
        .getOrCreate()

def test_catalog_connection():
    """Test connection to S3 Tables Catalog REST service"""
    try:
        response = requests.get('http://localhost:8080/v1/config')
        print("Catalog Configuration:")
        print(json.dumps(response.json(), indent=2))
        return True
    except Exception as e:
        print(f"Failed to connect to catalog service: {e}")
        return False

def create_namespace(spark):
    """Create a test namespace using SQL"""
    print("\nCreating namespace...")
    # Create namespace with single level name
    spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS sales
    """)

    # Show namespaces to verify creation
    print("\nListing available namespaces:")
    spark.sql("SHOW NAMESPACES").show()

def create_table(spark):
    """Create table using SQL"""
    print("\nCreating table...")
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS sales.orders (
        sale_id INT,
        product STRING,
        quantity INT,
        price DOUBLE,
        sale_date DATE
    ) USING iceberg
    PARTITIONED BY (days(sale_date))
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.metadata.compression-codec' = 'gzip'
    )
    """
    spark.sql(create_table_sql)

def populate_table(spark):
    """Populate table with sample data"""
    print("\nChecking if table exists and has data...")
    count_df = spark.sql("SELECT COUNT(*) as count FROM sales.orders").collect()
    row_count = count_df[0]['count']
    if row_count > 0:
        print(f"Table already exists with {row_count} rows. Skipping data insertion.")
        return

    print("\nInserting sample data...")
    insert_data_sql = """
    INSERT INTO sales.orders (sale_id, product, quantity, price, sale_date) VALUES
        (1, 'Laptop', 1, 999.99, DATE '2024-01-01'),
        (2, 'Mouse', 2, 24.99, DATE '2024-01-01'),
        (3, 'Keyboard', 1, 89.99, DATE '2024-01-02'),
        (4, 'Monitor', 2, 299.99, DATE '2024-01-02'),
        (5, 'Headphones', 3, 79.99, DATE '2024-01-03')
    """
    spark.sql(insert_data_sql)

def query_data(spark):
    """Query the data using SQL"""
    print("\nReading all data from table:")
    spark.sql("SELECT * FROM sales.orders").show()

    print("\nSales summary by product:")
    summary_sql = """
    SELECT
        product,
        SUM(quantity) as total_quantity,
        SUM(price) as total_revenue
    FROM sales.orders
    GROUP BY product
    """
    spark.sql(summary_sql).show()

def cleanup_resources(spark):
    """Clean up the created resources using SQL"""
    try:
        print("\nDropping table...")
        spark.sql("DROP TABLE IF EXISTS sales.orders")

        print("\nShowing tables...")
        spark.sql("SHOW TABLES IN sales").show()

        print("\nDropping namespace...")
        spark.sql("DROP NAMESPACE IF EXISTS sales")

        # Verify cleanup
        print("\nListing remaining namespaces:")
        spark.sql("SHOW NAMESPACES").show()
    except Exception as e:
        print(f"Error during cleanup: {e}")
        # Continue with cleanup even if there are errors

def main():
    print("Setting up dependencies...")
    setup_dependencies()

    print("\nInitializing Spark...")
    spark = init_spark()

    print("\nTesting catalog connection...")
    if not test_catalog_connection():
        print("Failed to connect to catalog service. Exiting.")
        return

    try:
        create_namespace(spark)
        create_table(spark)
        populate_table(spark)
        query_data(spark)
    finally:
        cleanup_resources(spark)
        print("\nStopping Spark session...")
        spark.stop()

if __name__ == "__main__":
    main()
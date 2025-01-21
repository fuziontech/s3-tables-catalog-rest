import os
import json
import requests
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def setup_dependencies():
    """Download required JARs if they don't exist"""
    if not os.path.exists('jars'):
        os.makedirs('jars')

    jars = [
        'https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar',
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

    return SparkSession.builder \
        .appName("S3TablesCatalogExample") \
        .config("spark.jars", "jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar,jars/bundle-2.29.26.jar,jars/url-connection-client-2.29.26.jar") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.demo.catalog-impl", "org.apache.iceberg.rest.RESTCatalog") \
        .config("spark.sql.catalog.demo.uri", "http://localhost:8080") \
        .config("spark.sql.catalog.demo.warehouse", warehouse_location) \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.region", aws_region) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
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

def create_namespace():
    """Create a test namespace"""
    namespace_data = {
        "namespace": "demo.test",
        "properties": {
            "comment": "Demo namespace for testing",
            "owner": "data_team"
        }
    }

    response = requests.post('http://localhost:8080/v1/namespaces', json=namespace_data)
    print(f"Create namespace status: {response.status_code}")

    # List namespaces to verify creation
    response = requests.get('http://localhost:8080/v1/namespaces')
    print("\nAvailable namespaces:")
    print(json.dumps(response.json(), indent=2))

def create_test_table():
    """Create a test table"""
    table_data = {
        "namespace": "demo.test",
        "name": "sales",
        "schema": {
            "type": "struct",
            "fields": [
                {"id": 1, "name": "sale_id", "type": "long", "required": True},
                {"id": 2, "name": "product", "type": "string"},
                {"id": 3, "name": "quantity", "type": "integer"},
                {"id": 4, "name": "price", "type": "double"},
                {"id": 5, "name": "sale_date", "type": "date"}
            ]
        },
        "properties": {
            "write.format.default": "parquet",
            "write.metadata.compression-codec": "gzip"
        }
    }

    response = requests.post('http://localhost:8080/v1/tables', json=table_data)
    print("Table creation response:")
    print(json.dumps(response.json(), indent=2))

def create_sample_data(spark):
    """Create and write sample data"""
    # Define schema
    schema = StructType([
        StructField("sale_id", LongType(), False),
        StructField("product", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("sale_date", DateType(), True)
    ])

    # Sample data
    data = [
        (1, "Laptop", 1, 999.99, date(2024, 1, 1)),
        (2, "Mouse", 2, 24.99, date(2024, 1, 1)),
        (3, "Keyboard", 1, 89.99, date(2024, 1, 2)),
        (4, "Monitor", 2, 299.99, date(2024, 1, 2)),
        (5, "Headphones", 3, 79.99, date(2024, 1, 3))
    ]

    # Create DataFrame
    df = spark.createDataFrame(data, schema)
    print("Sample DataFrame:")
    df.show()

    # Write data to the table
    df.writeTo("demo.demo.test.sales").using("iceberg").createOrReplace()
    print("Data written successfully!")

def read_and_analyze_data(spark):
    """Read and analyze the data"""
    result = spark.table("demo.demo.test.sales")
    print("Reading data from table:")
    result.show()

    print("\nSales summary by product:")
    result.groupBy("product") \
        .agg({"quantity": "sum", "price": "sum"}) \
        .withColumnRenamed("sum(quantity)", "total_quantity") \
        .withColumnRenamed("sum(price)", "total_revenue") \
        .show()

def cleanup_resources():
    """Clean up the created resources"""
    response = requests.delete('http://localhost:8080/v1/tables/demo.test/sales')
    print(f"Drop table status: {response.status_code}")

    response = requests.get('http://localhost:8080/v1/tables?namespace=demo.test')
    print("\nRemaining tables:")
    print(json.dumps(response.json(), indent=2))

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
        print("\nCreating namespace...")
        create_namespace()

        print("\nCreating test table...")
        create_test_table()

        print("\nCreating and writing sample data...")
        create_sample_data(spark)

        print("\nReading and analyzing data...")
        read_and_analyze_data(spark)

    finally:
        print("\nCleaning up resources...")
        cleanup_resources()

        print("\nStopping Spark session...")
        spark.stop()

if __name__ == "__main__":
    main()
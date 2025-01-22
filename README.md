# S3 Tables Catalog REST Service

This is a REST service wrapper for the S3 Tables Catalog library. It provides a RESTful API interface to interact with S3 Tables Catalog operations.

## Prerequisites

- Java 17 or later
- Gradle
- Python 3.10+ (for running PySpark examples)

## Building

```bash
./gradlew build
```

## Running Tests

The tests require AWS credentials to be set as environment variables. You can set these up using:

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-west-2
```

Then run the tests with:
```bash
./gradlew test
```

The tests use a local endpoint (http://localhost:4566) for S3 Tables operations, which assumes you have LocalStack or a similar service running locally.

To see the test results, open:
```
build/reports/tests/test/index.html
```

## Running the Service

```bash
./gradlew bootRun
```

The service will start on port 8080 by default.

## API Specification

### Catalog Configuration
- `GET /v1/config` - Get catalog configuration
  ```json
  {
    "catalog-version": "1.5.0",
    "catalog-impl": "software.amazon.s3tables.iceberg.S3TablesCatalog",
    "warehouse": "<warehouse-name>"
  }
  ```

### Namespace Operations
- `POST /v1/namespaces` - Create a new namespace
  ```json
  {
    "namespace": "example.test",
    "properties": {
      "comment": "Test namespace"
    }
  }
  ```

- `GET /v1/namespaces` - List all namespaces
- `GET /v1/namespaces/{namespace}` - Get namespace details
- `DELETE /v1/namespaces/{namespace}` - Delete a namespace

### Table Operations
- `POST /v1/tables` - Create a new table
  ```json
  {
    "namespace": "example.test",
    "name": "sample_table",
    "schema": {
      "type": "struct",
      "fields": [
        {"id": 1, "name": "id", "type": "long", "required": true},
        {"id": 2, "name": "data", "type": "string"}
      ]
    },
    "properties": {
      "write.format.default": "parquet"
    }
  }
  ```

- `GET /v1/tables` - List tables (query params: `namespace`)
- `GET /v1/tables/{namespace}/{table}` - Get table details
- `DELETE /v1/tables/{namespace}/{table}` - Drop a table

## Running the PySpark Example

The repository includes a PySpark example script that demonstrates how to use the REST catalog service with Spark SQL.

### Setup

1. Create a `.env` file with your AWS credentials and S3 warehouse location:
```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
AWS_S3_WAREHOUSE=arn for s3 tables
```

2. Install Python dependencies:
```bash
pip install pyspark python-dotenv requests
```

3. Run the example script:
```bash
python notebooks/s3_tables_sql_example.py
```

The script will:
- Download required JAR dependencies
- Initialize a Spark session with the REST catalog configuration
- Create a test namespace and table
- Insert sample data
- Run example queries
- (Optionally) Clean up created resources

### Example Operations

The script demonstrates:
- Creating and managing namespaces using Spark SQL
- Creating tables with partitioning
- Inserting sample data
- Running analytical queries
- Table and namespace cleanup

## Configuration

The service can be configured through `src/main/resources/application.properties`. Key properties include:

- `server.port` - HTTP port (default: 8080)
- `spring.application.name` - Application name
- `management.endpoints.web.exposure.include` - Exposed actuator endpoints
- `aws.access.key` - AWS access key
- `aws.secret.key` - AWS secret key
- `aws.region` - AWS region (default: us-east-1)
- `aws.s3.warehouse` - S3 warehouse location
- `s3tables.impl` - S3 implementation class (default: org.apache.hadoop.fs.s3a.S3AFileSystem)
# S3 Tables Catalog REST Service

This is a REST service wrapper for the S3 Tables Catalog library. It provides a RESTful API interface to interact with S3 Tables Catalog operations.

## Prerequisites

- Java 17 or later
- Gradle

## Building

```bash
./gradlew build
```

## Running

```bash
./gradlew bootRun
```

The service will start on port 8080 by default.

## Testing the Service

You can test if the service is running properly using these curl commands:

### 1. Check Service Configuration
```bash
curl -X GET http://localhost:8080/v1/config
```
Expected response:
```json
{
    "catalog-version": "1.5.0",
    "catalog-impl": "software.amazon.s3tables.iceberg.S3TablesCatalog",
    "warehouse": "<warehouse-name>"
}
```

### 2. List Namespaces
```bash
curl -X GET http://localhost:8080/v1/namespaces
```

### 3. Create a Namespace
```bash
curl -X POST http://localhost:8080/v1/namespaces \
  -H "Content-Type: application/json" \
  -d '{
    "namespace": "example.test",
    "properties": {
      "comment": "Test namespace"
    }
  }'
```

### 4. Create a Table
```bash
curl -X POST http://localhost:8080/v1/tables \
  -H "Content-Type: application/json" \
  -d '{
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
  }'
```

### 5. List Tables
```bash
curl -X GET http://localhost:8080/v1/tables?namespace=example.test
```

## API Endpoints

### Catalog Configuration
- `GET /v1/config` - Get catalog configuration

### Namespace Operations
- `POST /v1/namespaces` - Create a new namespace
- `GET /v1/namespaces` - List namespaces
- `GET /v1/namespaces/{namespace}` - Get namespace details

### Table Operations
- `POST /v1/tables` - Create a new table
- `GET /v1/tables` - List tables
- `GET /v1/tables/{namespace}/{table}` - Get table details
- `DELETE /v1/tables/{namespace}/{table}` - Drop a table

## Configuration

The service can be configured through `src/main/resources/application.properties`. Key properties include:

- `server.port` - HTTP port (default: 8080)
- `spring.application.name` - Application name
- `management.endpoints.web.exposure.include` - Exposed actuator endpoints
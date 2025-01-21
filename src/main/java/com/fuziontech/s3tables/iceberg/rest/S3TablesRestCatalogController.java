package com.fuziontech.s3tables.iceberg.rest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import software.amazon.s3tables.iceberg.S3TablesCatalog;

@RestController
@RequestMapping("/v1")
public class S3TablesRestCatalogController {

    @Autowired
    private S3TablesCatalog catalog;

    @GetMapping("/config")
    public Map<String, String> getConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("catalog-version", "1.5.0");
        config.put("catalog-impl", catalog.getClass().getName());
        return config;
    }

    @PostMapping("/namespaces")
    public ResponseEntity<?> createNamespace(
            @RequestBody Map<String, Object> request) {
        try {
            Object namespaceObj = request.get("namespace");
            if (namespaceObj == null) {
                throw new IllegalArgumentException("Namespace is required");
            }

            String[] levels;
            if (namespaceObj instanceof String) {
                String namespaceStr = (String) namespaceObj;
                if (namespaceStr.trim().isEmpty()) {
                    throw new IllegalArgumentException("Namespace cannot be empty");
                }
                levels = namespaceStr.split("\\.");
            } else if (namespaceObj instanceof List) {
                List<?> namespaceParts = (List<?>) namespaceObj;
                if (namespaceParts.isEmpty()) {
                    throw new IllegalArgumentException("Namespace cannot be empty");
                }
                levels = namespaceParts.stream()
                        .map(part -> {
                            if (part == null || part.toString().trim().isEmpty()) {
                                throw new IllegalArgumentException("Namespace parts cannot be null or empty");
                            }
                            return part.toString().trim();
                        })
                        .toArray(String[]::new);
            } else {
                throw new IllegalArgumentException("Namespace must be a string or array");
            }

            // Validate namespace format
            for (String level : levels) {
                if (!level.matches("^[a-zA-Z0-9_]+$")) {
                    throw new IllegalArgumentException("Namespace parts can only contain alphanumeric characters and underscores");
                }
            }

            Map<String, Object> rawProperties = (Map<String, Object>) request.getOrDefault("properties", new HashMap<>());
            Map<String, String> properties = rawProperties.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> e.getValue() != null ? e.getValue().toString() : ""
                    ));

            Namespace namespace = Namespace.of(levels);
            catalog.createNamespace(namespace, properties);

            Map<String, Object> response = new HashMap<>();
            response.put("namespace", Arrays.asList(levels));
            response.put("properties", properties);

            return ResponseEntity.ok(response);
        } catch (AlreadyExistsException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of(
                            "code", "NAMESPACE_ALREADY_EXISTS",
                            "error", "Namespace already exists",
                            "message", e.getMessage()
                    ));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of(
                            "code", "INVALID_ARGUMENT",
                            "error", "Invalid namespace format",
                            "message", e.getMessage()
                    ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of(
                            "code", "INTERNAL_ERROR",
                            "error", "Failed to create namespace",
                            "message", e.getMessage()
                    ));
        }
    }

    @GetMapping("/namespaces")
    public ResponseEntity<?> listNamespaces(
            @RequestParam(required = false) String parent) {
        try {
            List<Namespace> namespaces;
            if (parent != null && !parent.trim().isEmpty()) {
                String[] levels = parent.split("\\.");
                Namespace parentNs = Namespace.of(levels);
                namespaces = catalog.listNamespaces(parentNs);
            } else {
                namespaces = catalog.listNamespaces();
            }

            Map<String, Object> response = new HashMap<>();
            List<List<String>> namespaceList = namespaces.stream()
                    .map(ns -> Arrays.asList(ns.levels()))
                    .collect(Collectors.toList());
            response.put("namespaces", namespaceList);

            return ResponseEntity.ok(response);
        } catch (NoSuchNamespaceException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of(
                            "code", "NAMESPACE_NOT_FOUND",
                            "error", "Namespace not found",
                            "message", e.getMessage()
                    ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of(
                            "code", "INTERNAL_ERROR",
                            "error", "Failed to list namespaces",
                            "message", e.getMessage()
                    ));
        }
    }

    @GetMapping("/namespaces/{namespace}")
    public ResponseEntity<?> getNamespace(
            @PathVariable String namespace) {
        try {
            String[] levels = namespace.split("\\.");
            Namespace ns = Namespace.of(levels);
            Map<String, String> properties = catalog.loadNamespaceMetadata(ns);

            Map<String, Object> response = new HashMap<>();
            response.put("namespace", Arrays.asList(levels));
            response.put("properties", properties);

            return ResponseEntity.ok(response);
        } catch (NoSuchNamespaceException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of(
                            "code", "NAMESPACE_NOT_FOUND",
                            "error", "Namespace not found",
                            "message", e.getMessage()
                    ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of(
                            "code", "INTERNAL_ERROR",
                            "error", "Failed to get namespace",
                            "message", e.getMessage()
                    ));
        }
    }

    @PostMapping("/tables")
    public ResponseEntity<?> createTable(
            @RequestBody Map<String, Object> request) {
        try {
            String namespace = (String) request.get("namespace");
            String name = (String) request.get("name");
            Map<String, Object> schema = (Map<String, Object>) request.get("schema");
            Map<String, Object> spec = (Map<String, Object>) request.get("spec");
            Map<String, String> properties = (Map<String, String>) request.getOrDefault("properties", new HashMap<>());

            TableIdentifier identifier = TableIdentifier.of(Namespace.of(namespace.split("\\.")), name);

            Schema icebergSchema = SchemaParser.fromJson(schema.toString());
            PartitionSpec partitionSpec = spec != null
                    ? PartitionSpecParser.fromJson(icebergSchema, spec.toString())
                    : PartitionSpec.unpartitioned();

            Table table = catalog.createTable(
                    identifier,
                    icebergSchema,
                    partitionSpec,
                    properties);

            Map<String, Object> response = new HashMap<>();
            response.put("name", table.name());
            response.put("schema", SchemaParser.toJson(table.schema()));
            response.put("spec", PartitionSpecParser.toJson(table.spec()));
            response.put("properties", table.properties());
            return ResponseEntity.ok(response);
        } catch (AlreadyExistsException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of(
                            "code", "TABLE_ALREADY_EXISTS",
                            "error", "Table already exists",
                            "message", e.getMessage()
                    ));
        } catch (NoSuchNamespaceException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of(
                            "code", "NAMESPACE_NOT_FOUND",
                            "error", "Namespace not found",
                            "message", e.getMessage()
                    ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of(
                            "code", "INTERNAL_ERROR",
                            "error", "Failed to create table",
                            "message", e.getMessage()
                    ));
        }
    }

    @GetMapping("/tables")
    public ResponseEntity<?> listTables(
            @RequestParam(required = false) String namespace) {
        try {
            Namespace ns = namespace != null ? Namespace.of(namespace.split("\\.")) : null;
            List<TableIdentifier> tables = catalog.listTables(ns);

            List<Map<String, Object>> result = tables.stream()
                    .map(table -> {
                        Map<String, Object> tableMap = new HashMap<>();
                        tableMap.put("namespace", String.join(".", table.namespace().levels()));
                        tableMap.put("name", table.name());
                        return tableMap;
                    })
                    .collect(Collectors.toList());

            return ResponseEntity.ok(result);
        } catch (NoSuchNamespaceException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of(
                            "code", "NAMESPACE_NOT_FOUND",
                            "error", "Namespace not found",
                            "message", e.getMessage()
                    ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of(
                            "code", "INTERNAL_ERROR",
                            "error", "Failed to list tables",
                            "message", e.getMessage()
                    ));
        }
    }

    @GetMapping("/tables/{namespace}/{table}")
    public ResponseEntity<?> getTable(
            @PathVariable String namespace,
            @PathVariable String table) {
        try {
            TableIdentifier identifier = TableIdentifier.of(Namespace.of(namespace.split("\\.")), table);
            Table icebergTable = catalog.loadTable(identifier);

            Map<String, Object> response = new HashMap<>();
            response.put("name", icebergTable.name());
            response.put("schema", SchemaParser.toJson(icebergTable.schema()));
            response.put("spec", PartitionSpecParser.toJson(icebergTable.spec()));
            response.put("properties", icebergTable.properties());
            return ResponseEntity.ok(response);
        } catch (NoSuchTableException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of(
                            "code", "TABLE_NOT_FOUND",
                            "error", "Table not found",
                            "message", e.getMessage()
                    ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of(
                            "code", "INTERNAL_ERROR",
                            "error", "Failed to get table",
                            "message", e.getMessage()
                    ));
        }
    }

    @DeleteMapping("/tables/{namespace}/{table}")
    public ResponseEntity<?> dropTable(
            @PathVariable String namespace,
            @PathVariable String table) {
        try {
            TableIdentifier identifier = TableIdentifier.of(Namespace.of(namespace.split("\\.")), table);
            catalog.dropTable(identifier);
            return ResponseEntity.ok().build();
        } catch (NoSuchTableException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of(
                            "code", "TABLE_NOT_FOUND",
                            "error", "Table not found",
                            "message", e.getMessage()
                    ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of(
                            "code", "INTERNAL_ERROR",
                            "error", "Failed to drop table",
                            "message", e.getMessage()
                    ));
        }
    }
}

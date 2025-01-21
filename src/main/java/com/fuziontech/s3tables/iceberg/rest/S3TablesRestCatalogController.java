package com.fuziontech.s3tables.iceberg.rest;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import software.amazon.s3tables.iceberg.S3TablesCatalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
            String namespaceStr = (String) request.get("namespace");
            Map<String, String> properties = (Map<String, String>) request.getOrDefault("properties", new HashMap<>());

            // Split namespace string into levels
            String[] levels = namespaceStr.split("\\.");
            Namespace namespace = Namespace.of(levels);

            catalog.createNamespace(namespace, properties);
            return ResponseEntity.ok().build();
        } catch (AlreadyExistsException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of("error", "Namespace already exists", "message", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to create namespace", "message", e.getMessage()));
        }
    }

    @GetMapping("/namespaces")
    public ResponseEntity<?> listNamespaces(
            @RequestParam(required = false) String parent) {
        try {
            Namespace parentNs = parent != null ? Namespace.of(parent.split("\\.")) : null;
            List<Namespace> namespaces = catalog.listNamespaces(parentNs);

            List<Map<String, Object>> result = namespaces.stream()
                .map(ns -> {
                    Map<String, Object> nsMap = new HashMap<>();
                    nsMap.put("namespace", String.join(".", ns.levels()));
                    return nsMap;
                })
                .collect(Collectors.toList());

            return ResponseEntity.ok(result);
        } catch (NoSuchNamespaceException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", "Namespace not found", "message", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to list namespaces", "message", e.getMessage()));
        }
    }

    @GetMapping("/namespaces/{namespace}")
    public ResponseEntity<?> getNamespace(
            @PathVariable String namespace) {
        try {
            Namespace ns = Namespace.of(namespace.split("\\."));
            Map<String, String> properties = catalog.loadNamespaceMetadata(ns);

            Map<String, Object> result = new HashMap<>();
            result.put("namespace", namespace);
            result.put("properties", properties);
            return ResponseEntity.ok(result);
        } catch (NoSuchNamespaceException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", "Namespace not found", "message", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to get namespace", "message", e.getMessage()));
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
                .body(Map.of("error", "Table already exists", "message", e.getMessage()));
        } catch (NoSuchNamespaceException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", "Namespace not found", "message", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to create table", "message", e.getMessage()));
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
                .body(Map.of("error", "Namespace not found", "message", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to list tables", "message", e.getMessage()));
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
                .body(Map.of("error", "Table not found", "message", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to get table", "message", e.getMessage()));
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
                .body(Map.of("error", "Table not found", "message", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to drop table", "message", e.getMessage()));
        }
    }
}
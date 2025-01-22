package com.fuziontech.s3tables.iceberg.rest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.s3tables.iceberg.S3TablesCatalog;

@RestController
@RequestMapping("/v1")
public class S3TablesRestCatalogController {

    @Autowired
    private S3TablesCatalog catalog;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private ResponseEntity<Map<String, Object>> errorResponse(String message, String type, int code) {
        Map<String, Object> response = Map.of(
                "error", Map.of(
                        "message", message,
                        "type", type,
                        "code", code
                )
        );
        return ResponseEntity.status(code).body(response);
    }

    @GetMapping("/config")
    public ResponseEntity<Map<String, Object>> getConfig(@RequestParam(required = false) String warehouse) {
        try {
            Map<String, Object> config = new HashMap<>();

            // Add catalog defaults
            Map<String, String> defaults = new HashMap<>();
            defaults.put("clients", "4");
            config.put("defaults", defaults);

            // Add catalog overrides
            Map<String, String> overrides = new HashMap<>();
            if (warehouse != null) {
                overrides.put("warehouse", warehouse);
            } else {
                overrides.put("warehouse", "s3://posthog-table-bucket/");
            }
            config.put("overrides", overrides);

            // Add supported endpoints
            List<String> endpoints = Arrays.asList(
                    "GET /v1/config",
                    "GET /v1/namespaces",
                    "POST /v1/namespaces",
                    "GET /v1/namespaces/{namespace}",
                    "DELETE /v1/namespaces/{namespace}",
                    "GET /v1/namespaces/{namespace}/tables",
                    "POST /v1/namespaces/{namespace}/tables",
                    "GET /v1/namespaces/{namespace}/tables/{table}",
                    "DELETE /v1/namespaces/{namespace}/tables/{table}",
                    "POST /v1/tables/rename"
            );
            config.put("endpoints", endpoints);

            return ResponseEntity.ok(config);
        } catch (IllegalArgumentException e) {
            return errorResponse(e.getMessage(), "IllegalArgumentException", HttpStatus.BAD_REQUEST.value());
        } catch (Exception e) {
            return errorResponse(e.getMessage(), "InternalServerError", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }

    @PostMapping("/namespaces")
    public ResponseEntity<Map<String, Object>> createNamespace(@RequestBody Map<String, Object> request) {
        try {
            Object namespaceObj = request.get("namespace");
            if (namespaceObj == null) {
                return errorResponse("Namespace is required", "IllegalArgumentException", HttpStatus.BAD_REQUEST.value());
            }

            String[] levels;
            if (namespaceObj instanceof String) {
                String namespaceStr = (String) namespaceObj;
                if (namespaceStr.trim().isEmpty()) {
                    return errorResponse("Namespace cannot be empty", "IllegalArgumentException", HttpStatus.BAD_REQUEST.value());
                }
                levels = namespaceStr.split("\\.");
            } else if (namespaceObj instanceof List) {
                List<?> namespaceParts = (List<?>) namespaceObj;
                if (namespaceParts.isEmpty()) {
                    return errorResponse("Namespace cannot be empty", "IllegalArgumentException", HttpStatus.BAD_REQUEST.value());
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
                return errorResponse("Namespace must be a string or array", "IllegalArgumentException", HttpStatus.BAD_REQUEST.value());
            }

            // Validate namespace format
            for (String level : levels) {
                if (!level.matches("^[a-zA-Z0-9_]+$")) {
                    return errorResponse("Namespace parts can only contain alphanumeric characters and underscores", "IllegalArgumentException", HttpStatus.BAD_REQUEST.value());
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
            return errorResponse(e.getMessage(), "AlreadyExistsException", HttpStatus.CONFLICT.value());
        } catch (IllegalArgumentException e) {
            return errorResponse(e.getMessage(), "IllegalArgumentException", HttpStatus.BAD_REQUEST.value());
        } catch (Exception e) {
            return errorResponse(e.getMessage(), "InternalServerError", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }

    @GetMapping("/namespaces")
    public ResponseEntity<Map<String, Object>> listNamespaces(
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
            return errorResponse(e.getMessage(), "NoSuchNamespaceException", HttpStatus.NOT_FOUND.value());
        } catch (Exception e) {
            return errorResponse(e.getMessage(), "InternalServerError", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }

    @GetMapping("/namespaces/{namespace}")
    public ResponseEntity<Map<String, Object>> getNamespace(
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
            return errorResponse(e.getMessage(), "NoSuchNamespaceException", HttpStatus.NOT_FOUND.value());
        } catch (Exception e) {
            return errorResponse(e.getMessage(), "InternalServerError", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }

    @PostMapping("/tables")
    public ResponseEntity<Map<String, Object>> createTable(
            @RequestBody Map<String, Object> request) {
        try {
            String namespace = (String) request.get("namespace");
            String name = (String) request.get("name");
            Map<String, Object> schema = (Map<String, Object>) request.get("schema");
            Map<String, Object> spec = (Map<String, Object>) request.get("spec");
            Map<String, String> properties = (Map<String, String>) request.getOrDefault("properties", new HashMap<>());

            if (namespace == null || name == null || schema == null) {
                return errorResponse("Namespace, name, and schema are required", "IllegalArgumentException", HttpStatus.BAD_REQUEST.value());
            }

            TableIdentifier identifier = TableIdentifier.of(Namespace.of(namespace.split("\\.")), name);

            // Convert schema and spec to proper JSON strings
            String schemaJson = objectMapper.writeValueAsString(schema);
            Schema icebergSchema = SchemaParser.fromJson(schemaJson);

            PartitionSpec partitionSpec;
            if (spec != null) {
                String specJson = objectMapper.writeValueAsString(spec);
                partitionSpec = PartitionSpecParser.fromJson(icebergSchema, specJson);
            } else {
                partitionSpec = PartitionSpec.unpartitioned();
            }

            Table table = catalog.createTable(
                    identifier,
                    icebergSchema,
                    partitionSpec,
                    properties);

            Map<String, Object> metadata = new HashMap<>(table.properties());
            metadata.put("format-version", 2);
            metadata.put("location", table.location());
            metadata.put("table-uuid", table.uuid());
            metadata.put("last-updated-ms", System.currentTimeMillis());
            metadata.put("last-column-id", icebergSchema.highestFieldId());
            metadata.put("schema", SchemaParser.toJson(icebergSchema));
            metadata.put("current-schema-id", icebergSchema.schemaId());
            metadata.put("schemas", Arrays.asList(SchemaParser.toJson(icebergSchema)));
            metadata.put("partition-spec", PartitionSpecParser.toJson(partitionSpec));
            metadata.put("default-spec-id", partitionSpec.specId());
            metadata.put("partition-specs", Arrays.asList(PartitionSpecParser.toJson(partitionSpec)));
            metadata.put("sort-orders", Arrays.asList());
            metadata.put("default-sort-order-id", 0);
            metadata.put("snapshots", Arrays.asList());
            metadata.put("snapshot-log", Arrays.asList());
            metadata.put("metadata-log", Arrays.asList());

            Map<String, Object> response = new HashMap<>();
            response.put("metadata-location", table.location());
            response.put("metadata", metadata);
            return ResponseEntity.ok(response);
        } catch (AlreadyExistsException e) {
            return errorResponse(e.getMessage(), "AlreadyExistsException", HttpStatus.CONFLICT.value());
        } catch (NoSuchNamespaceException e) {
            return errorResponse(e.getMessage(), "NoSuchNamespaceException", HttpStatus.NOT_FOUND.value());
        } catch (IllegalArgumentException e) {
            return errorResponse(e.getMessage(), "IllegalArgumentException", HttpStatus.BAD_REQUEST.value());
        } catch (JsonProcessingException e) {
            return errorResponse("Invalid schema or partition spec format: " + e.getMessage(), "IllegalArgumentException", HttpStatus.BAD_REQUEST.value());
        } catch (Exception e) {
            return errorResponse(e.getMessage(), "InternalServerError", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }

    @PostMapping("/namespaces/{namespace}/tables")
    public ResponseEntity<Map<String, Object>> createTableInNamespace(
            @PathVariable String namespace,
            @RequestBody Map<String, Object> request) {
        // Add namespace to the request
        Map<String, Object> modifiedRequest = new HashMap<>(request);
        modifiedRequest.put("namespace", namespace);
        return createTable(modifiedRequest);
    }

    @GetMapping("/tables")
    public ResponseEntity<Map<String, Object>> listTables(
            @RequestParam(required = false) String namespace) {
        try {
            Namespace ns = namespace != null ? Namespace.of(namespace.split("\\.")) : null;
            List<TableIdentifier> tables = catalog.listTables(ns);

            Map<String, Object> response = new HashMap<>();
            List<Map<String, Object>> identifiers = tables.stream()
                    .map(table -> {
                        Map<String, Object> identifier = new HashMap<>();
                        identifier.put("namespace", Arrays.asList(table.namespace().levels()));
                        identifier.put("name", table.name());
                        return identifier;
                    })
                    .collect(Collectors.toList());
            response.put("identifiers", identifiers);

            return ResponseEntity.ok(response);
        } catch (NoSuchNamespaceException e) {
            return errorResponse(e.getMessage(), "NoSuchNamespaceException", HttpStatus.NOT_FOUND.value());
        } catch (Exception e) {
            return errorResponse(e.getMessage(), "InternalServerError", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }

    @GetMapping("/namespaces/{namespace}/tables")
    public ResponseEntity<Map<String, Object>> listTablesInNamespace(
            @PathVariable String namespace) {
        try {
            Namespace ns = Namespace.of(namespace.split("\\."));
            List<TableIdentifier> tables = catalog.listTables(ns);

            Map<String, Object> response = new HashMap<>();
            List<Map<String, Object>> identifiers = tables.stream()
                    .map(table -> {
                        Map<String, Object> identifier = new HashMap<>();
                        identifier.put("namespace", Arrays.asList(table.namespace().levels()));
                        identifier.put("name", table.name());
                        return identifier;
                    })
                    .collect(Collectors.toList());
            response.put("identifiers", identifiers);

            return ResponseEntity.ok(response);
        } catch (NoSuchNamespaceException e) {
            return errorResponse(e.getMessage(), "NoSuchNamespaceException", HttpStatus.NOT_FOUND.value());
        } catch (Exception e) {
            return errorResponse(e.getMessage(), "InternalServerError", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }

    @GetMapping("/namespaces/{namespace}/tables/{table}")
    public ResponseEntity<Map<String, Object>> getTableInNamespace(
            @PathVariable String namespace,
            @PathVariable String table,
            @RequestParam(required = false) String snapshots) {
        return getTable(namespace, table, snapshots);
    }

    @GetMapping("/tables/{namespace}/{table}")
    public ResponseEntity<Map<String, Object>> getTable(
            @PathVariable String namespace,
            @PathVariable String table,
            @RequestParam(required = false) String snapshots) {
        try {
            TableIdentifier identifier = TableIdentifier.of(Namespace.of(namespace.split("\\.")), table);
            Table icebergTable = catalog.loadTable(identifier);

            Map<String, Object> metadata = new HashMap<>(icebergTable.properties());
            metadata.put("format-version", 2);
            metadata.put("location", icebergTable.location());
            metadata.put("table-uuid", icebergTable.uuid());
            metadata.put("last-updated-ms", System.currentTimeMillis());
            metadata.put("last-column-id", icebergTable.schema().highestFieldId());
            metadata.put("schema", SchemaParser.toJson(icebergTable.schema()));
            metadata.put("current-schema-id", icebergTable.schema().schemaId());
            metadata.put("schemas", Arrays.asList(SchemaParser.toJson(icebergTable.schema())));
            metadata.put("partition-spec", PartitionSpecParser.toJson(icebergTable.spec()));
            metadata.put("default-spec-id", icebergTable.spec().specId());
            metadata.put("partition-specs", Arrays.asList(PartitionSpecParser.toJson(icebergTable.spec())));
            metadata.put("sort-orders", Arrays.asList());
            metadata.put("default-sort-order-id", 0);

            // Include snapshots if requested
            if ("all".equals(snapshots)) {
                List<Map<String, Object>> snapshotsList = new ArrayList<>();
                icebergTable.snapshots().forEach(snapshot -> {
                    Map<String, Object> snapshotInfo = new HashMap<>();
                    snapshotInfo.put("snapshot-id", snapshot.snapshotId());
                    snapshotInfo.put("timestamp-ms", snapshot.timestampMillis());
                    snapshotInfo.put("manifest-list", snapshot.manifestListLocation());
                    snapshotInfo.put("summary", snapshot.summary());
                    snapshotsList.add(snapshotInfo);
                });
                metadata.put("snapshots", snapshotsList);
                metadata.put("snapshot-log", Arrays.asList());
            } else {
                metadata.put("snapshots", Arrays.asList());
                metadata.put("snapshot-log", Arrays.asList());
            }
            metadata.put("metadata-log", Arrays.asList());

            Map<String, Object> response = new HashMap<>();
            response.put("metadata-location", icebergTable.location());
            response.put("metadata", metadata);
            return ResponseEntity.ok(response);
        } catch (NoSuchTableException e) {
            return errorResponse(e.getMessage(), "NoSuchTableException", HttpStatus.NOT_FOUND.value());
        } catch (NoSuchNamespaceException e) {
            return errorResponse(e.getMessage(), "NoSuchNamespaceException", HttpStatus.NOT_FOUND.value());
        } catch (IllegalArgumentException e) {
            return errorResponse(e.getMessage(), "IllegalArgumentException", HttpStatus.BAD_REQUEST.value());
        } catch (Exception e) {
            return errorResponse(e.getMessage(), "InternalServerError", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }

    @DeleteMapping("/tables/{namespace}/{table}")
    public ResponseEntity<Map<String, Object>> dropTable(
            @PathVariable String namespace,
            @PathVariable String table) {
        try {
            TableIdentifier identifier = TableIdentifier.of(Namespace.of(namespace.split("\\.")), table);
            catalog.dropTable(identifier);
            return ResponseEntity.ok().build();
        } catch (NoSuchTableException e) {
            return errorResponse(e.getMessage(), "NoSuchTableException", HttpStatus.NOT_FOUND.value());
        } catch (NoSuchNamespaceException e) {
            return errorResponse(e.getMessage(), "NoSuchNamespaceException", HttpStatus.NOT_FOUND.value());
        } catch (IllegalArgumentException e) {
            return errorResponse(e.getMessage(), "IllegalArgumentException", HttpStatus.BAD_REQUEST.value());
        } catch (Exception e) {
            return errorResponse(e.getMessage(), "InternalServerError", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }

    @PostMapping("/tables/rename")
    public ResponseEntity<Map<String, Object>> renameTable(
            @RequestBody Map<String, Object> request) {
        try {
            Map<String, Object> source = (Map<String, Object>) request.get("source");
            Map<String, Object> destination = (Map<String, Object>) request.get("destination");

            if (source == null || destination == null) {
                return errorResponse("Source and destination are required", "IllegalArgumentException", HttpStatus.BAD_REQUEST.value());
            }

            List<String> sourceNamespace = (List<String>) source.get("namespace");
            String sourceName = (String) source.get("name");
            List<String> destNamespace = (List<String>) destination.get("namespace");
            String destName = (String) destination.get("name");

            if (sourceNamespace == null || sourceName == null || destNamespace == null || destName == null) {
                return errorResponse("Namespace and name are required for both source and destination", "IllegalArgumentException", HttpStatus.BAD_REQUEST.value());
            }

            TableIdentifier from = TableIdentifier.of(
                    Namespace.of(sourceNamespace.toArray(new String[0])),
                    sourceName);
            TableIdentifier to = TableIdentifier.of(
                    Namespace.of(destNamespace.toArray(new String[0])),
                    destName);

            catalog.renameTable(from, to);
            return ResponseEntity.ok().build();
        } catch (NoSuchTableException e) {
            return errorResponse(e.getMessage(), "NoSuchTableException", HttpStatus.NOT_FOUND.value());
        } catch (IllegalArgumentException e) {
            return errorResponse(e.getMessage(), "IllegalArgumentException", HttpStatus.BAD_REQUEST.value());
        } catch (Exception e) {
            return errorResponse(e.getMessage(), "InternalServerError", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }

    @PostMapping("/namespaces/{namespace}/properties")
    public ResponseEntity<Map<String, Object>> updateNamespaceProperties(
            @PathVariable String namespace,
            @RequestBody Map<String, Object> request) {
        try {
            List<String> removals = request.containsKey("removals")
                    ? (List<String>) request.get("removals")
                    : Collections.emptyList();
            Map<String, String> updates = request.containsKey("updates")
                    ? ((Map<String, Object>) request.get("updates")).entrySet().stream()
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    e -> e.getValue().toString()
                            ))
                    : Collections.emptyMap();

            Namespace ns = Namespace.of(namespace.split("\\."));
            Map<String, String> properties = catalog.loadNamespaceMetadata(ns);

            // Track which properties were actually removed and updated
            List<String> removed = new ArrayList<>();
            for (String key : removals) {
                if (properties.remove(key) != null) {
                    removed.add(key);
                }
            }

            // Track which properties were actually updated
            List<String> updated = new ArrayList<>();
            for (Map.Entry<String, String> entry : updates.entrySet()) {
                String oldValue = properties.put(entry.getKey(), entry.getValue());
                if (!Objects.equals(oldValue, entry.getValue())) {
                    updated.add(entry.getKey());
                }
            }

            catalog.setProperties(ns, properties);

            Map<String, Object> response = new HashMap<>();
            response.put("removed", removed);
            response.put("updated", updated);
            response.put("missing", removals.stream()
                    .filter(key -> !removed.contains(key))
                    .collect(Collectors.toList()));

            return ResponseEntity.ok(response);
        } catch (NoSuchNamespaceException e) {
            return errorResponse(e.getMessage(), "NoSuchNamespaceException", HttpStatus.NOT_FOUND.value());
        } catch (IllegalArgumentException e) {
            return errorResponse(e.getMessage(), "IllegalArgumentException", HttpStatus.BAD_REQUEST.value());
        } catch (Exception e) {
            return errorResponse(e.getMessage(), "InternalServerError", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }

    @GetMapping("/tables/{namespace}/{table}/metadata")
    public ResponseEntity<Map<String, Object>> getTableMetadata(
            @PathVariable String namespace,
            @PathVariable String table) {
        try {
            TableIdentifier identifier = TableIdentifier.of(Namespace.of(namespace.split("\\.")), table);
            Table icebergTable = catalog.loadTable(identifier);

            Map<String, Object> response = new HashMap<>();
            response.put("metadata-location", icebergTable.location());
            response.put("metadata", icebergTable.properties());
            response.put("config", icebergTable.properties());
            response.put("schema", SchemaParser.toJson(icebergTable.schema()));
            response.put("partition-spec", PartitionSpecParser.toJson(icebergTable.spec()));
            response.put("sort-order", "[]"); // Default empty sort order
            response.put("schema-id", icebergTable.schema().schemaId());
            response.put("partition-spec-id", icebergTable.spec().specId());

            return ResponseEntity.ok(response);
        } catch (NoSuchTableException e) {
            return errorResponse(e.getMessage(), "NoSuchTableException", HttpStatus.NOT_FOUND.value());
        } catch (Exception e) {
            return errorResponse(e.getMessage(), "InternalServerError", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }

    @GetMapping("/tables/{namespace}/{table}/snapshots")
    public ResponseEntity<Map<String, Object>> listTableSnapshots(
            @PathVariable String namespace,
            @PathVariable String table) {
        try {
            TableIdentifier identifier = TableIdentifier.of(Namespace.of(namespace.split("\\.")), table);
            Table icebergTable = catalog.loadTable(identifier);

            List<Map<String, Object>> snapshots = new ArrayList<>();
            icebergTable.snapshots().forEach(snapshot -> {
                Map<String, Object> snapshotInfo = new HashMap<>();
                snapshotInfo.put("snapshot-id", snapshot.snapshotId());
                snapshotInfo.put("timestamp-ms", snapshot.timestampMillis());
                snapshotInfo.put("manifest-list", snapshot.manifestListLocation());
                snapshotInfo.put("summary", snapshot.summary());
                snapshots.add(snapshotInfo);
            });

            Map<String, Object> response = new HashMap<>();
            response.put("snapshots", snapshots);

            return ResponseEntity.ok(response);
        } catch (NoSuchTableException e) {
            return errorResponse(e.getMessage(), "NoSuchTableException", HttpStatus.NOT_FOUND.value());
        } catch (Exception e) {
            return errorResponse(e.getMessage(), "InternalServerError", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }

    @GetMapping("/tables/{namespace}/{table}/metadata/{metadata-branch}")
    public ResponseEntity<Map<String, Object>> getMetadataByBranch(
            @PathVariable String namespace,
            @PathVariable String table,
            @PathVariable("metadata-branch") String metadataBranch) {
        try {
            TableIdentifier identifier = TableIdentifier.of(Namespace.of(namespace.split("\\.")), table);
            Table icebergTable = catalog.loadTable(identifier);

            // Note: This is a simplified implementation. In a full implementation,
            // we would need to handle different metadata branches properly
            Map<String, Object> response = new HashMap<>();
            response.put("metadata-location", icebergTable.location());
            response.put("metadata", icebergTable.properties());
            response.put("config", icebergTable.properties());
            response.put("schema", SchemaParser.toJson(icebergTable.schema()));
            response.put("partition-spec", PartitionSpecParser.toJson(icebergTable.spec()));
            response.put("sort-order", "[]");
            response.put("schema-id", icebergTable.schema().schemaId());
            response.put("partition-spec-id", icebergTable.spec().specId());

            return ResponseEntity.ok(response);
        } catch (NoSuchTableException e) {
            return errorResponse(e.getMessage(), "NoSuchTableException", HttpStatus.NOT_FOUND.value());
        } catch (Exception e) {
            return errorResponse(e.getMessage(), "InternalServerError", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }

    @DeleteMapping("/namespaces/{namespace}")
    public ResponseEntity<Map<String, Object>> dropNamespace(
            @PathVariable String namespace) {
        try {
            String[] levels = namespace.split("\\.");
            Namespace ns = Namespace.of(levels);
            catalog.dropNamespace(ns);
            return ResponseEntity.ok().build();
        } catch (NoSuchNamespaceException e) {
            return errorResponse(e.getMessage(), "NoSuchNamespaceException", HttpStatus.NOT_FOUND.value());
        } catch (NamespaceNotEmptyException e) {
            return errorResponse(e.getMessage(), "NamespaceNotEmptyException", HttpStatus.CONFLICT.value());
        } catch (IllegalArgumentException e) {
            return errorResponse(e.getMessage(), "IllegalArgumentException", HttpStatus.BAD_REQUEST.value());
        } catch (Exception e) {
            return errorResponse(e.getMessage(), "InternalServerError", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }

    @GetMapping("/namespaces/{namespace}/views/{view}")
    public ResponseEntity<Map<String, Object>> getView(
            @PathVariable String namespace,
            @PathVariable String view) {
        try {
            TableIdentifier identifier = TableIdentifier.of(Namespace.of(namespace.split("\\.")), view);
            Table icebergTable = catalog.loadTable(identifier);

            // Check if the table is actually a view
            String tableType = icebergTable.properties().getOrDefault("type", "table");
            if (!"view".equals(tableType)) {
                return errorResponse("Not a view: " + identifier, "NotAViewException", HttpStatus.NOT_FOUND.value());
            }

            Map<String, Object> metadata = new HashMap<>(icebergTable.properties());
            metadata.put("format-version", 2);
            metadata.put("location", icebergTable.location());
            metadata.put("table-uuid", icebergTable.uuid());
            metadata.put("last-updated-ms", System.currentTimeMillis());
            metadata.put("last-column-id", icebergTable.schema().highestFieldId());
            metadata.put("schema", SchemaParser.toJson(icebergTable.schema()));
            metadata.put("current-schema-id", icebergTable.schema().schemaId());
            metadata.put("view-version", metadata.getOrDefault("view-version", 1));

            Map<String, Object> response = new HashMap<>();
            response.put("metadata-location", icebergTable.location());
            response.put("metadata", metadata);
            return ResponseEntity.ok(response);
        } catch (NoSuchTableException e) {
            return errorResponse(e.getMessage(), "NoSuchTableException", HttpStatus.NOT_FOUND.value());
        } catch (NoSuchNamespaceException e) {
            return errorResponse(e.getMessage(), "NoSuchNamespaceException", HttpStatus.NOT_FOUND.value());
        } catch (IllegalArgumentException e) {
            return errorResponse(e.getMessage(), "IllegalArgumentException", HttpStatus.BAD_REQUEST.value());
        } catch (Exception e) {
            return errorResponse(e.getMessage(), "InternalServerError", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }
}

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
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateProperties;
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
        Map<String, Object> error = new HashMap<>();
        error.put("message", message);
        error.put("type", type);
        error.put("code", code);
        error.put("status", code);

        Map<String, Object> response = new HashMap<>();
        response.put("error", error);

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
                    "GET /v1/tables/{namespace}/{table}",
                    "DELETE /v1/tables/{namespace}/{table}",
                    "POST /v1/tables/rename",
                    "POST /v1/namespaces/{namespace}/tables/{table}/metrics"
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
            System.out.println("Received create table request: " + objectMapper.writeValueAsString(request));

            // Check if this is a commit request (has requirements and updates fields)
            if (request.containsKey("requirements") && request.containsKey("updates")) {
                // This is a commit request
                String namespace = (String) request.get("namespace");
                String name = (String) request.get("name");
                if (namespace == null || name == null) {
                    return errorResponse("Namespace and name are required for commit", "IllegalArgumentException", HttpStatus.BAD_REQUEST.value());
                }

                TableIdentifier identifier = TableIdentifier.of(Namespace.of(namespace.split("\\.")), name);
                Table table = catalog.loadTable(identifier);

                // Process updates
                List<Map<String, Object>> updates = (List<Map<String, Object>>) request.get("updates");
                for (Map<String, Object> update : updates) {
                    String action = (String) update.get("action");
                    if ("add-snapshot".equals(action)) {
                        Map<String, Object> snapshot = (Map<String, Object>) update.get("snapshot");
                        // Convert sequence number to long before storing
                        Object seqNum = snapshot.get("sequence-number");
                        long sequenceNumber = seqNum instanceof Number ? ((Number) seqNum).longValue() : Long.parseLong(String.valueOf(seqNum));

                        // Update table properties
                        UpdateProperties updateProps = table.updateProperties();
                        updateProps.set("last-updated-ms", String.valueOf(snapshot.get("timestamp-ms")));
                        updateProps.set("last-snapshot-id", String.valueOf(snapshot.get("snapshot-id")));
                        updateProps.set("last-sequence-number", String.valueOf(sequenceNumber));
                        updateProps.set("manifest-list", String.valueOf(snapshot.get("manifest-list")));
                        updateProps.commit();
                    } else if ("set-snapshot-ref".equals(action)) {
                        // For set-snapshot-ref, we just update the reference in properties
                        String refName = (String) update.get("ref-name");
                        Number snapshotId = (Number) update.get("snapshot-id");
                        table.updateProperties()
                                .set("ref." + refName, String.valueOf(snapshotId))
                                .commit();
                    }
                }

                // Build metadata in the format Iceberg expects
                Map<String, Object> metadata = new HashMap<>();
                metadata.put("format-version", 2);
                metadata.put("table-uuid", table.uuid());
                metadata.put("location", table.location());
                metadata.put("last-updated-ms", System.currentTimeMillis());
                metadata.put("last-column-id", table.schema().highestFieldId());
                metadata.put("schema", objectMapper.readValue(SchemaParser.toJson(table.schema()), Object.class));
                metadata.put("schemas", Arrays.asList(objectMapper.readValue(SchemaParser.toJson(table.schema()), Object.class)));
                metadata.put("current-schema-id", table.schema().schemaId());

                // Add partition spec
                metadata.put("partition-spec", objectMapper.readValue(PartitionSpecParser.toJson(table.spec()), Object.class));
                metadata.put("partition-specs", Arrays.asList(objectMapper.readValue(PartitionSpecParser.toJson(table.spec()), Object.class)));
                metadata.put("default-spec-id", table.spec().specId());
                metadata.put("last-partition-id", table.spec().fields().stream()
                        .mapToInt(field -> field.fieldId())
                        .max()
                        .orElse(0));

                // Add sort order
                Map<String, Object> defaultSortOrder = new HashMap<>();
                defaultSortOrder.put("order-id", 0);
                defaultSortOrder.put("fields", Arrays.asList());
                metadata.put("sort-orders", Arrays.asList(defaultSortOrder));
                metadata.put("default-sort-order-id", 0);

                // Add snapshot information
                List<Map<String, Object>> snapshots = new ArrayList<>();
                for (Snapshot s : table.snapshots()) {
                    Map<String, Object> snap = new HashMap<>();
                    snap.put("snapshot-id", s.snapshotId());
                    snap.put("timestamp-ms", s.timestampMillis());
                    snap.put("summary", s.summary());
                    snap.put("manifest-list", s.manifestListLocation());
                    snap.put("schema-id", s.schemaId());
                    snapshots.add(snap);
                }
                metadata.put("snapshots", snapshots);
                metadata.put("snapshot-log", Arrays.asList());
                metadata.put("metadata-log", Arrays.asList());

                // Add properties
                for (Map.Entry<String, String> entry : table.properties().entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    if (key.equals("last-sequence-number")
                            || key.equals("last-updated-ms")
                            || key.equals("last-snapshot-id")
                            || key.equals("schema-id")
                            || key.equals("snapshot-id")
                            || key.equals("timestamp-ms")) {
                        metadata.put(key, Long.parseLong(value));
                    } else {
                        metadata.put(key, value);
                    }
                }

                // Ensure last-sequence-number is present
                if (!metadata.containsKey("last-sequence-number")) {
                    metadata.put("last-sequence-number", 0L);
                }

                Map<String, Object> response = new HashMap<>();
                String metadataLocation = table.location() + "/metadata/00000-" + table.uuid() + ".metadata.json";
                response.put("metadata-location", metadataLocation);
                response.put("metadata", metadata);
                response.put("config", table.properties());

                return ResponseEntity.ok(response);
            }

            // Regular table creation request
            String namespace = (String) request.get("namespace");
            String name = (String) request.get("name");
            Map<String, Object> schema = (Map<String, Object>) request.get("schema");
            Map<String, Object> spec = (Map<String, Object>) request.get("spec");
            Map<String, String> properties = (Map<String, String>) request.getOrDefault("properties", new HashMap<>());

            if (namespace == null || name == null || schema == null) {
                System.out.println("Missing required fields - namespace: " + namespace + ", name: " + name + ", schema present: " + (schema != null));
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
            metadata.put("schema", objectMapper.readValue(SchemaParser.toJson(icebergSchema), Object.class));
            metadata.put("schemas", Arrays.asList(objectMapper.readValue(SchemaParser.toJson(icebergSchema), Object.class)));
            metadata.put("current-schema-id", icebergSchema.schemaId());
            metadata.put("partition-spec", objectMapper.readValue(PartitionSpecParser.toJson(partitionSpec), Object.class));
            metadata.put("default-spec-id", partitionSpec.specId());
            metadata.put("partition-specs", Arrays.asList(objectMapper.readValue(PartitionSpecParser.toJson(partitionSpec), Object.class)));
            metadata.put("last-partition-id", partitionSpec.fields().stream()
                    .mapToInt(field -> field.fieldId())
                    .max()
                    .orElse(0));

            // Handle sort orders
            Map<String, Object> defaultSortOrder = new HashMap<>();
            defaultSortOrder.put("order-id", 0);
            defaultSortOrder.put("fields", Arrays.asList());
            metadata.put("sort-orders", Arrays.asList(defaultSortOrder));
            metadata.put("default-sort-order-id", 0);

            metadata.put("snapshots", Arrays.asList());
            metadata.put("snapshot-log", Arrays.asList());
            metadata.put("metadata-log", Arrays.asList());
            metadata.put("last-sequence-number", 0L);

            // Add table properties
            metadata.putAll(table.properties());

            // Ensure last-sequence-number is present
            if (!metadata.containsKey("last-sequence-number")) {
                metadata.put("last-sequence-number", 0L);
            }

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

    @PostMapping("/namespaces/{namespace}/tables/{table}")
    public ResponseEntity<Map<String, Object>> createTableInNamespaceWithName(
            @PathVariable String namespace,
            @PathVariable String table,
            @RequestBody Map<String, Object> request) {
        // Add namespace and table name to the request
        Map<String, Object> modifiedRequest = new HashMap<>(request);
        modifiedRequest.put("namespace", namespace);
        modifiedRequest.put("name", table);
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

            Map<String, Object> metadata = new HashMap<>();
            metadata.put("format-version", 2);
            metadata.put("location", icebergTable.location());
            metadata.put("table-uuid", icebergTable.uuid());
            metadata.put("last-updated-ms", System.currentTimeMillis());
            metadata.put("last-column-id", icebergTable.schema().highestFieldId());

            // Handle schema properly
            Schema schema = icebergTable.schema();
            String schemaJson = SchemaParser.toJson(schema);
            Map<String, Object> schemaMap = objectMapper.readValue(schemaJson, Map.class);
            metadata.put("schema", schemaMap);
            metadata.put("current-schema-id", schema.schemaId());
            metadata.put("schemas", Arrays.asList(schemaMap));

            // Handle partition spec
            String specJson = PartitionSpecParser.toJson(icebergTable.spec());
            Map<String, Object> specMap = objectMapper.readValue(specJson, Map.class);
            metadata.put("partition-spec", specMap);
            metadata.put("default-spec-id", icebergTable.spec().specId());
            metadata.put("partition-specs", Arrays.asList(specMap));
            metadata.put("last-partition-id", icebergTable.spec().fields().stream()
                    .mapToInt(field -> field.fieldId())
                    .max()
                    .orElse(0));

            // Handle sort orders
            Map<String, Object> defaultSortOrder = new HashMap<>();
            defaultSortOrder.put("order-id", 0);
            defaultSortOrder.put("fields", Arrays.asList());
            metadata.put("sort-orders", Arrays.asList(defaultSortOrder));
            metadata.put("default-sort-order-id", 0);

            // Always include snapshots
            List<Map<String, Object>> snapshotsList = new ArrayList<>();
            icebergTable.snapshots().forEach(snapshot -> {
                Map<String, Object> snapshotInfo = new HashMap<>();
                snapshotInfo.put("snapshot-id", snapshot.snapshotId());
                snapshotInfo.put("timestamp-ms", snapshot.timestampMillis());
                snapshotInfo.put("manifest-list", snapshot.manifestListLocation());
                snapshotInfo.put("schema-id", snapshot.schemaId());
                snapshotInfo.put("summary", snapshot.summary());
                snapshotInfo.put("sequence-number", snapshot.sequenceNumber());
                snapshotsList.add(snapshotInfo);
            });
            metadata.put("snapshots", snapshotsList);
            metadata.put("snapshot-log", new ArrayList<>());
            metadata.put("metadata-log", new ArrayList<>());

            // Add table properties
            for (Map.Entry<String, String> entry : icebergTable.properties().entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (key.equals("last-sequence-number")
                        || key.equals("last-updated-ms")
                        || key.equals("last-snapshot-id")
                        || key.equals("schema-id")
                        || key.equals("snapshot-id")
                        || key.equals("timestamp-ms")) {
                    metadata.put(key, Long.parseLong(value));
                } else {
                    metadata.put(key, value);
                }
            }

            // Ensure last-sequence-number is present
            if (!metadata.containsKey("last-sequence-number")) {
                metadata.put("last-sequence-number", 0L);
            }

            Map<String, Object> response = new HashMap<>();
            response.put("metadata-location", icebergTable.location() + "/metadata/00000-" + icebergTable.uuid() + ".metadata.json");
            response.put("metadata", metadata);
            response.put("config", icebergTable.properties());
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

    @DeleteMapping("/namespaces/{namespace}/tables/{table}")
    public ResponseEntity<Map<String, Object>> dropTableInNamespace(
            @PathVariable String namespace,
            @PathVariable String table) {
        return dropTable(namespace, table);
    }

    @PostMapping("/namespaces/{namespace}/tables/{table}/metrics")
    public ResponseEntity<Map<String, Object>> reportMetrics(
            @PathVariable String namespace,
            @PathVariable String table,
            @RequestBody Map<String, Object> metrics) {
        try {
            // For now, we'll just acknowledge the metrics without storing them
            Map<String, Object> response = new HashMap<>();
            response.put("status", "accepted");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return errorResponse(e.getMessage(), "InternalServerError", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }
}

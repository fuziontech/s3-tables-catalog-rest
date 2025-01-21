package com.fuziontech.s3tables.iceberg.rest;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.s3tables.iceberg.S3TablesCatalog;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class S3TablesRestCatalogConfig {

    @Value("${aws.access.key:#{null}}")
    private String awsAccessKey;

    @Value("${aws.secret.key:#{null}}")
    private String awsSecretKey;

    @Value("${aws.region:us-east-1}")
    private String awsRegion;

    @Value("${aws.s3.warehouse}")
    private String warehouseLocation;

    @Value("${s3tables.impl:org.apache.hadoop.fs.s3a.S3AFileSystem}")
    private String s3Implementation;

    @Bean
    public S3TablesCatalog s3TablesCatalog() {
        Map<String, String> properties = new HashMap<>();

        // AWS Credentials
        properties.put("aws.access.key", awsAccessKey);
        properties.put("aws.secret.key", awsSecretKey);
        properties.put("aws.region", awsRegion);

        // Warehouse location
        properties.put("warehouse", warehouseLocation);

        // S3Tables specific configuration
        properties.put("s3tables.impl", s3Implementation);
        properties.put("s3tables.access-key", awsAccessKey);
        properties.put("s3tables.secret-key", awsSecretKey);
        properties.put("s3tables.region", awsRegion);

        // S3A filesystem configuration
        properties.put("fs.s3a.access.key", awsAccessKey);
        properties.put("fs.s3a.secret.key", awsSecretKey);
        properties.put("fs.s3a.region", awsRegion);
        properties.put("fs.s3a.impl", s3Implementation);
        properties.put("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        properties.put("fs.s3a.connection.ssl.enabled", "true");

        S3TablesCatalog catalog = new S3TablesCatalog();
        catalog.initialize("demo", properties);
        return catalog;
    }
}
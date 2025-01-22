package com.fuziontech.s3tables.iceberg.rest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.util.ContentCachingResponseWrapper;

import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import software.amazon.s3tables.iceberg.S3TablesCatalog;

@Configuration
public class S3TablesRestCatalogConfig implements WebMvcConfigurer {

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
    public FilterRegistrationBean<ResponseLoggingFilter> loggingFilter() {
        FilterRegistrationBean<ResponseLoggingFilter> registrationBean = new FilterRegistrationBean<>();
        registrationBean.setFilter(new ResponseLoggingFilter());
        registrationBean.addUrlPatterns("/v1/*");
        return registrationBean;
    }

    public static class ResponseLoggingFilter extends OncePerRequestFilter {

        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
                throws ServletException, IOException {
            // Log request
            String queryString = request.getQueryString();
            String fullPath = queryString != null
                    ? request.getRequestURI() + "?" + queryString
                    : request.getRequestURI();
            System.out.println("\n=== Incoming Request ===");
            System.out.println("Method: " + request.getMethod());
            System.out.println("Path: " + fullPath);
            System.out.println("Remote Address: " + request.getRemoteAddr());

            // Wrap response
            ContentCachingResponseWrapper responseWrapper = new ContentCachingResponseWrapper(response);

            try {
                filterChain.doFilter(request, responseWrapper);
            } finally {
                // Log response
                byte[] responseBody = responseWrapper.getContentAsByteArray();
                if (responseBody.length > 0) {
                    String responseContent = new String(responseBody);
                    try {
                        Object json = objectMapper.readValue(responseContent, Object.class);
                        String prettyJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
                        System.out.println("\n=== Response ===");
                        System.out.println("Status: " + responseWrapper.getStatus());
                        System.out.println("Body:\n" + prettyJson);
                    } catch (Exception e) {
                        // If it's not JSON, just log the raw content
                        System.out.println("\n=== Response ===");
                        System.out.println("Status: " + responseWrapper.getStatus());
                        System.out.println("Body: " + responseContent);
                    }
                } else {
                    System.out.println("\n=== Response ===");
                    System.out.println("Status: " + responseWrapper.getStatus());
                    System.out.println("Body: <empty>");
                }

                // Copy content to the original response
                responseWrapper.copyBodyToResponse();
            }
        }
    }

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

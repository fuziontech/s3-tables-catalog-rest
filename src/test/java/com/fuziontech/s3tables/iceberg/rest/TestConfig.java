package com.fuziontech.s3tables.iceberg.rest;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import software.amazon.s3tables.iceberg.S3TablesCatalog;
import static org.mockito.Mockito.mock;

@TestConfiguration
public class TestConfig {

  @Bean
  @Primary
  public S3TablesCatalog s3TablesCatalog() {
    return mock(S3TablesCatalog.class);
  }
}
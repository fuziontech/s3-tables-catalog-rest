package com.fuziontech.s3tables.iceberg.rest;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import software.amazon.s3tables.iceberg.S3TablesCatalog;

import java.util.HashMap;
import java.util.Map;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(properties = {
    "s3tables.warehouse=s3://test-bucket/warehouse",
    "s3tables.s3.endpoint=http://localhost:4566",
    "s3tables.s3.access-key-id=test",
    "s3tables.s3.secret-access-key=test"
})
public class S3TablesRestCatalogControllerTest {

  @Autowired
  private MockMvc mockMvc;

  @Autowired
  private S3TablesCatalog catalog;

  @BeforeEach
  void setUp() {
    Map<String, String> options = new HashMap<>();
    options.put("warehouse", "s3://test-bucket/warehouse");
    options.put("s3.endpoint", "http://localhost:4566");

    catalog.initialize("test-catalog", options);

    Map<String, String> properties = new HashMap<>();
    properties.put("comment", "Test namespace");
    catalog.setProperties(Namespace.of("test"), properties);
  }

  @Test
  void testGetConfig() throws Exception {
    mockMvc.perform(get("/v1/config"))
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.catalog-version").value("1.5.0"))
        .andExpect(jsonPath("$.implementation").value("s3tables"))
        .andExpect(jsonPath("$.warehouse").value("s3://test-bucket/warehouse"));
  }

  @Test
  void testListNamespaces() throws Exception {
    mockMvc.perform(get("/v1/namespaces"))
        .andDo(print())
        .andExpect(status().isOk());
  }

  @Test
  void testCreateNamespace() throws Exception {
    String requestBody = "{\"namespace\":\"test\",\"properties\":{\"comment\":\"Test namespace\"}}";
    mockMvc.perform(post("/v1/namespaces")
        .contentType(MediaType.APPLICATION_JSON)
        .content(requestBody))
        .andDo(print())
        .andExpect(status().isOk());
  }

  @Test
  void testListTables() throws Exception {
    mockMvc.perform(get("/v1/tables?namespace=test"))
        .andDo(print())
        .andExpect(status().isOk());
  }
}
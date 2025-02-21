/*
 * Copyright 2020-2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.fhir.analytics;

import java.io.IOException;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@AutoConfigureObservability
@TestPropertySource("classpath:application-test.properties")
public class DataPropertiesTest {

  @Autowired private DataProperties dataProperties;

  private static MockWebServer mockFhirServer;

  @BeforeAll
  public static void setUp() throws IOException {
    mockFhirServer = new MockWebServer();
    mockFhirServer.start(9091);
    mockFhirServer.enqueue(MockUtil.getMockResponse("data/fhir-metadata-sample.json"));
    mockFhirServer.enqueue(MockUtil.getMockResponse("data/fhir-metadata-sample.json"));
  }

  @Test
  void outputParquetFilePath_has_allUnderscores() {
    PipelineConfig pipelineConfig = dataProperties.createBatchOptions();
    Assertions.assertEquals(pipelineConfig.getThriftServerParquetPath().indexOf(":"), -1);
    Assertions.assertEquals(pipelineConfig.getThriftServerParquetPath().indexOf("-"), -1);
    Assertions.assertEquals(pipelineConfig.getThriftServerParquetPath().indexOf("."), -1);
    Assertions.assertNotEquals(pipelineConfig.getThriftServerParquetPath().indexOf("_"), -1);
  }

  @Test
  void outputParquetFilePath_has_rightTimestampSuffix() {
    PipelineConfig pipelineConfig = dataProperties.createBatchOptions();
    String timestampSuffix = pipelineConfig.getTimestampSuffix();
    Assertions.assertTrue(
        pipelineConfig.getFhirEtlOptions().getOutputParquetPath().contains(timestampSuffix));
  }

  @Test
  void thriftServerParquetFilePath_has_rightTimestampSuffix() {
    PipelineConfig pipelineConfig = dataProperties.createBatchOptions();
    String timestampSuffix = pipelineConfig.getTimestampSuffix();
    Assertions.assertTrue(pipelineConfig.getThriftServerParquetPath().contains(timestampSuffix));
  }

  @Test
  void fhirCredentials_are_set() {
    PipelineConfig pipelineConfig = dataProperties.createBatchOptions();
    Assertions.assertEquals("Admin123", pipelineConfig.getFhirEtlOptions().getFhirServerPassword());
    Assertions.assertEquals("Admin", pipelineConfig.getFhirEtlOptions().getFhirServerUserName());
  }

  @AfterAll
  public static void tearDown() throws IOException {
    mockFhirServer.shutdown();
  }
}

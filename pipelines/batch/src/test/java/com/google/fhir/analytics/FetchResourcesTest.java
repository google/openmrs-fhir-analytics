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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.api.SummaryEnum;
import com.cerner.bunsen.exception.ProfileException;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.core.IsNull;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Observation;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FetchResourcesTest {

  @Rule public transient TestPipeline testPipeline = TestPipeline.create();

  private FetchResources fetchResources;

  private FhirContext fhirContext;

  private IParser parser;

  private Bundle bundle;

  @Before
  public void setup() throws IOException {
    URL url = Resources.getResource("observation_decimal_bundle.json");
    String bundleStr = Resources.toString(url, StandardCharsets.UTF_8);
    this.fhirContext = FhirContext.forR4();
    this.parser = fhirContext.newJsonParser();
    bundle = parser.parseResource(Bundle.class, bundleStr);
    String[] args = {"--outputParquetPath=SOME_PATH"};
    FhirEtlOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FhirEtlOptions.class);
    fetchResources = new MockedFetchResources(options, bundle);
    AvroConversionUtil.initializeAvroConverters();
  }

  @Test
  public void testGetPatientId() throws IOException {
    URL url = Resources.getResource("observation.json");
    String obsStr = Resources.toString(url, StandardCharsets.UTF_8);
    Observation observation = parser.parseResource(Observation.class, obsStr);
    String expectedId = "471be3bc-08c7-4d78-a4ab-1b3d044dae67";
    String patientId = FetchResources.getSubjectPatientIdOrNull(observation);
    assertThat(patientId, notNullValue());
    assertThat(patientId, equalTo(expectedId));
  }

  @Test
  public void testGetPatientIdBadSubject() throws IOException {
    URL url = Resources.getResource("observation_bad_subject.json");
    String obsStr = Resources.toString(url, StandardCharsets.UTF_8);
    Observation observation = parser.parseResource(Observation.class, obsStr);
    String patientId = FetchResources.getSubjectPatientIdOrNull(observation);
    assertThat(patientId, IsNull.nullValue());
  }

  @Test
  public void testPatientIdsFromBundle() {
    List<SearchSegmentDescriptor> searchSegments =
        Lists.newArrayList(SearchSegmentDescriptor.create("test_url", 10));
    PCollection<SearchSegmentDescriptor> segments =
        testPipeline.apply("Input", Create.of(searchSegments));
    segments.apply(fetchResources);
    PCollection<KV<String, Integer>> patientIds =
        segments.apply("Extract PatientIds", fetchResources);
    List<KV<String, Integer>> expectedIds = new ArrayList<>();
    expectedIds.add(KV.of("7_SOME_PATIENT_REF", 1));
    PAssert.that(patientIds).containsInAnyOrder(expectedIds);
    testPipeline.run();
  }

  static class MockedFetchResources extends FetchResources {

    MockedFetchResources(FhirEtlOptions options, Bundle bundle) {
      super(options, "TEST_FetchResources");
      this.fetchSearchPageFn =
          new SearchFn(options, "TEST_FetchResources") {

            @Override
            public void setup() throws SQLException, ProfileException {
              super.setup();
              this.fhirSearchUtil = Mockito.mock(FhirSearchUtil.class);
              when(fhirSearchUtil.searchByUrl(
                      any(String.class), any(Integer.class), any(SummaryEnum.class)))
                  .thenReturn(bundle);
              this.parquetUtil = Mockito.mock(ParquetUtil.class);
            }
          };
    }
  }
}

// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package org.openmrs.analytics;

import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.common.io.Resources;
import junit.framework.TestCase;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.io.FileUtils;
import org.hl7.fhir.dstu3.model.Encounter;
import org.hl7.fhir.dstu3.model.Resource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JdbcFhirModeTest extends TestCase {
	
	private String resourceStr;
	
	@Rule
	public transient TestPipeline testPipeline = TestPipeline.create();
	
	private Resource resource;
	
	private FhirContext fhirContext;
	
	private JdbcFhirMode jdbcFhirMode;
	
	private ParquetUtil parquetUtil;
	
	private String basePath = "/tmp/JUNIT/Parquet/TEST/";
	
	@Before
	public void setup() throws IOException {
		URL url = Resources.getResource("encounter.json");
		resourceStr = Resources.toString(url, StandardCharsets.UTF_8);
		this.fhirContext = FhirContext.forDstu3();
		IParser parser = fhirContext.newJsonParser();
		resource = parser.parseResource(Encounter.class, resourceStr);
		jdbcFhirMode = new JdbcFhirMode();
		parquetUtil = new ParquetUtil(basePath);
		// clean up if folder exists
		File file = new File(basePath);
		if (file.exists())
			FileUtils.cleanDirectory(file);
	}
	
	@Test
	public void testGetJdbcConfig() throws PropertyVetoException {
		String[] args = { "--fhirSinkPath=", "--openmrsServerUrl=http://localhost:8099/openmrs" };
		FhirEtl.FhirEtlOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
		        .as(FhirEtl.FhirEtlOptions.class);
		JdbcIO.DataSourceConfiguration config = jdbcFhirMode.getJdbcConfig(options.getJdbcDriverClass(),
		    options.getJdbcUrl(), options.getDbUser(), options.getDbPassword(), options.getJdbcMaxPoolSize());
		assertTrue(JdbcIO.PoolableDataSourceProvider.of(config).apply(null) instanceof PoolingDataSource);
	}
	
	@Test
	public void testCreateIdRanges() {
		int batchSize = 100;
		
		Integer[] maxId = { 200 };
		PCollection<KV<String, Iterable<Integer>>> idRanges = testPipeline
		        .apply("Create input", Create.of(Arrays.asList(maxId)))
		        // Inject
		        .apply(new JdbcFhirMode.CreateIdRanges(batchSize));
		
		Integer[] fromZero = { 0 };
		Integer[] from100 = { 100 };
		PAssert.that(idRanges).containsInAnyOrder(KV.of("0,100", Arrays.asList(fromZero)),
		    KV.of("100,200", Arrays.asList(from100)));
		testPipeline.run();
	}
	
	@Test
	public void testCreateSearchSegmentDescriptor() {
		
		String resourceType = "Encounter";
		String baseBundleUrl = "https://test.com/" + resourceType;
		int parallelRequests = 2;
		String[] uuIds = { "<uuid>,<uuid>,<uuid>", "<uuid>,<uuid>,<uuid>" };
		PCollection<SearchSegmentDescriptor> segements = testPipeline.apply("Create input", Create.of(Arrays.asList(uuIds)))
		        // Inject
		        .apply(new JdbcFhirMode.CreateSearchSegments(baseBundleUrl, parallelRequests));
		// create expected output
		List<SearchSegmentDescriptor> segments = new ArrayList<>();
		// first batch
		segments.add(SearchSegmentDescriptor.create(
		    String.format("%s?_id=%s", baseBundleUrl, String.join(",", new String[] { "<uuid>,<uuid>,<uuid>" })), 3));
		// second batch
		segments.add(SearchSegmentDescriptor.create(
		    String.format("%s?_id=%s", baseBundleUrl, String.join(",", new String[] { "<uuid>,<uuid>,<uuid>" })), 3));
		// assert
		PAssert.that(segements).containsInAnyOrder(segments);
		testPipeline.run();
	}
	
	@Test
	public void testCreateFhirReverseMap() throws IOException {
		// here we pass Encounters as such we expect visits to be included in the reverseMap as well
		String[] args = { "--tableFhirMapPath=../utils/dbz_event_to_fhir_config.json",
		        "--searchList=Patient,Encounter,Observation", "--fhirSinkPath=", "--fileParquetPath=/tmp/TEST/",
		        "--openmrsServerUrl=http://localhost:8099/openmrs" };
		FhirEtl.FhirEtlOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
		        .as(FhirEtl.FhirEtlOptions.class);
		
		LinkedHashMap<String, String> reverseMap = jdbcFhirMode.createFhirReverseMap(options.getSearchList(),
		    options.getTableFhirMapPath());
		// we expect 4 objects, and visit should be included
		assertTrue(reverseMap.size() == 4);// not 3
		assertFalse(reverseMap.get("visit").isEmpty());
		assertFalse(reverseMap.get("encounter").isEmpty());
		assertFalse(reverseMap.get("obs").isEmpty());
		assertFalse(reverseMap.get("person").isEmpty());
		
	}
	
}

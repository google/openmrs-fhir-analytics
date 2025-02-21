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

import ca.uhn.fhir.rest.api.SummaryEnum;
import com.cerner.bunsen.exception.ProfileException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.fhir.analytics.view.ViewApplicationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.hl7.fhir.r4.model.Base;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Property;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Resource;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This function object fetches all input segments from the FHIR source and writes each resource to
 * a Parquet file. If a FHIR sink address is provided, those resources are passed to that FHIR sink
 * too. The output PCollection contains, as keys, patient-ids extracted from the fetched resources,
 * with the value being the number of resources for that patient.
 */
public class FetchResources
    extends PTransform<PCollection<SearchSegmentDescriptor>, PCollection<KV<String, Integer>>> {

  private static final Logger log = LoggerFactory.getLogger(FetchResources.class);

  private static final Pattern PATIENT_REFERENCE = Pattern.compile(".*Patient/([^/]+)");

  @VisibleForTesting protected FetchSearchPageFn<SearchSegmentDescriptor> fetchSearchPageFn;

  FetchResources(FhirEtlOptions options, String stageIdentifier) {
    fetchSearchPageFn = new SearchFn(options, stageIdentifier);
  }

  @VisibleForTesting
  static String getSubjectPatientIdOrNull(Resource resource) {
    String patientId = null;
    // TODO: Instead of hard-coding `subject` here, use Patient Compartment:
    //  https://hl7.org/fhir/compartmentdefinition-patient.html
    Property subject = resource.getNamedProperty("subject");
    if (subject != null) {
      List<Base> values = subject.getValues();
      if (values.size() == 1 && values.get(0) instanceof Reference) {
        Reference reference = (Reference) values.get(0);
        // TODO: Find a more generic way to check if this is a reference to a Patient. With the
        // current OpenMRS setup, reference.getType() is null so we cannot rely on that.
        String refStr = reference.getReference();
        if (refStr != null) {
          Matcher matcher = PATIENT_REFERENCE.matcher(refStr);
          if (matcher.matches()) {
            patientId = matcher.group(1);
          }
        }
      }
      if (values.size() > 1) {
        log.warn(
            String.format(
                "Unexpected multiple values for subject of %s with id %s",
                resource.getResourceType(), resource.getId()));
      }
    }
    if (patientId == null) {
      log.warn("Ignoring subject of {} with id {}", resource.getResourceType(), resource.getId());
    }
    return patientId;
  }

  @Override
  public PCollection<KV<String, Integer>> expand(PCollection<SearchSegmentDescriptor> segments) {
    return segments.apply(ParDo.of(fetchSearchPageFn));
  }

  static class SearchFn extends FetchSearchPageFn<SearchSegmentDescriptor> {

    // This is to optimize counting number of patient resources. This will be initialized in
    // StartBundle; we don't need to `synchronized` access of it because each instance of the DoFn
    // class is accessed by a single thread.
    // https://beam.apache.org/documentation/programming-guide/#user-code-thread-compatibility
    private Map<String, Integer> patientCount = null;

    SearchFn(FhirEtlOptions options, String stageIdentifier) {
      super(options, stageIdentifier);
    }

    @StartBundle
    public void startBundle(StartBundleContext context) {
      patientCount = Maps.newHashMap();
    }

    private void incrementPatientResources(String patientId) {
      int current = patientCount.getOrDefault(patientId, 0);
      patientCount.put(patientId, current + 1);
    }

    @Override
    public void finishBundle(FinishBundleContext context) {
      for (Map.Entry<String, Integer> entry : patientCount.entrySet()) {
        context.output(
            KV.of(entry.getKey(), entry.getValue()), Instant.now(), GlobalWindow.INSTANCE);
      }
      super.finishBundle(context);
    }

    @ProcessElement
    public void processElement(@Element SearchSegmentDescriptor segment)
        throws IOException, SQLException, ViewApplicationException, ProfileException {
      String searchUrl = segment.searchUrl();
      log.info(
          String.format(
              "Fetching %d resources for state %s; URL= %s",
              segment.count(),
              this.stageIdentifier,
              searchUrl.substring(0, Math.min(200, searchUrl.length()))));
      long fetchStartTime = System.currentTimeMillis();
      Bundle pageBundle = fhirSearchUtil.searchByUrl(searchUrl, segment.count(), SummaryEnum.DATA);
      addFetchTime(System.currentTimeMillis() - fetchStartTime);
      processBundle(pageBundle);
      if (pageBundle != null && pageBundle.getEntry() != null) {
        for (BundleEntryComponent entry : pageBundle.getEntry()) {
          String patientId = getSubjectPatientIdOrNull(entry.getResource());
          if (patientId != null) {
            incrementPatientResources(patientId);
          }
        }
      }
    }
  }
}

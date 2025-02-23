/*
 * Copyright 2020-2024 Google LLC
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

import com.cerner.bunsen.exception.ProfileException;
import com.google.common.base.Strings;
import java.io.IOException;
import java.sql.SQLException;
import org.apache.beam.sdk.values.KV;
import org.hl7.fhir.r4.model.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessFhirBundleFn extends FetchSearchPageFn<String> {
  private static final Logger log = LoggerFactory.getLogger(ProcessFhirBundleFn.class);

  public ProcessFhirBundleFn(FhirEtlOptions options) {
    super(options, "ProcessFhirBundleFn");
  }

  @ProcessElement
  public void processElement(@Element String bundleJson, OutputReceiver<KV<String, Integer>> out)
      throws SQLException, ProfileException {
    if (Strings.isNullOrEmpty(bundleJson)) {
      log.warn("Received empty message from Kafka - skipping");
      return;
    }

    try {
      Bundle bundle = (Bundle) parser.parseResource(bundleJson);
      if (bundle == null) {
        log.warn("Failed to parse message as FHIR Bundle - skipping");
        return;
      }
      processBundle(bundle);
      out.output(KV.of("processed", 1));
    } catch (Exception e) {
      log.error("Error processing FHIR bundle from Kafka: {}", e.getMessage(), e);
    }
  }

  @Teardown
  public void teardown() throws IOException {
    super.teardown();
  }

  @Override
  public void finishBundle(FinishBundleContext context) {
    super.finishBundle(context);
  }
}

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

import ca.uhn.fhir.context.FhirVersionEnum;
import com.google.common.base.Preconditions;
import com.google.fhir.analytics.model.BulkExportHttpResponse;
import com.google.fhir.analytics.model.BulkExportResponse;
import java.io.IOException;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.http.HttpStatus;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class containing methods to operate on Bulk Export APIs in the FHIR server */
public class BulkExportUtil {

  private static final Logger logger = LoggerFactory.getLogger(BulkExportUtil.class.getName());

  private BulkExportApiClient bulkExportApiClient;

  BulkExportUtil(BulkExportApiClient bulkExportApiClient) {
    Preconditions.checkNotNull(bulkExportApiClient, "BulkExportApiClient cannot be null");
    this.bulkExportApiClient = bulkExportApiClient;
  }

  /**
   * This method triggers the Bulk export job in the FHIR server for the given resource types and
   * fhirVersionEnum. It then waits for the job to complete and returns the BulkExportResponse
   *
   * @param resourceTypes - the resource types to be downloaded
   * @param since - the fhir resources fetched should have updated timestamp greater than this
   * @param fhirVersionEnum - the fhir version of resource types
   * @return the BulkExportResponse
   * @throws IOException
   */
  public BulkExportResponse triggerBulkExport(
      List<String> resourceTypes, @Nullable String since, FhirVersionEnum fhirVersionEnum)
      throws IOException {
    Preconditions.checkState(!CollectionUtils.isEmpty(resourceTypes));
    Preconditions.checkNotNull(fhirVersionEnum);
    String contentLocationUrl =
        bulkExportApiClient.triggerBulkExportJob(resourceTypes, since, fhirVersionEnum);
    logger.info("Bulk Export has been started, contentLocationUrl={}", contentLocationUrl);
    // TODO: Persist the bulk export content location url so that it can be reused in the cases of
    //  application crash https://github.com/google/fhir-data-pipes/issues/1170
    return pollBulkExportJob(contentLocationUrl);
  }

  /**
   * This method keeps polling for the status of bulk export job until it is complete and returns
   * the BulkExportResponse once completed.
   */
  private BulkExportResponse pollBulkExportJob(String bulkExportStatusUrl) throws IOException {
    // TODO : Add timeout here to avoid infinite polling
    while (true) {
      BulkExportHttpResponse bulkExportHttpResponse =
          bulkExportApiClient.fetchBulkExportHttpResponse(bulkExportStatusUrl);
      if (bulkExportHttpResponse.httpStatus() == HttpStatus.SC_OK) {
        logger.info("Bulk Export job is complete");
        return bulkExportHttpResponse.bulkExportResponse();
      } else if (bulkExportHttpResponse.httpStatus() == HttpStatus.SC_ACCEPTED
          || bulkExportHttpResponse.httpStatus() == 429) { // Server is busy
        int retryAfterInMillis =
            bulkExportHttpResponse.retryAfter() > 0
                ? bulkExportHttpResponse.retryAfter() * 1000
                : 60000;
        logger.info(
            "Export job is still in progress, will check again in {} secs",
            retryAfterInMillis / 1000);
        try {
          Thread.sleep(retryAfterInMillis);
        } catch (InterruptedException e) {
          logger.error(
              "Caught InterruptedException; resetting interrupt flag and throwing "
                  + "RuntimeException! ",
              e);
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      } else {
        logger.error(
            "Error while checking the status of the bulk export job, httpStatus={}, response={}",
            bulkExportHttpResponse.httpStatus(),
            bulkExportHttpResponse);
        throw new IllegalStateException(
            "Error while checking the status of the bulk export job, please check logs for more"
                + " details");
      }
    }
  }
}

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

import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IClientInterceptor;
import ca.uhn.fhir.rest.client.api.IRestfulClientFactory;
import ca.uhn.fhir.rest.client.interceptor.BearerTokenAuthInterceptor;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.healthcare.v1.CloudHealthcare;
import com.google.api.services.healthcare.v1.CloudHealthcareScopes;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import org.apache.http.client.utils.URIBuilder;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Resource;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class extends the functionality of FhirStoreUtil for communication with Google Cloud
 * Healthcare API services. The GCP communication requires the request to be authenticated and
 * contain specific headers. This class takes care of these idiosyncracies and then leverages the
 * generic communication patterns in FhirStoreUtil.
 *
 * @see FhirStoreUtil
 */
class GcpStoreUtil extends FhirStoreUtil {

  private static final JsonFactory JSON_FACTORY = new GsonFactory();

  private static final NetHttpTransport HTTP_TRANSPORT = new NetHttpTransport();

  private static final Logger log = LoggerFactory.getLogger(GcpStoreUtil.class);

  // TODO replace this with local variables returned by `createClient()`; this does not need
  //  to be an instance variable.
  @Nullable private GoogleCredentials credential = null;

  protected GcpStoreUtil(String sinkUrl, IRestfulClientFactory clientFactory) {
    super(sinkUrl, "", "", clientFactory);
  }

  @Override
  @Nullable
  public MethodOutcome uploadResource(Resource resource) {
    try {
      updateFhirResource(sinkUrl, resource);
    } catch (Exception e) {
      log.error(String.format("Exception while sending to sink: %s", e.toString()));
    }
    return null;
  }

  @Override
  public Collection<MethodOutcome> uploadBundle(Bundle bundle) {
    try {
      // Initialize the client, which will be used to interact with the service.
      CloudHealthcare client = createClient();
      String uri = String.format("%sv1/%s/fhir", client.getRootUrl(), sinkUrl);
      URIBuilder uriBuilder = new URIBuilder(uri);
      log.info("Full URL is: {}", uriBuilder.build());

      return super.uploadBundle(
          uri,
          bundle,
          Collections.singletonList(
              new BearerTokenAuthInterceptor(credential.refreshAccessToken().getTokenValue())));
    } catch (IOException e) {
      log.error("IOException while using Google APIs: {}", e.toString(), e);
    } catch (URISyntaxException e) {
      log.error("URI syntax exception while using Google APIs: {}", e.toString(), e);
    }
    return null;
  }

  @Nullable
  protected MethodOutcome updateFhirResource(String fhirStoreName, Resource resource) {
    try {
      // Initialize the client, which will be used to interact with the service.
      CloudHealthcare client = createClient();
      String uri = String.format("%sv1/%s/fhir", client.getRootUrl(), fhirStoreName);
      URIBuilder uriBuilder = new URIBuilder(uri);
      log.info(String.format("Full URL is: %s", uriBuilder.build()));

      return super.updateFhirResource(
          uri,
          resource,
          Collections.<IClientInterceptor>singletonList(
              new BearerTokenAuthInterceptor(credential.refreshAccessToken().getTokenValue())));
    } catch (IOException e) {
      log.error(String.format("IOException while using Google APIs: %s", e.toString()));
    } catch (URISyntaxException e) {
      log.error(String.format("URI syntax exception while using Google APIs: %s", e.toString()));
    }
    return null;
  }

  private CloudHealthcare createClient() throws IOException {
    // Use Application Default Credentials (ADC) to authenticate the requests
    // For more information see https://cloud.google.com/docs/authentication/production
    credential =
        GoogleCredentials.getApplicationDefault()
            .createScoped(Collections.singleton(CloudHealthcareScopes.CLOUD_PLATFORM));

    // Create a HttpRequestInitializer, which will provide a baseline configuration to all requests.
    HttpRequestInitializer requestInitializer =
        request -> {
          new HttpCredentialsAdapter(credential).initialize(request);
          request.setConnectTimeout(60000); // 1 minute connect timeout
          request.setReadTimeout(60000); // 1 minute read timeout
        };

    // Build the client for interacting with the service.
    return new CloudHealthcare.Builder(HTTP_TRANSPORT, JSON_FACTORY, requestInitializer)
        .setApplicationName("openmrs-fhir-warehouse")
        .build();
  }
}

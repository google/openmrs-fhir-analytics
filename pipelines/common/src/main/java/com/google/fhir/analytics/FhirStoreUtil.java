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

import static com.google.common.base.Strings.isNullOrEmpty;

import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IClientInterceptor;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.api.IRestfulClientFactory;
import ca.uhn.fhir.rest.client.interceptor.AdditionalRequestHeadersInterceptor;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.instance.model.api.IBaseOperationOutcome;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Meta;
import org.hl7.fhir.r4.model.Resource;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FhirStoreUtil {

  private static final Logger log = LoggerFactory.getLogger(FhirStoreUtil.class);

  protected IRestfulClientFactory clientFactory;

  protected final String sinkUrl;

  private final String sinkUsername;

  private final String sinkPassword;

  private static final Pattern GCP_PATTERN =
      Pattern.compile("projects/(.*)/locations/(.*)/datasets/(.*)/fhirStores/(.*)");

  protected FhirStoreUtil(
      String sinkUrl,
      String sinkUsername,
      String sinkPassword,
      IRestfulClientFactory clientFactory) {
    this.clientFactory = clientFactory;
    this.sinkUrl = sinkUrl;
    this.sinkUsername = sinkUsername;
    this.sinkPassword = sinkPassword;
  }

  public static FhirStoreUtil createFhirStoreUtil(
      String sinkUrl, IRestfulClientFactory clientFactory) throws IllegalArgumentException {
    return createFhirStoreUtil(sinkUrl, "", "", clientFactory);
  }

  public static FhirStoreUtil createFhirStoreUtil(
      String sinkUrl, String sinkUsername, String sinkPassword, IRestfulClientFactory clientFactory)
      throws IllegalArgumentException {
    if (matchesGcpPattern(sinkUrl)) {
      return new GcpStoreUtil(sinkUrl, clientFactory);
    } else {
      return new FhirStoreUtil(sinkUrl, sinkUsername, sinkPassword, clientFactory);
    }
  }

  @Nullable
  public MethodOutcome uploadResource(Resource resource) {
    Collection<IClientInterceptor> interceptors = Collections.<IClientInterceptor>emptyList();

    if (!isNullOrEmpty(sinkUsername) && !isNullOrEmpty(sinkPassword)) {
      interceptors =
          Collections.<IClientInterceptor>singleton(
              new BasicAuthInterceptor(sinkUsername, sinkPassword));
    }

    return updateFhirResource(sinkUrl, resource, interceptors);
  }

  /**
   * Deletes a resource using the given resourceType and id
   *
   * @param resourceType the type of resource to be deleted
   * @param id the id of the resource to be deleted
   * @return the output result of delete operation
   */
  public MethodOutcome deleteResourceById(String resourceType, String id) {
    Collection<IClientInterceptor> interceptors = Collections.emptyList();
    if (!isNullOrEmpty(sinkUsername) && !isNullOrEmpty(sinkPassword)) {
      interceptors = Collections.singleton(new BasicAuthInterceptor(sinkUsername, sinkPassword));
    }
    IGenericClient client = createGenericClient(sinkUrl, interceptors);
    // Initialize the client, which will be used to interact with the service.
    MethodOutcome outcome = client.delete().resourceById(resourceType, id).execute();
    log.debug("FHIR resource deleted at" + sinkUrl + "? " + outcome.getCreated());
    return outcome;
  }

  public Collection<MethodOutcome> uploadBundle(Bundle bundle) {
    Collection<IClientInterceptor> interceptors = Collections.emptyList();

    if (!isNullOrEmpty(sinkUsername) && !isNullOrEmpty(sinkPassword)) {
      interceptors = Collections.singleton(new BasicAuthInterceptor(sinkUsername, sinkPassword));
    }

    return uploadBundle(sinkUrl, bundle, interceptors);
  }

  protected IGenericClient createGenericClient(
      String sinkUrl, Collection<IClientInterceptor> interceptors) {
    IGenericClient client = clientFactory.newGenericClient(sinkUrl);

    for (IClientInterceptor interceptor : interceptors) {
      client.registerInterceptor(interceptor);
    }

    AdditionalRequestHeadersInterceptor interceptor = new AdditionalRequestHeadersInterceptor();
    interceptor.addHeaderValue("Accept", "application/fhir+json");
    interceptor.addHeaderValue("Accept-Charset", "utf-8");
    interceptor.addHeaderValue("Content-Type", "application/fhir+json; charset=UTF-8");

    client.registerInterceptor(interceptor);

    return client;
  }

  protected Collection<MethodOutcome> uploadBundle(
      String sinkUrl, Bundle bundle, Collection<IClientInterceptor> interceptors) {
    IGenericClient client = createGenericClient(sinkUrl, interceptors);

    List<MethodOutcome> responses = new ArrayList<>(bundle.getTotal());

    Bundle transactionBundle = new Bundle();

    // use transaction so this is atomic, but we should probably consider if we need this overhead
    transactionBundle.setType(Bundle.BundleType.TRANSACTION);

    for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
      Resource resource = entry.getResource();
      removeSubsettedTag(resource.getMeta());

      Bundle.BundleEntryComponent component = transactionBundle.addEntry();
      component.setResource(resource);
      component
          .getRequest()
          .setUrl(resource.fhirType() + "/" + resource.getIdElement().getIdPart())
          .setMethod(Bundle.HTTPVerb.PUT);
    }

    Bundle outcomes = client.transaction().withBundle(transactionBundle).execute();

    if (outcomes != null) {
      for (Bundle.BundleEntryComponent response : outcomes.getEntry()) {
        Bundle.BundleEntryResponseComponent responseComponent = response.getResponse();
        MethodOutcome methodOutcome = new MethodOutcome();

        if (StringUtils.isNotBlank(responseComponent.getStatus())
            && responseComponent.getStatus().length() >= 3) {
          int status;
          try {
            status = Integer.parseUnsignedInt(responseComponent.getStatus().substring(0, 3));
            methodOutcome.setCreatedUsingStatusCode(status);
          } catch (NumberFormatException e) {
            // swallow
          }
        }

        Resource outcome = responseComponent.getOutcome();
        if (outcome instanceof IBaseOperationOutcome) {
          methodOutcome.setOperationOutcome((IBaseOperationOutcome) outcome);
        }

        responses.add(methodOutcome);
      }
    }

    return responses;
  }

  protected MethodOutcome updateFhirResource(
      String sinkUrl, @NonNull Resource resource, Collection<IClientInterceptor> interceptors) {
    IGenericClient client = createGenericClient(sinkUrl, interceptors);

    removeSubsettedTag(resource.getMeta());

    // Initialize the client, which will be used to interact with the service.
    MethodOutcome outcome =
        client.update().resource(resource).withId(resource.getId()).encodedJson().execute();

    log.debug("FHIR resource created at" + sinkUrl + "? " + outcome.getCreated());

    return outcome;
  }

  protected void removeSubsettedTag(Meta meta) {
    Coding tag =
        meta.getTag("http://terminology.hl7.org/CodeSystem/v3-ObservationValue", "SUBSETTED");
    if (tag != null) {
      meta.setTag(
          meta.getTag().stream().filter(t -> !Objects.equals(t, tag)).collect(Collectors.toList()));
    }
  }

  static boolean matchesGcpPattern(String gcpFhirStore) {
    return GCP_PATTERN.matcher(gcpFhirStore).matches();
  }

  public String getSinkUrl() {
    return this.sinkUrl;
  }
}

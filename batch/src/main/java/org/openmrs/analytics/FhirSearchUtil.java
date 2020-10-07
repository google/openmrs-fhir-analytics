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

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import ca.uhn.fhir.rest.api.SummaryEnum;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FhirSearchUtil {
	
	private static final Logger log = LoggerFactory.getLogger(FhirSearchUtil.class);
	
	private FhirStoreUtil fhirStoreUtil;
	
	private OpenmrsUtil openmrsUtil;
	
	FhirSearchUtil(FhirStoreUtil fhirStoreUtil, OpenmrsUtil openmrsUtil) {
		this.fhirStoreUtil = fhirStoreUtil;
		this.openmrsUtil = openmrsUtil;
	}
	
	Bundle searchForResource(String resourceType, int count, SummaryEnum summaryMode) {
		try {
			IGenericClient client = openmrsUtil.getSourceClient();
			
			Bundle result = client.search().forResource(resourceType).count(count).summaryMode(summaryMode)
			        .returnBundle(Bundle.class).execute();
			
			return result;
		}
		catch (Exception e) {
			log.error("Failed to search for resource " + resourceType + ";  " + "Exception: " + e);
		}
		return null;
	}
	
	Bundle searchByPage(String pageId, int count, int first, SummaryEnum summaryMode) {
		try {
			IGenericClient client = openmrsUtil.getSourceClient();
			
			Bundle result = client.search().byUrl(client.getServerBase() + "?" + pageId + "&_getpagesoffset=" + first)
			        .count(count).summaryMode(summaryMode).returnBundle(Bundle.class).execute();
			
			return result;
		}
		catch (Exception e) {
			log.error("Failed to search for page with id:" + pageId + ";  " + "Exception: " + e);
		}
		return null;
	}
	
	public String findBaseSearchUrl(Bundle searchBundle) {
		String searchLink = null;
		
		if (searchBundle.getLink(Bundle.LINK_NEXT) != null) {
			searchLink = searchBundle.getLink(Bundle.LINK_NEXT).getUrl();
		}
		
		if (searchLink == null) {
			throw new IllegalArgumentException(String.format("No proper link information in bundle %s", searchBundle));
		}
		
		try {
			URI searchUri = new URI(searchLink);
			NameValuePair pagesParam = null;
			for (NameValuePair pair : URLEncodedUtils.parse(searchUri, StandardCharsets.UTF_8)) {
				if (pair.getName().equals("_getpages")) {
					pagesParam = pair;
				}
			}
			if (pagesParam == null) {
				throw new IllegalArgumentException(
				        String.format("No _getpages parameter found in search link %s", searchLink));
			}
			return pagesParam.toString();
		}
		catch (URISyntaxException e) {
			throw new IllegalArgumentException(
			        String.format("Malformed link information with error %s in bundle %s", e.getMessage(), searchBundle));
		}
	}
	
	public void uploadBundleToCloud(Bundle bundle) {
		for (BundleEntryComponent entry : bundle.getEntry()) {
			Resource resource = entry.getResource();
			fhirStoreUtil.uploadResourceToCloud(resource.getIdElement().getResourceType(),
			    resource.getIdElement().getIdPart(), resource);
		}
	}
	
}

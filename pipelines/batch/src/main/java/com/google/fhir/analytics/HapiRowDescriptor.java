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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;
import lombok.Data;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.jspecify.annotations.Nullable;

@DefaultCoder(SerializableCoder.class)
@AutoValue
@Data
abstract class HapiRowDescriptor implements Serializable {

  private static final long serialVersionUID = 1L;

  static HapiRowDescriptor create(
      String resourceId,
      String forcedId,
      String resourceType,
      String lastUpdated,
      String fhirVersion,
      String resourceVersion,
      String jsonResource) {
    return new AutoValue_HapiRowDescriptor(
        resourceId,
        forcedId,
        resourceType,
        lastUpdated,
        fhirVersion,
        resourceVersion,
        jsonResource);
  }

  abstract String resourceId();

  @Nullable
  abstract String forcedId();

  abstract String resourceType();

  abstract String lastUpdated();

  abstract String fhirVersion();

  abstract String resourceVersion();

  abstract String jsonResource();

  // FHIR tags.
  List<ResourceTag> tags;
}

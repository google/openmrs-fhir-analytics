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

import io.debezium.data.Envelope.Operation;
import java.util.Map;
import org.apache.camel.component.debezium.DebeziumConstants;

public class DebeziumTestUtil {

  static Map<String, String> genExpectedBody() {
    return Map.of("uuid", "encounter_uuid");
  }

  static Map<String, String> genExpectedBodyWithoutUUid() {
    return Map.of("patient_id", "1");
  }

  static Map<String, Object> genExpectedHeaders(final Operation operation, final String tableName) {
    return Map.of(
        DebeziumConstants.HEADER_OPERATION,
        operation.code(),
        DebeziumConstants.HEADER_SOURCE_METADATA,
        Map.of("table", tableName));
  }
}

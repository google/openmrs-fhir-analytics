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
package com.google.fhir.analytics.enumeration;

/**
 * A set of enumerations to specify the mechanism for authenticating a client in the Client
 * Credentials flow.
 */
public enum ClientCredentialsAuthMechanism {
  /**
   * the client sends its client ID and client secret as part of the Authorization header in an HTTP
   * request. The Authorization header contains a Base64-encoded string of <code>
   * {URL-encoded-client-ID}:{URL-encoded-client-secret}</code>
   */
  BASIC,

  /**
   * The client sends its client ID and client secret as parameters in the body of the HTTP request.
   * This is similar to Basic Authentication, but instead of sending the credentials in the
   * Authorization header, they are sent in the request body.
   */
  BODY,

  /** TODO: Unimplemented yet, as the need for it has not arisen. */
  JWT
}

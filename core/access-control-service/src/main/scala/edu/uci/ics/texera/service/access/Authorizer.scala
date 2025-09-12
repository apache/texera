// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package edu.uci.ics.texera.service.access

import jakarta.ws.rs.core.{HttpHeaders, Response, UriInfo}

/**
 *
 * An interface for authorizing HTTP requests.
 * Implementations of this interface should provide the logic to authorize requests based on
 * the path and specific requirements of the destination service.
 *
 * @param uriInfo The UriInfo object containing information about the request URI that is
 *                forwarded by Envoy service. This URI is the original request URI from the
 *                client since Envoy forwards the original request URI in the path.
 * @param headers The HttpHeaders object containing the HTTP headers of the request that is
 *                forwarded by Envoy service. This includes headers such as Authorization.
 *
 */
trait Authorizer {
  def authorize(uriInfo: UriInfo, headers: HttpHeaders): Response
}
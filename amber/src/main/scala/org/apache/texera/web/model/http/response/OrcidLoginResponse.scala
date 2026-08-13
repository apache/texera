/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.web.model.http.response

/**
  * [[TokenIssueResponse]] plus the address to prefill the email prompt with.
  *
  * ORCID authenticates an iD and asserts no email, so the account behind `accessToken` may have
  * none yet and the frontend has to ask for one. `suggestedEmail` is whatever the ORCID record
  * publishes — a convenience for that form and nothing more. It is not a claim about the address:
  * the backend has not matched anything on it, and it is not in the token.
  *
  * Absent when the account already has an address, when the record publishes none, or when the
  * lookup failed.
  */
case class OrcidLoginResponse(accessToken: String, suggestedEmail: Option[String])

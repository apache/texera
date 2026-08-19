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
  * What a registration attempt produced: either an account and its token, or a request for the code
  * that was just mailed.
  *
  * `accessToken` is null exactly when `verificationRequired` is true, and it stays a nullable String
  * rather than an Option so the JSON keeps the `{ "accessToken": ... }` shape the frontend already
  * reads from [[TokenIssueResponse]].
  */
case class RegistrationResponse(accessToken: String, verificationRequired: Boolean)

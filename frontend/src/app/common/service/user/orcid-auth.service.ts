/**
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

import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { HttpClient } from "@angular/common/http";
import { AppSettings } from "../../app-setting";

/**
 * What the login page needs to send the browser to ORCID: the registered client id, and the
 * authorize endpoint of whichever ORCID deployment this backend is configured against
 * (sandbox or production). Both come from the server so the two can never disagree.
 */
export interface OrcidConfig {
  clientId: string;
  authorizeUrl: string;
}

/**
 * sessionStorage key holding the CSRF `state` value across the ORCID round trip. Written by the
 * login page before it redirects, read back by the callback page — shared here so the two sides
 * cannot drift apart.
 */
export const ORCID_STATE_KEY = "orcid_state";

@Injectable({
  providedIn: "root",
})
export class OrcidAuthService {
  constructor(private http: HttpClient) {}

  getConfig(): Observable<OrcidConfig> {
    return this.http.get<OrcidConfig>(`${AppSettings.getApiEndpoint()}/auth/orcid/config`);
  }
}

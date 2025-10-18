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
import { AuthService } from "../../../common/service/user/auth.service";
import { GuiConfigService } from "../../../common/service/gui-config.service";

@Injectable({
  providedIn: "root",
})
export class ComputingUnitSshService {
  private static readonly SSH_ENDPOINT = "/wsapi/cu-ssh";

  constructor(private config: GuiConfigService) {}

  /**
   * Generate the ttyd terminal URL for iframe
   */
  public getComputingUnitSshUrl(uid: number, cuid: number): string {
    const baseUrl = ComputingUnitSshService.SSH_ENDPOINT;
    const params = new URLSearchParams({
      uid: uid.toString(),
      cuid: cuid.toString(),
    });

    const accessToken = AuthService.getAccessToken();
    if (accessToken) {
      params.append("access-token", accessToken);
    }

    return `${baseUrl}?${params.toString()}`;
  }
}

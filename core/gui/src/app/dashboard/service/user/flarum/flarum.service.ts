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
import { HttpClient } from "@angular/common/http";
import { UserService } from "../../../../common/service/user/user.service";

@Injectable({
  providedIn: "root",
})
export class FlarumService {
  constructor(
    private http: HttpClient,
    private userService: UserService
  ) {}

  register() {
    const user = this.userService.getCurrentUser();
    return this.http.post(
      "forum/api/users",
      {
        data: {
          attributes: { username: user!.email.split("@")[0] + user!.uid, email: user!.email, password: user!.googleId },
        },
      },
      { headers: { Authorization: "Token hdebsyxiigyklxgsqivyswwiisohzlnezzzzzzzz;userId=1" } }
    );
  }

  auth() {
    const user = this.userService.getCurrentUser();
    return this.http.post("forum/api/token", { identification: user!.email, password: user!.googleId, remember: "1" });
  }
}

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
import { ActivatedRouteSnapshot, CanActivate, Router } from "@angular/router";
import { UserService } from "./user.service";
import { USER_WORKFLOW } from "../../../app-routing.constant";

/**
 * The mirror image of AuthGuardService: keeps an already-signed-in user off the login page,
 * which would otherwise offer them a sign-in form they have no use for.
 *
 * Sends them wherever they were originally headed if a returnUrl survived the round trip
 * (the auth guard and the 401 interceptor both attach one), and to their workflows otherwise.
 */
@Injectable()
export class GuestGuardService implements CanActivate {
  constructor(
    private userService: UserService,
    private router: Router
  ) {}

  canActivate(route: ActivatedRouteSnapshot): boolean {
    if (!this.userService.isLogin()) {
      return true;
    }
    this.router.navigateByUrl(route.queryParams["returnUrl"] || USER_WORKFLOW);
    return false;
  }
}

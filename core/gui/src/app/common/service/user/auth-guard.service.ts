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
import { Router, CanActivate, RouterStateSnapshot, ActivatedRouteSnapshot } from "@angular/router";
import { UserService } from "./user.service";
import { environment } from "../../../../environments/environment";
import { DASHBOARD_ABOUT } from "../../../app-routing.constant";

/**
 * AuthGuardService is a service can tell the router whether
 * it should allow navigation to a requested route.
 */
@Injectable()
export class AuthGuardService implements CanActivate {
  constructor(
    private userService: UserService,
    private router: Router
  ) {}
  canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): boolean {
    if (this.userService.isLogin() || !environment.userSystemEnabled) {
      return true;
    } else {
      this.router.navigate([DASHBOARD_ABOUT], { queryParams: { returnUrl: state.url === "/" ? null : state.url } });
      return false;
    }
  }
}

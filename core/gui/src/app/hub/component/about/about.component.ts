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

import { Component, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { UserService } from "src/app/common/service/user/user.service";
import { BehaviorSubject } from "rxjs";
import { GuiConfigService } from "../../../common/service/gui-config.service";

@UntilDestroy()
@Component({
  selector: "texera-about",
  templateUrl: "./about.component.html",
  styleUrls: ["./about.component.scss"],
})
export class AboutComponent implements OnInit {
  isLogin$ = new BehaviorSubject<boolean>(false); // control the visibility of the local login component

  constructor(
    private userService: UserService,
    protected config: GuiConfigService
  ) {}

  ngOnInit() {
    this.isLogin$.next(this.userService.isLogin());
    // Subscribe to user changes
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(user => {
        this.isLogin$.next(user !== undefined);
      });
  }
}

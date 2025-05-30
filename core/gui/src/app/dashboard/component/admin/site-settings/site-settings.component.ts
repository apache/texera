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
import { SiteSettingsService, SiteSetting } from "../../../service/admin/site-settings/admin-site-settings.service";
import { NzMessageService } from "ng-zorro-antd/message";

@Component({
  selector: "texera-site-settings",
  templateUrl: "./site-settings.component.html",
  styleUrls: ["./site-settings.component.scss"],
})
export class SiteSettingsComponent implements OnInit {
  settings: SiteSetting[] = [];

  constructor(
    private settingsSvc: SiteSettingsService,
    private message: NzMessageService
  ) {}

  ngOnInit(): void {
    // Use the correct injected service and method
    this.settingsSvc.getAllSettings().subscribe({
      next: data => {
        this.settings = data;
      },
      error: (err: any) => {
        this.message.error("Failed to load site settings. Please try again later.");
      },
    });
  }
}

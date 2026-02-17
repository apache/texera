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

import { Component, Inject, Input, TemplateRef, ViewChild } from "@angular/core";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";

@Component({
  selector: "texera-registration-request-modal",
  templateUrl: "./registration-request-modal.component.html",
  styleUrls: ["./registration-request-modal.component.scss"],
})

// Component for registration form modal
export class RegistrationRequestModalComponent {
  name = "";
  email = "";

  affiliation = "";
  reason = "";

  @ViewChild("modalTitle", { static: true })
  modalTitle!: TemplateRef<any>;

  constructor(@Inject(NZ_MODAL_DATA) public data: { uid: number; email: string; name: string }) {
    this.name = data?.name ?? "";
    this.email = data?.email ?? "";
  }

  getValues() {
    return {
      affiliation: (this.affiliation ?? "").trim(),
      reason: (this.reason ?? "").trim(),
    };
  }
}

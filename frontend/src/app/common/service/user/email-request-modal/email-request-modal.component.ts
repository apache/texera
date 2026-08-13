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

import { Component, Inject, TemplateRef, ViewChild } from "@angular/core";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { NzInputDirective } from "ng-zorro-antd/input";
import { FormsModule } from "@angular/forms";

/**
 * Asks a signed-in user for the email address their account does not have.
 *
 * ORCID authenticates an iD and asserts no address, so an ORCID-only account arrives here with
 * `email` unset — and email is what the rest of the product addresses a user by: dataset storage
 * paths are built from it and every access grant names one. So this is not a profile nicety; the
 * account cannot be shared with or own a dataset until it is answered.
 *
 * `suggestedEmail` prefills the field from what the ORCID record publishes, which is a convenience
 * and not a verified fact — the user can replace it.
 */
@Component({
  selector: "texera-email-request-modal",
  templateUrl: "./email-request-modal.component.html",
  styleUrls: ["./email-request-modal.component.scss"],
  imports: [NzInputDirective, FormsModule],
})
export class EmailRequestModalComponent {
  name = "";
  email = "";

  @ViewChild("modalTitle", { static: true })
  modalTitle!: TemplateRef<any>;

  constructor(@Inject(NZ_MODAL_DATA) public data: { name: string; suggestedEmail?: string }) {
    this.name = data?.name ?? "";
    this.email = data?.suggestedEmail ?? "";
  }

  getValues() {
    return { email: (this.email ?? "").trim() };
  }
}

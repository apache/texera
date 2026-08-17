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
 * No account should reach this: every path that writes a credential supplies an address, and the
 * one that used not to (`AdminUserResource.addUser`) no longer writes a credential at all. This
 * covers the rows older deployments still carry from it, and is the remedy a sign-in method that
 * authenticates someone without asserting an address would reuse.
 *
 * Email is what the rest of the product addresses a user by — dataset storage paths are built
 * from it and every access grant names one — so this is not a profile nicety. `AuthService`
 * hands out no `User` at all until it is answered.
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

  constructor(@Inject(NZ_MODAL_DATA) public data: { name: string }) {
    this.name = data?.name ?? "";
  }

  getValues() {
    return { email: (this.email ?? "").trim() };
  }
}

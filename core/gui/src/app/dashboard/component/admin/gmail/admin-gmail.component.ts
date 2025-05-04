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
import { GmailService } from "../../../../common/service/gmail/gmail.service";
import { FormBuilder, FormGroup, Validators } from "@angular/forms";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
@UntilDestroy()
@Component({
  selector: "texera-gmail",
  templateUrl: "./admin-gmail.component.html",
  styleUrls: ["./admin-gmail.component.scss"],
})
export class AdminGmailComponent implements OnInit {
  public validateForm!: FormGroup;
  public email: String | undefined;
  constructor(
    private gmailAuthService: GmailService,
    private formBuilder: FormBuilder
  ) {}

  ngOnInit(): void {
    this.validateForm = this.formBuilder.group({
      email: [null, [Validators.email, Validators.required]],
      subject: [null, [Validators.required]],
      content: [null, [Validators.required]],
    });
    this.getSenderEmail();
  }

  getSenderEmail() {
    this.gmailAuthService
      .getSenderEmail()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: email => (this.email = email),
        error: (err: unknown) => {
          this.email = undefined;
          console.log(err);
        },
      });
  }
  sendTestEmail(): void {
    this.gmailAuthService.sendEmail(
      this.validateForm.value.subject,
      this.validateForm.value.content,
      this.validateForm.value.email
    );
  }
}

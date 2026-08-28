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
import { FormGroup, FormsModule } from "@angular/forms";
import { FormlyFieldConfig, FormlyModule } from "@ngx-formly/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { HttpErrorResponse } from "@angular/common/http";
import { NzModalRef } from "ng-zorro-antd/modal";
import { NzSpinComponent } from "ng-zorro-antd/spin";
import { NgClass } from "@angular/common";
import { NzSwitchComponent } from "ng-zorro-antd/switch";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";

import {
  MODEL_FORMATS,
  MODEL_FRAMEWORKS,
  ModelService,
  validateModelName,
} from "../../../../service/user/model/model.service";
import { Model } from "../../../../../common/type/model";
import { NotificationService } from "../../../../../common/service/notification/notification.service";

export function sanitizeModelName(name: string): string {
  return name
    .trimStart()
    .replace(/[^a-zA-Z0-9_-]+/g, "-")
    .toLowerCase();
}

@UntilDestroy()
@Component({
  selector: "texera-user-model-creator",
  templateUrl: "./user-model-creator.component.html",
  styleUrls: ["./user-model-creator.component.scss"],
  imports: [
    NzSpinComponent,
    NgClass,
    FormlyModule,
    NzSwitchComponent,
    FormsModule,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
  ],
})
export class UserModelCreatorComponent implements OnInit {
  public form: FormGroup = new FormGroup({});
  formModel: any = {};
  fields: FormlyFieldConfig[] = [];

  isModelPublic: boolean = false;
  isModelDownloadable: boolean = false;
  isModelNameSanitized: boolean = false;
  isCreating: boolean = false;

  constructor(
    private modalRef: NzModalRef,
    private modelService: ModelService,
    private notificationService: NotificationService
  ) {}

  ngOnInit() {
    this.setFormFields();
    this.isModelNameSanitized = false;
  }

  private setFormFields() {
    this.fields = [
      {
        key: "name",
        type: "input",
        templateOptions: {
          label: "Name",
          required: true,
        },
      },
      {
        key: "description",
        type: "input",
        defaultValue: "",
        templateOptions: {
          label: "Description",
        },
      },
      // Options mirror ModelResource's whitelists, which reject anything else with a 400.
      {
        key: "framework",
        type: "select",
        defaultValue: MODEL_FRAMEWORKS[0],
        templateOptions: {
          label: "Framework",
          required: true,
          options: MODEL_FRAMEWORKS.map(value => ({ label: value, value })),
        },
      },
      {
        key: "format",
        type: "select",
        defaultValue: MODEL_FORMATS[0],
        templateOptions: {
          label: "Format",
          required: true,
          options: MODEL_FORMATS.map(value => ({ label: value, value })),
        },
      },
    ];
  }

  private triggerValidation() {
    Object.keys(this.form.controls).forEach(field => {
      this.form.get(field)?.markAsTouched({ onlySelf: true });
    });
  }

  onClickCancel() {
    this.modalRef.close(null);
  }

  onClickCreate() {
    this.triggerValidation();

    if (!this.form.valid) {
      return;
    }

    const originalName = this.form.get("name")?.value as string;
    const sanitizedName = sanitizeModelName(originalName);
    this.isModelNameSanitized = sanitizedName !== originalName;

    // A name of only punctuation or whitespace sanitizes to "", which passes `required` but the
    // backend rejects. Check the sanitized name against the same rule the rename path uses.
    const nameError = sanitizedName === "" ? "Model name cannot be empty." : validateModelName(sanitizedName);
    if (nameError) {
      this.notificationService.error(nameError);
      return;
    }

    const model: Model = {
      name: sanitizedName,
      description: this.form.get("description")?.value ?? "",
      framework: this.form.get("framework")?.value,
      format: this.form.get("format")?.value,
      isPublic: this.isModelPublic,
      isDownloadable: this.isModelDownloadable,
      mid: undefined,
      ownerUid: undefined,
      repositoryName: undefined,
      creationTime: undefined,
      coverImage: undefined,
    };

    this.isCreating = true;
    this.modelService
      .createModel(model)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: res => {
          const msg = this.isModelNameSanitized
            ? `Model '${originalName}' was sanitized to '${sanitizedName}' and created successfully.`
            : `Model '${sanitizedName}' created successfully.`;
          this.notificationService.success(msg);
          this.isCreating = false;
          this.modalRef.close(res);
        },
        error: (res: unknown) => {
          const err = res as HttpErrorResponse;
          this.notificationService.error(`Model ${sanitizedName} creation failed: ${err.error?.message}`);
          // Left open on purpose, so a rejected name can be corrected rather than retyped.
          this.isCreating = false;
        },
      });
  }

  onPublicStatusChange(newValue: boolean): void {
    this.isModelPublic = newValue;
  }

  onDownloadableStatusChange(newValue: boolean): void {
    this.isModelDownloadable = newValue;
  }
}

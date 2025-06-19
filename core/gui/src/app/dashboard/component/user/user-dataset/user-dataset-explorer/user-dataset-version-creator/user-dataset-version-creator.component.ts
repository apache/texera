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

import { Component, EventEmitter, inject, Input, OnInit, Output } from "@angular/core";
import { FormBuilder, FormGroup, Validators } from "@angular/forms";
import { FormlyFieldConfig } from "@ngx-formly/core";
import { DatasetService } from "../../../../../service/user/dataset/dataset.service";
import { Dataset } from "../../../../../../common/type/dataset";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NotificationService } from "../../../../../../common/service/notification/notification.service";
import { HttpErrorResponse } from "@angular/common/http";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";

@UntilDestroy()
@Component({
  selector: "texera-user-dataset-version-creator",
  templateUrl: "./user-dataset-version-creator.component.html",
  styleUrls: ["./user-dataset-version-creator.component.scss"],
})
export class UserDatasetVersionCreatorComponent implements OnInit {
  readonly isCreatingVersion: boolean = inject(NZ_MODAL_DATA).isCreatingVersion;

  readonly did: number = inject(NZ_MODAL_DATA)?.did ?? undefined;

  isCreateButtonDisabled: boolean = false;

  public form: FormGroup = new FormGroup({});
  model: any = {};
  fields: FormlyFieldConfig[] = [];
  isDatasetPublic: boolean = false;

  // used when creating the dataset
  isDatasetNameSanitized: boolean = false;

  // boolean to control if is uploading
  isCreating: boolean = false;

  constructor(
    private modalRef: NzModalRef,
    private datasetService: DatasetService,
    private notificationService: NotificationService,
    private formBuilder: FormBuilder
  ) {}

  ngOnInit() {
    this.setFormFields();
    this.isDatasetNameSanitized = false;
  }

  private setFormFields() {
    this.fields = this.isCreatingVersion
      ? [
          // Fields when isCreatingVersion is true
          {
            key: "versionDescription",
            type: "input",
            defaultValue: "",
            templateOptions: {
              label: "Describe the new version",
              required: false,
            },
          },
        ]
      : [
          // Fields when isCreatingVersion is false
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
          {
            key: "contributors",
            type: "array",
            templateOptions: {
              label: "Add a New Contributor",
            },
            fieldArray: {
              className: "contributor-fields",
              fieldGroup: [
                {
                  key: "name",
                  type: "input",
                  templateOptions: {
                    label: "Contributor's Name",
                    placeholder: "Contributor's Name",
                    required: true,
                  },
                },
                {
                  key: "creator",
                  type: "checkbox",
                  templateOptions: {
                    label: "Creator",
                  },
                },
                {
                  key: "role",
                  type: "select",
                  templateOptions: {
                    label: "Contributor Role",
                    options: [
                      { label: "Researcher", value: "RESEARCHER" },
                      { label: "Principal Investigator (PI)", value: "PRINCIPAL INVESTIGATOR" },
                      { label: "Project Member", value: "PROJECT MEMBER" },
                      { label: "Other", value: "OTHER" },
                    ],
                  },
                },
                {
                  key: "affiliation",
                  type: "input",
                  templateOptions: {
                    label: "Department",
                    placeholder: "Department",
                  },
                },
                {
                  key: "email",
                  type: "input",
                  templateOptions: {
                    label: "Email",
                    placeholder: "Email",
                    type: "email",
                  },
                },
              ],
            },
          },
        ];
  }
  get formControlNames(): string[] {
    return Object.keys(this.form.controls);
  }

  datasetNameSanitization(datasetName: string): string {
    // Remove leading spaces
    let sanitizedDatasetName = datasetName.trimStart();

    // Replace all characters that are not letters (a-z, A-Z), numbers (0-9) with a short dash "-"
    sanitizedDatasetName = sanitizedDatasetName.replace(/[^a-zA-Z0-9]+/g, "-");

    // Lower-case everything
    sanitizedDatasetName = sanitizedDatasetName.toLowerCase();

    // Track whether user’s input be changed
    if (sanitizedDatasetName !== datasetName) {
      this.isDatasetNameSanitized = true;
    }

    return sanitizedDatasetName;
  }

  private triggerValidation() {
    Object.keys(this.form.controls).forEach(field => {
      const control = this.form.get(field);
      control?.markAsTouched({ onlySelf: true });
    });
  }

  onClickCancel() {
    this.modalRef.close(null);
  }

  onClickCreate() {
    // check if the form is valid
    this.triggerValidation();

    if (!this.form.valid) {
      return; // Stop further execution if the form is not valid
    }

    this.isCreating = true;
    if (this.isCreatingVersion && this.did) {
      const versionName = this.form.get("versionDescription")?.value;
      this.datasetService
        .createDatasetVersion(this.did, versionName)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: res => {
            this.notificationService.success("Version Created");
            this.isCreating = false;
            // creation succeed, emit created version
            this.modalRef.close(res);
          },
          error: (res: unknown) => {
            const err = res as HttpErrorResponse;
            this.notificationService.error(`Version creation failed: ${err.error.message}`);
            this.isCreating = false;
            // creation failed, emit null value
            this.modalRef.close(null);
          },
        });
    } else {
      // capture original and sanitized names
      const originalName = this.form.get("name")?.value as string;
      const sanitizedName = this.datasetNameSanitization(originalName);

      const ds: Dataset = {
        name: sanitizedName,
        description: this.form.get("description")?.value,
        isPublic: this.isDatasetPublic,
        did: undefined,
        ownerUid: undefined,
        storagePath: undefined,
        creationTime: undefined,
      };
      this.datasetService
        .createDataset(ds)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: res => {
            const msg = this.isDatasetNameSanitized
              ? `Dataset '${originalName}' was sanitized to '${sanitizedName}' and created successfully.`
              : `Dataset '${sanitizedName}' created successfully.`;

            this.notificationService.success(msg);
            this.isCreating = false;
            // if creation succeed, emit the created dashboard dataset
            this.modalRef.close(res);
          },
          error: (res: unknown) => {
            const err = res as HttpErrorResponse;
            this.notificationService.error(`Dataset ${ds.name} creation failed: ${err.error.message}`);
            this.isCreating = false;
            // if creation failed, emit null value
            this.modalRef.close(null);
          },
        });
    }
  }

  onPublicStatusChange(newValue: boolean): void {
    // Handle the change in dataset public status
    this.isDatasetPublic = newValue;
  }
}

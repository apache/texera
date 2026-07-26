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

import { Component } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { FieldType, FieldTypeConfig, FormlyModule } from "@ngx-formly/core";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { of, throwError } from "rxjs";
import { UserDatasetVersionCreatorComponent } from "./user-dataset-version-creator.component";
import { DatasetService } from "../../../../../service/user/dataset/dataset.service";
import { NotificationService } from "../../../../../../common/service/notification/notification.service";
import { commonTestProviders } from "../../../../../../common/testing/test-utils";

// Minimal stand-in for the "input" formly type (the real one comes from the app's
// UI formly module). Registering it lets formly materialize the declared fields
// into real form controls — with their validators — on render.
@Component({ template: "", standalone: true })
class StubFormlyInputComponent extends FieldType<FieldTypeConfig> {}

describe("UserDatasetVersionCreatorComponent", () => {
  let modalClose: ReturnType<typeof vi.fn>;
  let createDataset: ReturnType<typeof vi.fn>;
  let createDatasetVersion: ReturnType<typeof vi.fn>;
  let notifySuccess: ReturnType<typeof vi.fn>;
  let notifyError: ReturnType<typeof vi.fn>;

  async function createFixture(modalData: {
    isCreatingVersion: boolean;
    did?: number;
  }): Promise<ComponentFixture<UserDatasetVersionCreatorComponent>> {
    modalClose = vi.fn();
    createDataset = vi.fn();
    createDatasetVersion = vi.fn();
    notifySuccess = vi.fn();
    notifyError = vi.fn();

    await TestBed.configureTestingModule({
      imports: [
        UserDatasetVersionCreatorComponent,
        BrowserAnimationsModule,
        FormsModule,
        ReactiveFormsModule,
        FormlyModule.forRoot({ types: [{ name: "input", component: StubFormlyInputComponent }] }),
        HttpClientTestingModule,
      ],
      providers: [
        { provide: NZ_MODAL_DATA, useValue: modalData },
        { provide: NzModalRef, useValue: { close: modalClose } },
        { provide: DatasetService, useValue: { createDataset, createDatasetVersion } },
        { provide: NotificationService, useValue: { success: notifySuccess, error: notifyError } },
        ...commonTestProviders,
      ],
    }).compileComponents();

    return TestBed.createComponent(UserDatasetVersionCreatorComponent);
  }

  it("should create and render the formly form (dataset-creation mode)", async () => {
    const fixture = await createFixture({ isCreatingVersion: false });
    fixture.detectChanges();

    expect(fixture.componentInstance).toBeTruthy();
    // Formly materializes the declared fields into real form controls on render.
    expect(fixture.componentInstance.form.contains("name")).toBe(true);
  });

  it("ngOnInit builds the version-description field when creating a version", async () => {
    const fixture = await createFixture({ isCreatingVersion: true, did: 5 });
    fixture.detectChanges();

    expect(fixture.componentInstance.fields.map(f => f.key)).toEqual(["versionDescription"]);
  });

  it("ngOnInit builds the name + description fields when creating a dataset", async () => {
    const fixture = await createFixture({ isCreatingVersion: false });
    fixture.detectChanges();

    expect(fixture.componentInstance.fields.map(f => f.key)).toEqual(["name", "description"]);
  });

  it("onClickCreate does nothing when the required name is empty (invalid form)", async () => {
    const fixture = await createFixture({ isCreatingVersion: false });
    fixture.detectChanges();
    const component = fixture.componentInstance;

    expect(component.form.valid).toBe(false); // name is required and empty
    component.onClickCreate();

    expect(createDataset).not.toHaveBeenCalled();
    expect(modalClose).not.toHaveBeenCalled();
  });

  it("onClickCreate creates a dataset with a sanitized name and closes the modal on success", async () => {
    const fixture = await createFixture({ isCreatingVersion: false });
    fixture.detectChanges();
    const component = fixture.componentInstance;
    component.form.get("name")?.setValue("My Dataset");
    component.form.get("description")?.setValue("desc");
    component.onPublicStatusChange(true);
    component.onDownloadableStatusChange(true);
    createDataset.mockReturnValue(of({ did: 7 }));

    component.onClickCreate();

    expect(createDataset).toHaveBeenCalledTimes(1);
    expect(createDataset.mock.calls[0][0]).toMatchObject({
      name: "my-dataset",
      description: "desc",
      isPublic: true,
      isDownloadable: true,
    });
    expect(notifySuccess).toHaveBeenCalled();
    expect(modalClose).toHaveBeenCalledWith({ did: 7 });
  });

  it("onClickCreate creates a dataset version and closes the modal on success", async () => {
    const fixture = await createFixture({ isCreatingVersion: true, did: 42 });
    fixture.detectChanges();
    const component = fixture.componentInstance;
    component.form.get("versionDescription")?.setValue("v2 notes");
    createDatasetVersion.mockReturnValue(of({ dvid: 1 }));

    component.onClickCreate();

    expect(createDatasetVersion).toHaveBeenCalledWith(42, "v2 notes");
    expect(notifySuccess).toHaveBeenCalledWith("Version Created");
    expect(modalClose).toHaveBeenCalledWith({ dvid: 1 });
  });

  it("onClickCreate notifies and closes with null when version creation fails", async () => {
    const fixture = await createFixture({ isCreatingVersion: true, did: 42 });
    fixture.detectChanges();
    const component = fixture.componentInstance;
    component.form.get("versionDescription")?.setValue("v2");
    createDatasetVersion.mockReturnValue(throwError(() => ({ error: { message: "boom" } })));

    component.onClickCreate();

    expect(notifyError).toHaveBeenCalled();
    expect(modalClose).toHaveBeenCalledWith(null);
  });

  it("onClickCancel closes the modal with null", async () => {
    const fixture = await createFixture({ isCreatingVersion: false });
    fixture.detectChanges();

    fixture.componentInstance.onClickCancel();

    expect(modalClose).toHaveBeenCalledWith(null);
  });

  it("datasetNameSanitization lowercases, dashes non-alphanumerics, and flags the change", async () => {
    const fixture = await createFixture({ isCreatingVersion: false });
    const component = fixture.componentInstance;

    expect(component.datasetNameSanitization("  My Data Set!! ")).toBe("my-data-set-");
    expect(component.isDatasetNameSanitized).toBe(true);
  });

  it("switch handlers toggle the public and downloadable flags", async () => {
    const fixture = await createFixture({ isCreatingVersion: false });
    const component = fixture.componentInstance;

    component.onPublicStatusChange(true);
    component.onDownloadableStatusChange(true);

    expect(component.isDatasetPublic).toBe(true);
    expect(component.isDatasetDownloadable).toBe(true);
  });
});

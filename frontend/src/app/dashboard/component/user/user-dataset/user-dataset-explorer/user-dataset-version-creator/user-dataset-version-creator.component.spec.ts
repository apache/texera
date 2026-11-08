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
import { By } from "@angular/platform-browser";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { FieldType, FieldTypeConfig, FormlyModule } from "@ngx-formly/core";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { of, Subject, throwError } from "rxjs";
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
        FormlyModule.forRoot({
          types: ["input", "checkbox", "textarea", "array"].map(name => ({
            name,
            component: StubFormlyInputComponent,
          })),
        }),
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

    expect(fixture.componentInstance.fields.map(f => f.key)).toEqual(["name", "description", "contributors"]);
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

  it("onClickCreate reports both the typed and the sanitized name when the name had to be rewritten", async () => {
    const fixture = await createFixture({ isCreatingVersion: false });
    fixture.detectChanges();
    const component = fixture.componentInstance;
    // Padded on both sides, and the only junk at the end is the whitespace itself.
    // That is what separates the four candidate trims: only stripping the leading
    // side leaves a trailing run for the dash rule to collapse, so "my-dataset-"
    // distinguishes it from "my-dataset" (trim), "-my-dataset" (trimEnd) and
    // "-my-dataset-" (no trim at all).
    component.form.get("name")?.setValue("  My Dataset ");
    createDataset.mockReturnValue(of({ did: 7 }));

    component.onClickCreate();

    expect(createDataset.mock.calls[0][0]).toMatchObject({ name: "my-dataset-" });
    // The writer typed one name and got another, so the toast has to name both —
    // and in that order, or it reads as the rewrite having gone the other way.
    expect(notifySuccess).toHaveBeenCalledWith(
      "Dataset '  My Dataset ' was sanitized to 'my-dataset-' and created successfully."
    );
    // The spinner comes back down on the success path too, or the form stays
    // locked behind a request that has already returned.
    expect(component.isCreating).toBe(false);
  });

  it("onClickCreate reports a name that needed no rewriting just once", async () => {
    const fixture = await createFixture({ isCreatingVersion: false });
    fixture.detectChanges();
    const component = fixture.componentInstance;
    // Already lower-case, alphanumeric and hyphenated: sanitization is a no-op.
    component.form.get("name")?.setValue("already-clean-1");
    createDataset.mockReturnValue(of({ did: 7 }));

    component.onClickCreate();

    expect(component.isDatasetNameSanitized).toBe(false);
    expect(notifySuccess).toHaveBeenCalledWith("Dataset 'already-clean-1' created successfully.");
  });

  it("onClickCreate notifies with the stored name and closes with null when dataset creation fails", async () => {
    const fixture = await createFixture({ isCreatingVersion: false });
    fixture.detectChanges();
    const component = fixture.componentInstance;
    component.form.get("name")?.setValue("My Dataset");
    createDataset.mockReturnValue(throwError(() => ({ error: { message: "duplicate name" } })));

    component.onClickCreate();

    // The failure names the dataset as it would have been stored, and the backend's
    // own reason, so the writer can tell a clash from a rejected name.
    expect(notifyError).toHaveBeenCalledWith("Dataset my-dataset creation failed: duplicate name");
    // The spinner has to come back down, or the form is left permanently locked.
    expect(component.isCreating).toBe(false);
    expect(modalClose).toHaveBeenCalledWith(null);
  });

  it("onClickCreate locks the form while the creation request is still outstanding", async () => {
    const fixture = await createFixture({ isCreatingVersion: false });
    fixture.detectChanges();
    const component = fixture.componentInstance;
    component.form.get("name")?.setValue("My Dataset");
    // Never emits: the request is still in flight when we look at the flag.
    createDataset.mockReturnValue(new Subject().asObservable());

    component.onClickCreate();

    // `isCreating` drives both the spinner and the disabled state of the create
    // button, so without it being raised a second click fires a second create.
    expect(component.isCreating).toBe(true);
    expect(modalClose).not.toHaveBeenCalled();
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
    expect(component.isCreating).toBe(false);
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
    expect(component.isCreating).toBe(false);
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

  /**
   * Every test above calls the component's methods directly, so the template's
   * own wiring never ran: which button submits and which cancels, and which
   * nz-switch feeds which flag. These go through the rendered controls, so
   * swapping the two switches or the two buttons fails here.
   */
  describe("rendered controls", () => {
    /** Renders in dataset-creation mode (the only mode that shows the switches). */
    async function renderCreator(): Promise<ComponentFixture<UserDatasetVersionCreatorComponent>> {
      const fixture = await createFixture({ isCreatingVersion: false });
      fixture.detectChanges();
      fixture.componentInstance.form.get("name")?.setValue("My Dataset");
      fixture.detectChanges();
      return fixture;
    }

    const switches = (fixture: ComponentFixture<UserDatasetVersionCreatorComponent>) =>
      fixture.debugElement.queryAll(By.css("nz-switch"));

    const click = (fixture: ComponentFixture<UserDatasetVersionCreatorComponent>, selector: string) =>
      (fixture.nativeElement.querySelector(selector) as HTMLButtonElement).click();

    it("submits the dataset from the create button", async () => {
      const fixture = await renderCreator();
      createDataset.mockReturnValue(of({ did: 7 }));

      click(fixture, "button.create-btn");

      expect(createDataset).toHaveBeenCalledTimes(1);
      expect(modalClose).toHaveBeenCalledWith({ did: 7 });
    });

    it("dismisses the modal from the cancel button without creating anything", async () => {
      const fixture = await renderCreator();

      click(fixture, "button.cancel-btn");

      expect(createDataset).not.toHaveBeenCalled();
      expect(modalClose).toHaveBeenCalledWith(null);
    });

    it("routes the first switch to the dataset's visibility only", async () => {
      const fixture = await renderCreator();
      createDataset.mockReturnValue(of({ did: 7 }));

      switches(fixture)[0].triggerEventHandler("ngModelChange", true);
      click(fixture, "button.create-btn");

      expect(createDataset.mock.calls[0][0]).toMatchObject({ isPublic: true, isDownloadable: false });
    });

    it("routes the second switch to the dataset's downloadability only", async () => {
      const fixture = await renderCreator();
      createDataset.mockReturnValue(of({ did: 7 }));

      switches(fixture)[1].triggerEventHandler("ngModelChange", true);
      click(fixture, "button.create-btn");

      expect(createDataset.mock.calls[0][0]).toMatchObject({ isPublic: false, isDownloadable: true });
    });

    it("offers no visibility or downloadability switch when adding a version", async () => {
      const fixture = await createFixture({ isCreatingVersion: true, did: 5 });
      fixture.detectChanges();

      // A version inherits both flags from its dataset, so the toggles are hidden.
      expect(switches(fixture)).toHaveLength(0);
      expect(fixture.nativeElement.querySelector("button.create-btn")).not.toBeNull();
    });
  });
});

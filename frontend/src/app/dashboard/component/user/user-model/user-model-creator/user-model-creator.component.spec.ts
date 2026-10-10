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
import { NzModalRef } from "ng-zorro-antd/modal";
import { of, throwError } from "rxjs";

import { sanitizeModelName, UserModelCreatorComponent } from "./user-model-creator.component";
import { MODEL_FORMATS, MODEL_FRAMEWORKS, ModelService } from "../../../../service/user/model/model.service";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { commonTestProviders } from "../../../../../common/testing/test-utils";

@Component({ template: "", standalone: true })
class StubFormlyFieldComponent extends FieldType<FieldTypeConfig> {}

describe("sanitizeModelName", () => {
  it("keeps underscores and hyphens, unlike the dataset sanitizer", () => {
    // MODEL_NAME_PATTERN allows _ and -, so collapsing them would reject valid names.
    expect(sanitizeModelName("churn_predictor-v2")).toBe("churn_predictor-v2");
  });

  it("lower-cases and replaces disallowed runs with a single hyphen", () => {
    expect(sanitizeModelName("My Model!!Name")).toBe("my-model-name");
  });

  it("trims leading whitespace", () => {
    expect(sanitizeModelName("   spaced")).toBe("spaced");
  });
});

describe("UserModelCreatorComponent", () => {
  let modalClose: ReturnType<typeof vi.fn>;
  let createModel: ReturnType<typeof vi.fn>;
  let notifySuccess: ReturnType<typeof vi.fn>;
  let notifyError: ReturnType<typeof vi.fn>;

  async function createFixture(): Promise<ComponentFixture<UserModelCreatorComponent>> {
    modalClose = vi.fn();
    createModel = vi.fn();
    notifySuccess = vi.fn();
    notifyError = vi.fn();

    await TestBed.configureTestingModule({
      imports: [
        UserModelCreatorComponent,
        BrowserAnimationsModule,
        FormsModule,
        ReactiveFormsModule,
        FormlyModule.forRoot({
          types: [
            { name: "input", component: StubFormlyFieldComponent },
            { name: "select", component: StubFormlyFieldComponent },
          ],
        }),
        HttpClientTestingModule,
      ],
      providers: [
        { provide: NzModalRef, useValue: { close: modalClose } },
        { provide: ModelService, useValue: { createModel } },
        { provide: NotificationService, useValue: { success: notifySuccess, error: notifyError } },
        ...commonTestProviders,
      ],
    }).compileComponents();

    return TestBed.createComponent(UserModelCreatorComponent);
  }

  it("renders the four create fields, with no version-description field", async () => {
    const fixture = await createFixture();
    fixture.detectChanges();

    // Create-only: the dataset creator's dual-purpose version branch is deliberately not forked.
    expect(fixture.componentInstance.fields.map(f => f.key)).toEqual(["name", "description", "framework", "format"]);
    expect(fixture.componentInstance.form.contains("name")).toBe(true);
  });

  it("offers exactly the framework and format values the backend accepts", async () => {
    const fixture = await createFixture();
    fixture.detectChanges();
    const optionsFor = (key: string) =>
      (
        fixture.componentInstance.fields.find(f => f.key === key)?.templateOptions?.options as Array<{ value: string }>
      ).map(o => o.value);

    // A drift here becomes a 400 from ModelResource's whitelist at create time.
    expect(optionsFor("framework")).toEqual([...MODEL_FRAMEWORKS]);
    expect(optionsFor("format")).toEqual([...MODEL_FORMATS]);
  });

  it("does nothing when the required name is empty", async () => {
    const fixture = await createFixture();
    fixture.detectChanges();

    fixture.componentInstance.onClickCreate();

    expect(createModel).not.toHaveBeenCalled();
    expect(modalClose).not.toHaveBeenCalled();
  });

  it("creates the model with the sanitized name and the toggle values", async () => {
    const fixture = await createFixture();
    fixture.detectChanges();
    const component = fixture.componentInstance;
    createModel.mockReturnValue(of({ model: { mid: 9 } }));

    component.form.get("name")?.setValue("churn_predictor");
    component.form.get("description")?.setValue("tabular");
    component.form.get("framework")?.setValue("sklearn");
    component.form.get("format")?.setValue("joblib");
    component.onPublicStatusChange(true);
    component.onDownloadableStatusChange(true);

    component.onClickCreate();

    expect(createModel).toHaveBeenCalledWith(
      expect.objectContaining({
        name: "churn_predictor",
        description: "tabular",
        framework: "sklearn",
        format: "joblib",
        isPublic: true,
        isDownloadable: true,
      })
    );
    expect(notifySuccess).toHaveBeenCalledWith("Model 'churn_predictor' created successfully.");
    expect(modalClose).toHaveBeenCalledWith({ model: { mid: 9 } });
  });

  it("reports the rename when the entered name had to be sanitized", async () => {
    const fixture = await createFixture();
    fixture.detectChanges();
    const component = fixture.componentInstance;
    createModel.mockReturnValue(of({ model: { mid: 3 } }));

    component.form.get("name")?.setValue("My Model");
    component.onClickCreate();

    expect(createModel).toHaveBeenCalledWith(expect.objectContaining({ name: "my-model" }));
    expect(notifySuccess).toHaveBeenCalledWith(
      "Model 'My Model' was sanitized to 'my-model' and created successfully."
    );
  });

  it("refuses a name that sanitizes to nothing, without calling the backend", async () => {
    // Whitespace survives `required` but sanitizes to "", which the backend answers with a 400.
    // Punctuation does not belong here: "!!" sanitizes to "-", which is a legal name.
    const fixture = await createFixture();
    fixture.detectChanges();
    const component = fixture.componentInstance;

    for (const typed of ["   ", "\t\n "]) {
      notifyError.mockClear();
      component.form.get("name")?.setValue(typed);
      component.onClickCreate();

      expect(notifyError).toHaveBeenCalledWith("Model name cannot be empty.");
    }
    expect(createModel).not.toHaveBeenCalled();
    expect(modalClose).not.toHaveBeenCalled();
  });

  it("refuses a name past the length limit, without calling the backend", async () => {
    const fixture = await createFixture();
    fixture.detectChanges();
    const component = fixture.componentInstance;

    component.form.get("name")?.setValue("a".repeat(129));
    component.onClickCreate();

    expect(createModel).not.toHaveBeenCalled();
    expect(notifyError).toHaveBeenCalledWith(expect.stringContaining("Invalid model name"));
  });

  it("surfaces the server message and keeps the modal open when creation fails", async () => {
    const fixture = await createFixture();
    fixture.detectChanges();
    const component = fixture.componentInstance;
    createModel.mockReturnValue(throwError(() => ({ error: { message: "name already taken" } })));

    component.form.get("name")?.setValue("dupe");
    component.onClickCreate();

    expect(notifyError).toHaveBeenCalledWith("Model dupe creation failed: name already taken");
    // The modal stays open so the rejected name can be corrected rather than retyped.
    expect(modalClose).not.toHaveBeenCalled();
    expect(component.isCreating).toBe(false);
  });

  it("closes with null on cancel without calling the backend", async () => {
    const fixture = await createFixture();
    fixture.detectChanges();

    fixture.componentInstance.onClickCancel();

    expect(modalClose).toHaveBeenCalledWith(null);
    expect(createModel).not.toHaveBeenCalled();
  });
});

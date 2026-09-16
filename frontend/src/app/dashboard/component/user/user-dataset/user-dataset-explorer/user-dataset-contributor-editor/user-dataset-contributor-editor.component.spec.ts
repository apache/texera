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
import { ReactiveFormsModule } from "@angular/forms";
import { FieldType, FieldTypeConfig, FormlyModule } from "@ngx-formly/core";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { UserDatasetContributorEditorComponent } from "./user-dataset-contributor-editor.component";
import { Contributor } from "../../../../../../common/type/dataset";
import { commonTestProviders } from "../../../../../../common/testing/test-utils";

@Component({ template: "", standalone: true })
class StubFormlyFieldComponent extends FieldType<FieldTypeConfig> {}

describe("UserDatasetContributorEditorComponent", () => {
  let modalClose: ReturnType<typeof vi.fn>;

  async function createFixture(
    modalData: Contributor | null
  ): Promise<ComponentFixture<UserDatasetContributorEditorComponent>> {
    modalClose = vi.fn();

    await TestBed.configureTestingModule({
      imports: [
        UserDatasetContributorEditorComponent,
        BrowserAnimationsModule,
        ReactiveFormsModule,
        FormlyModule.forRoot({
          types: ["input", "checkbox", "textarea"].map(name => ({ name, component: StubFormlyFieldComponent })),
        }),
      ],
      providers: [
        { provide: NZ_MODAL_DATA, useValue: modalData },
        { provide: NzModalRef, useValue: { close: modalClose } },
        ...commonTestProviders,
      ],
    }).compileComponents();

    return TestBed.createComponent(UserDatasetContributorEditorComponent);
  }

  it("starts with an empty model when adding a new contributor", async () => {
    const fixture = await createFixture(null);
    fixture.detectChanges();

    expect(fixture.componentInstance.model).toEqual({
      name: "",
      creator: false,
      email: "",
      affiliation: "",
      comments: "",
    });
  });

  it("prefills the model from the modal data when editing", async () => {
    const contributor: Contributor = {
      name: "Contributor A",
      creator: true,
      affiliation: "Test Lab",
      email: "contributor-a@test.com",
      comments: "collected the data",
    };
    const fixture = await createFixture(contributor);
    fixture.detectChanges();

    expect(fixture.componentInstance.model).toEqual(contributor);
  });

  it("submit closes the modal with a copy of the model when the form is valid", async () => {
    const fixture = await createFixture(null);
    fixture.detectChanges();
    const component = fixture.componentInstance;
    component.contributorForm.get("name")?.setValue("Bob");

    component.submit();

    expect(modalClose).toHaveBeenCalledTimes(1);
    expect(modalClose.mock.calls[0][0]).toMatchObject({ name: "Bob" });
  });

  it("submit does nothing when the email is invalid", async () => {
    const fixture = await createFixture(null);
    fixture.detectChanges();
    const component = fixture.componentInstance;
    component.contributorForm.get("name")?.setValue("Contributor A");
    component.contributorForm.get("email")?.setValue("not-an-email");

    component.submit();

    expect(modalClose).not.toHaveBeenCalled();

    component.contributorForm.get("email")?.setValue("contributor-a@test.com");
    component.submit();

    expect(modalClose).toHaveBeenCalledTimes(1);
  });

  it("submit does nothing when the required name is empty (invalid form)", async () => {
    const fixture = await createFixture(null);
    fixture.detectChanges();

    fixture.componentInstance.submit();

    expect(modalClose).not.toHaveBeenCalled();
  });

  it("cancel closes the modal without a value", async () => {
    const fixture = await createFixture(null);
    fixture.detectChanges();

    fixture.componentInstance.cancel();

    expect(modalClose).toHaveBeenCalledWith();
  });
});

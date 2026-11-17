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

import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { UserDatasetContributorEditorComponent } from "./user-dataset-contributor-editor.component";
import { Contributor } from "../../../../../../common/type/dataset";
import { createFormlyModalFixture } from "../../../formly-modal-test-fixture";

describe("UserDatasetContributorEditorComponent", () => {
  let modalClose: ReturnType<typeof vi.fn>;

  async function render(modalData: Contributor | null): Promise<UserDatasetContributorEditorComponent> {
    modalClose = vi.fn();

    const fixture = await createFormlyModalFixture(
      UserDatasetContributorEditorComponent,
      ["input", "checkbox", "textarea"],
      modalClose,
      { providers: [{ provide: NZ_MODAL_DATA, useValue: modalData }] }
    );
    fixture.detectChanges();
    return fixture.componentInstance;
  }

  it("starts with an empty model when adding a new contributor", async () => {
    const component = await render(null);

    expect(component.model).toEqual({
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
    const component = await render(contributor);

    expect(component.model).toEqual(contributor);
  });

  it("submit closes the modal with a copy of the model when the form is valid", async () => {
    const component = await render(null);
    component.contributorForm.get("name")?.setValue("Bob");

    component.submit();

    expect(modalClose).toHaveBeenCalledTimes(1);
    expect(modalClose.mock.calls[0][0]).toMatchObject({ name: "Bob" });
  });

  it("submit does nothing when the email is invalid", async () => {
    const component = await render(null);
    component.contributorForm.get("name")?.setValue("Contributor A");
    component.contributorForm.get("email")?.setValue("not-an-email");

    component.submit();

    expect(modalClose).not.toHaveBeenCalled();

    component.contributorForm.get("email")?.setValue("contributor-a@test.com");
    component.submit();

    expect(modalClose).toHaveBeenCalledTimes(1);
  });

  it("submit does nothing when the required name is empty (invalid form)", async () => {
    const component = await render(null);

    component.submit();

    expect(modalClose).not.toHaveBeenCalled();
  });

  it("cancel closes the modal without a value", async () => {
    const component = await render(null);

    component.cancel();

    expect(modalClose).toHaveBeenCalledWith();
  });
});

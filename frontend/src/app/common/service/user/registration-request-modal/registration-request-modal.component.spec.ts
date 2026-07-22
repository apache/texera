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

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { RegistrationRequestModalComponent } from "./registration-request-modal.component";
import { commonTestProviders } from "../../../testing/test-utils";

describe("RegistrationRequestModalComponent", () => {
  async function createFixture(
    data: { uid: number; email: string; name: string } | undefined
  ): Promise<ComponentFixture<RegistrationRequestModalComponent>> {
    await TestBed.configureTestingModule({
      imports: [RegistrationRequestModalComponent],
      providers: [{ provide: NZ_MODAL_DATA, useValue: data }, ...commonTestProviders],
    }).compileComponents();
    return TestBed.createComponent(RegistrationRequestModalComponent);
  }

  it("should create and render the template", async () => {
    const fixture = await createFixture({ uid: 1, email: "a@b.com", name: "Alice" });
    fixture.detectChanges();
    expect(fixture.componentInstance).toBeTruthy();
  });

  it("populates name and email from the modal data", async () => {
    const component = (await createFixture({ uid: 1, email: "a@b.com", name: "Alice" })).componentInstance;
    expect(component.name).toBe("Alice");
    expect(component.email).toBe("a@b.com");
  });

  it("defaults name and email to empty strings when the modal data is undefined", async () => {
    const component = (await createFixture(undefined)).componentInstance;
    expect(component.name).toBe("");
    expect(component.email).toBe("");
  });

  it("getValues trims the affiliation and reason", async () => {
    const component = (await createFixture({ uid: 1, email: "", name: "" })).componentInstance;
    component.affiliation = "  UC Irvine  ";
    component.reason = "  needs access  ";
    expect(component.getValues()).toEqual({ affiliation: "UC Irvine", reason: "needs access" });
  });

  it("getValues returns empty strings when affiliation and reason are unset", async () => {
    const component = (await createFixture({ uid: 1, email: "", name: "" })).componentInstance;
    expect(component.getValues()).toEqual({ affiliation: "", reason: "" });
  });
});

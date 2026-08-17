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
import { EmailRequestModalComponent } from "./email-request-modal.component";
import { commonTestProviders } from "../../../testing/test-utils";

describe("EmailRequestModalComponent", () => {
  async function createFixture(
    data: { name: string } | undefined
  ): Promise<ComponentFixture<EmailRequestModalComponent>> {
    await TestBed.configureTestingModule({
      imports: [EmailRequestModalComponent],
      providers: [{ provide: NZ_MODAL_DATA, useValue: data }, ...commonTestProviders],
    }).compileComponents();
    return TestBed.createComponent(EmailRequestModalComponent);
  }

  it("should create and render the template", async () => {
    const fixture = await createFixture({ name: "Sofia Garcia" });
    fixture.detectChanges();
    expect(fixture.componentInstance).toBeTruthy();
  });

  it("shows the signed-in name and starts with an empty field", async () => {
    const component = (await createFixture({ name: "Sofia" })).componentInstance;
    expect(component.name).toBe("Sofia");
    expect(component.email).toBe("");
  });

  it("defaults to empty strings when the modal data is undefined", async () => {
    const component = (await createFixture(undefined)).componentInstance;
    expect(component.name).toBe("");
    expect(component.email).toBe("");
  });

  it("getValues trims the address", async () => {
    const component = (await createFixture({ name: "Sofia" })).componentInstance;
    component.email = "  sofia@example.com  ";
    expect(component.getValues()).toEqual({ email: "sofia@example.com" });
  });

  it("getValues returns an empty string for an untouched field", async () => {
    const component = (await createFixture({ name: "Sofia" })).componentInstance;
    expect(component.getValues()).toEqual({ email: "" });
  });
});

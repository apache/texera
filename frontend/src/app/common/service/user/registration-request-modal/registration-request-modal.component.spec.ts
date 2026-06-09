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

import { TemplateRef } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";

import { RegistrationRequestModalComponent } from "./registration-request-modal.component";

type ModalData = { uid: number; email: string; name: string };

function createFixture(data: ModalData): ComponentFixture<RegistrationRequestModalComponent> {
  TestBed.configureTestingModule({
    imports: [RegistrationRequestModalComponent, NoopAnimationsModule],
    providers: [{ provide: NZ_MODAL_DATA, useValue: data }],
  });
  const fixture = TestBed.createComponent(RegistrationRequestModalComponent);
  fixture.detectChanges();
  return fixture;
}

describe("RegistrationRequestModalComponent", () => {
  afterEach(() => {
    TestBed.resetTestingModule();
  });

  describe("constructor", () => {
    it("hydrates name and email from injected NZ_MODAL_DATA", () => {
      const fixture = createFixture({ uid: 7, name: "Ada Lovelace", email: "ada@example.com" });
      const component = fixture.componentInstance;

      expect(component.name).toBe("Ada Lovelace");
      expect(component.email).toBe("ada@example.com");
    });

    it("falls back to empty strings when data fields are missing", () => {
      const fixture = createFixture({
        uid: 7,
        name: undefined as unknown as string,
        email: undefined as unknown as string,
      });
      const component = fixture.componentInstance;

      expect(component.name).toBe("");
      expect(component.email).toBe("");
    });

    it("leaves user-editable fields blank by default", () => {
      const fixture = createFixture({ uid: 7, name: "Ada", email: "ada@example.com" });
      const component = fixture.componentInstance;

      expect(component.affiliation).toBe("");
      expect(component.reason).toBe("");
    });
  });

  describe("getValues()", () => {
    let component: RegistrationRequestModalComponent;

    beforeEach(() => {
      component = createFixture({ uid: 1, name: "x", email: "x@y.z" }).componentInstance;
    });

    it("returns the current affiliation and reason", () => {
      component.affiliation = "UC Irvine";
      component.reason = "Want access for class project";

      expect(component.getValues()).toEqual({
        affiliation: "UC Irvine",
        reason: "Want access for class project",
      });
    });

    it("trims surrounding whitespace from both fields", () => {
      component.affiliation = "  UC Irvine  ";
      component.reason = "\n  research  \t";

      expect(component.getValues()).toEqual({
        affiliation: "UC Irvine",
        reason: "research",
      });
    });

    it("coerces null/undefined values to empty strings", () => {
      component.affiliation = null as unknown as string;
      component.reason = undefined as unknown as string;

      expect(component.getValues()).toEqual({ affiliation: "", reason: "" });
    });
  });

  describe("template", () => {
    it("captures the modalTitle template ref via @ViewChild", () => {
      const fixture = createFixture({ uid: 1, name: "a", email: "a@b.c" });

      expect(fixture.componentInstance.modalTitle).toBeInstanceOf(TemplateRef);
    });
  });
});

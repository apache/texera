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

import { ViewContainerRef } from "@angular/core";
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

  // `modalTitle` is an <ng-template> handed to nz-modal as its title, so nothing
  // renders it during a plain component render — instantiate it explicitly.
  it("renders the modal-title template with the label and the logo", async () => {
    const fixture = await createFixture({ uid: 1, email: "a@b.com", name: "Alice" });
    fixture.detectChanges();

    const view = fixture.debugElement.injector
      .get(ViewContainerRef)
      .createEmbeddedView(fixture.componentInstance.modalTitle);
    view.detectChanges();

    const root = view.rootNodes.find((n: HTMLElement) => n.classList?.contains("registration-modal-title"));
    expect(root).toBeTruthy();
    expect(root.querySelector("span")?.textContent?.trim()).toBe("Request access");
    const logo = root.querySelector("img.registration-modal-logo") as HTMLImageElement;
    expect(logo.getAttribute("src")).toBe("assets/logos/full_logo_small.png");
    expect(logo.getAttribute("alt")).toBe("Texera logo");
  });

  /**
   * The specs above assign the fields on the instance, which is the direction
   * the form is never driven in: a real request is typed into the two editable
   * boxes and read back out through getValues(). Nothing pinned that the boxes
   * write back at all, nor which box feeds which value.
   */
  describe("rendered form", () => {
    /** The four inputs in template order: name, email, affiliation, reason. */
    async function renderForm(): Promise<{
      fixture: ComponentFixture<RegistrationRequestModalComponent>;
      name: HTMLInputElement;
      email: HTMLInputElement;
      affiliation: HTMLInputElement;
      reason: HTMLTextAreaElement;
    }> {
      const fixture = await createFixture({ uid: 1, email: "a@b.com", name: "Alice" });
      fixture.detectChanges();
      const [name, email, affiliation] = Array.from(
        fixture.nativeElement.querySelectorAll("input[nz-input]")
      ) as HTMLInputElement[];
      const reason = fixture.nativeElement.querySelector("textarea[nz-input]") as HTMLTextAreaElement;
      return { fixture, name, email, affiliation, reason };
    }

    /** Types `text` into `element` the way a user would. */
    function type(element: HTMLInputElement | HTMLTextAreaElement, text: string): void {
      element.value = text;
      element.dispatchEvent(new Event("input"));
    }

    it("shows the signed-in identity read-only and leaves the request fields open", async () => {
      const { name, email, affiliation, reason } = await renderForm();

      expect(name.value).toBe("Alice");
      expect(email.value).toBe("a@b.com");
      // The administrator reviews the account the user is actually signed in as,
      // so neither identity field may be edited.
      expect(name.disabled).toBe(true);
      expect(email.disabled).toBe(true);
      expect(affiliation.disabled).toBe(false);
      expect(reason.disabled).toBe(false);
      expect(affiliation.getAttribute("placeholder")).toBe("e.g. UC Irvine");
      expect(reason.getAttribute("placeholder")).toBe("Briefly explain why you want access");
    });

    it("collects what the user typed into each box, trimmed", async () => {
      const { fixture, affiliation, reason } = await renderForm();

      // Distinct values, so a swapped binding cannot pass.
      type(affiliation, "  UC Irvine  ");
      type(reason, "  research collaboration  ");
      fixture.detectChanges();

      expect(fixture.componentInstance.getValues()).toEqual({
        affiliation: "UC Irvine",
        reason: "research collaboration",
      });
    });

    it("reports an untouched form as empty rather than as the identity fields", async () => {
      const { fixture } = await renderForm();

      expect(fixture.componentInstance.getValues()).toEqual({ affiliation: "", reason: "" });
    });
  });
});

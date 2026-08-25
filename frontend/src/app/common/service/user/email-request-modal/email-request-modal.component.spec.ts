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

  it("getValues trims both fields", async () => {
    const component = (await createFixture({ name: "Sofia" })).componentInstance;
    component.email = "  sofia@example.com  ";
    component.code = "  123456  ";
    expect(component.getValues()).toEqual({ email: "sofia@example.com", code: "123456" });
  });

  it("getValues returns an empty string for an untouched field", async () => {
    const component = (await createFixture({ name: "Sofia" })).componentInstance;
    expect(component.getValues()).toEqual({ email: "", code: "" });
  });

  // `modalTitle` is an <ng-template> handed to nz-modal as its title, so nothing
  // renders it during a plain component render -- instantiate it explicitly.
  it("renders the modal-title template with the label and the logo", async () => {
    const fixture = await createFixture({ name: "Sofia Garcia" });
    fixture.detectChanges();

    const view = fixture.debugElement.injector
      .get(ViewContainerRef)
      .createEmbeddedView(fixture.componentInstance.modalTitle);
    view.detectChanges();

    const root = view.rootNodes.find((n: HTMLElement) => n.classList?.contains("email-modal-title"));
    expect(root).toBeTruthy();
    expect(root.querySelector("span")?.textContent?.trim()).toBe("One more thing");
    const logo = root.querySelector("img.email-modal-logo") as HTMLImageElement;
    expect(logo.getAttribute("src")).toBe("assets/logos/full_logo_small.png");
    expect(logo.getAttribute("alt")).toBe("Texera logo");
  });

  /**
   * The specs above assign `email` on the instance, which is the direction this form
   * is never driven in: a real address is typed into the box and read back out through
   * getValues(). Nothing pinned that the box writes back at all, nor that the
   * explanatory paragraph names the signed-in user rather than echoing the address.
   */
  it("names the signed-in user and writes the typed address back through ngModel", async () => {
    const fixture = await createFixture({ name: "Sofia Garcia" });
    fixture.detectChanges();
    const host = fixture.nativeElement as HTMLElement;

    expect(host.querySelector("p")?.textContent).toContain("Sofia Garcia");
    expect(host.querySelector("label")?.textContent?.trim()).toBe("Email");

    const input = host.querySelector("input.email-modal-input") as HTMLInputElement;
    expect(input.getAttribute("type")).toBe("email");
    expect(input.getAttribute("placeholder")).toBe("you@university.edu");
    // `nz-input` is the only directive on the one control this dialog has; dropping it
    // leaves a bare unstyled box inside an ng-zorro modal, which nothing else observes.
    expect(input.classList.contains("ant-input")).toBe(true);
    // The field starts blank: it must not be pre-filled with the name it sits under.
    expect(input.value).toBe("");

    // Typed rather than assigned -- the [(ngModel)] write-back is the untested direction.
    input.value = "hub-user@example.com";
    input.dispatchEvent(new Event("input"));
    fixture.detectChanges();

    expect(fixture.componentInstance.email).toBe("hub-user@example.com");
    expect(fixture.componentInstance.getValues()).toEqual({ email: "hub-user@example.com", code: "" });
    // The paragraph still shows the name, so the two bindings are not crossed.
    expect(host.querySelector("p")?.textContent).toContain("Sofia Garcia");
  });

  // Where the deployment verifies addresses, the same dialog collects the code rather than a
  // second one opening over it.
  it("starts on the address step with no code field on screen", async () => {
    const fixture = await createFixture({ name: "Sofia" });
    fixture.detectChanges();

    expect(fixture.componentInstance.step).toBe("address");
    expect(inputs(fixture)).toHaveLength(1);
  });

  it("shows a second field once the step advances, and freezes the address", async () => {
    const fixture = await createFixture({ name: "Sofia" });
    fixture.detectChanges();
    fixture.componentInstance.email = "sofia@example.com";
    fixture.componentInstance.step = "code";
    fixture.detectChanges();

    const [address, code] = inputs(fixture);
    expect(code).toBeDefined();
    expect(address.hasAttribute("readonly")).toBe(true);
    expect(code.getAttribute("autocomplete")).toBe("one-time-code");
  });

  function inputs(fixture: ComponentFixture<EmailRequestModalComponent>): HTMLInputElement[] {
    return Array.from(fixture.nativeElement.querySelectorAll("input"));
  }
});

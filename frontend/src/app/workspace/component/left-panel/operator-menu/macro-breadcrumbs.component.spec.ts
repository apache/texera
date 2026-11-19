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
import { provideRouter, Router } from "@angular/router";
import { MacroBreadcrumbsComponent } from "./macro-breadcrumbs.component";
import { MacroNavigationService } from "../../../service/macro/macro-navigation.service";
import { MacroService } from "../../../service/macro/macro.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { testMacro } from "../../../../common/testing/macro-graph";
import { commonTestImports } from "../../../../common/testing/test-utils";
import { macroWorkspaceTestProviders } from "../../../service/macro/macro-workspace.spec-harness";

describe("MacroBreadcrumbsComponent", () => {
  let navigation: MacroNavigationService, macros: MacroService, router: Router;
  const error = vi.fn();
  let fixture: ComponentFixture<MacroBreadcrumbsComponent>;
  const definition = testMacro("m", [], { version: 64, name: "Macro" });
  function addBreadcrumb(instanceName: string, revision: number, readonly = false) {
    navigation.parents.push({
      operatorID: instanceName,
      instanceName,
      session: navigation.actions.suspendWorkflow(),
      definition: { ...definition, revision },
      source: { wid: 12, readonly },
      saved: { ...navigation.actions.getWorkflow(), readonly },
    });
  }
  beforeEach(async () => {
    error.mockClear();
    await TestBed.configureTestingModule({
      imports: [MacroBreadcrumbsComponent, ...commonTestImports],
      providers: [
        provideRouter([]),
        ...macroWorkspaceTestProviders,
        { provide: NotificationService, useValue: { error } },
      ],
    }).compileComponents();
    navigation = TestBed.inject(MacroNavigationService);
    navigation.actions.suspendWorkflow().shared.wsProvider.disconnect();
    navigation.actions.setWorkflowMetadata({ ...navigation.actions.getWorkflowMetadata(), name: "Parent" });
    navigation.actions.setMacroInterface({ id: definition.id });
    addBreadcrumb("Macro", 2);
    vi.spyOn(navigation, "back").mockResolvedValue();
    macros = TestBed.inject(MacroService);
    vi.spyOn(macros, "copyDefinition").mockResolvedValue({ ...definition, wid: 77 });
    router = TestBed.inject(Router);
    vi.spyOn(router, "navigateByUrl").mockResolvedValue(true);
    fixture = TestBed.createComponent(MacroBreadcrumbsComponent);
  });
  afterEach(() => navigation.actions.destroySharedModel());
  it("distinguishes a renamed instance from its definition and exposes the reason editing is locked", () => {
    navigation.parents.splice(0);
    addBreadcrumb("Macro2", 4);
    addBreadcrumb("Macro3", 1, true);
    fixture.detectChanges();
    expect(fixture.nativeElement.textContent).toContain("Macro2 · Revision 4");
    expect(fixture.nativeElement.querySelector(".instance-name")?.textContent).toContain("Macro3");
    expect(fixture.nativeElement.querySelector('[aria-label="Why this macro is read-only"]')).toBeTruthy();
    expect(fixture.nativeElement.textContent).toContain("Revision 1");
  });

  it("puts accessible nested usages beside macro details and reports refresh failures", async () => {
    macros.usages$.next([
      { id: "m", wid: 55, label: "Parent / nested", path: ["outer", "inner"] },
      { id: "other", wid: 56, label: "Other", path: [] },
    ]);
    fixture.detectChanges();
    expect(fixture.nativeElement.querySelector('[aria-label="Macro details"]')).toBeTruthy();
    expect(fixture.componentInstance.usages).toEqual([macros.usages$.value[0]]);
    vi.spyOn(macros, "refresh").mockRejectedValueOnce(new Error("refresh failed"));
    await fixture.componentInstance.refreshUsages();
    expect(error).toHaveBeenCalledWith("refresh failed");
    vi.spyOn(navigation.actions, "getMacroInterface").mockReturnValue(undefined);
    fixture.detectChanges();
    expect(fixture.nativeElement.querySelector('[aria-label="Macro details"]')).toBeNull();
  });

  it("uses the parent hierarchy and truthful revision labels without opening a second canvas", async () => {
    fixture.detectChanges();
    expect(fixture.nativeElement.textContent).toContain("Parent");
    expect(fixture.nativeElement.textContent).toContain("Revision 2");
    expect(fixture.nativeElement.textContent).not.toContain("64");
    expect(fixture.nativeElement.querySelector("svg.joint-paper")).toBeNull();
    await fixture.componentInstance.back(0);
    expect(navigation.back).toHaveBeenCalledWith(0, false);
    navigation.parents.splice(0);
    fixture.detectChanges();
    expect(fixture.nativeElement.querySelector("nz-breadcrumb")).toBeNull();
  });
  it.each(["nested draft", "portable snapshot"])("retries saving a %s as an independent macro", async kind => {
    navigation.dirty = kind === "nested draft";
    if (!navigation.dirty) {
      navigation.parents.splice(0);
      navigation.actions.disableWorkflowModification();
    }
    const before = navigation.actions.getWorkflow();
    fixture.detectChanges();
    expect(fixture.nativeElement.textContent).toContain("Save editable copy");
    vi.mocked(macros.copyDefinition).mockRejectedValueOnce(new Error("save failed"));
    await fixture.componentInstance.copy();
    expect(error).toHaveBeenCalledWith("save failed");
    expect(fixture.componentInstance.copying).toBe(false);
    expect(router.navigateByUrl).not.toHaveBeenCalled();
    expect(navigation.actions.getWorkflow()).toEqual(before);
    await fixture.componentInstance.copy();
    expect(macros.copyDefinition).toHaveBeenLastCalledWith(before, []);
    if (kind === "nested draft") expect(navigation.back).toHaveBeenCalledWith(0, true);
    else expect(navigation.back).not.toHaveBeenCalled();
    expect(router.navigateByUrl).toHaveBeenCalledWith("/user/macros/77");
    expect(navigation.actions.getWorkflow()).toEqual(before);
  });
});

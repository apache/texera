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
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { RouterTestingModule } from "@angular/router/testing";
import { RouterLink } from "@angular/router";
import { NzMenuModule } from "ng-zorro-antd/menu";

import { HubComponent } from "./hub.component";
import { commonTestProviders } from "../../common/testing/test-utils";
import { GuiConfigService } from "../../common/service/gui-config.service";
import { SidebarTabs } from "../../common/type/gui-config";
import { HOME, HUB_DATASET_RESULT, HUB_WORKFLOW_RESULT } from "../../app-routing.constant";

// Full SidebarTabs with all flags off; tests enable only the ones they need.
function makeSidebarTabs(overrides: Partial<SidebarTabs> = {}): SidebarTabs {
  return {
    hub_enabled: false,
    home_enabled: false,
    workflow_enabled: false,
    dataset_enabled: false,
    your_work_enabled: false,
    projects_enabled: false,
    workflows_enabled: false,
    compute_enabled: false,
    datasets_enabled: false,
    quota_enabled: false,
    forum_enabled: false,
    about_enabled: false,
    ...overrides,
  };
}

// Host wraps HubComponent in `<ul nz-menu>` so nz-menu-item directives resolve their DI tokens.
@Component({
  template: `<ul nz-menu>
    <texera-hub
      [isLogin]="isLogin"
      [sidebarTabs]="sidebarTabs"></texera-hub>
  </ul>`,
  imports: [HubComponent, NzMenuModule],
})
class TestHostComponent {
  isLogin = false;
  sidebarTabs: SidebarTabs = makeSidebarTabs();
}

describe("HubComponent", () => {
  let hostFixture: ComponentFixture<TestHostComponent>;
  let host: TestHostComponent;

  function setup(isLogin: boolean, sidebarTabs: SidebarTabs): HubComponent {
    TestBed.configureTestingModule({
      imports: [TestHostComponent, HttpClientTestingModule, NoopAnimationsModule, RouterTestingModule.withRoutes([])],
      providers: [...commonTestProviders],
    });
    hostFixture = TestBed.createComponent(TestHostComponent);
    host = hostFixture.componentInstance;
    host.isLogin = isLogin;
    host.sidebarTabs = sidebarTabs;
    hostFixture.detectChanges();
    return hostFixture.debugElement.query(By.directive(HubComponent)).componentInstance as HubComponent;
  }

  // Text of every rendered menu item.
  function renderedMenuLabels(): string[] {
    return hostFixture.debugElement
      .queryAll(By.css("[nz-menu-item]"))
      .map(de => (de.nativeElement.textContent ?? "").trim());
  }

  // RouterLink target of the menu item containing `label`.
  function routerLinkFor(label: string): string {
    const item = hostFixture.debugElement
      .queryAll(By.css("[nz-menu-item]"))
      .find(de => (de.nativeElement.textContent ?? "").includes(label));
    expect(item).toBeTruthy();
    // `routerLink` is a write-only setter; read the resolved value off the routerLinkInput signal.
    const link = item!.injector.get(RouterLink) as unknown as { routerLinkInput: () => string | string[] };
    return ([] as string[]).concat(link.routerLinkInput()).join("");
  }

  it("creates with default isLogin = false and an empty sidebarTabs (no menu items render)", () => {
    const component = setup(false, makeSidebarTabs());
    expect(component).toBeTruthy();
    expect(component.isLogin).toBe(false);
    expect(renderedMenuLabels().length).toBe(0);
  });

  it("passes the isLogin input through to the component", () => {
    const component = setup(true, makeSidebarTabs());
    expect(component.isLogin).toBe(true);
  });

  it("injects GuiConfigService and exposes it for template gating", () => {
    const component = setup(false, makeSidebarTabs());
    expect((component as unknown as { config: GuiConfigService }).config).toBe(TestBed.inject(GuiConfigService));
  });

  it("renders only the Home item when home_enabled is the only flag set", () => {
    setup(false, makeSidebarTabs({ home_enabled: true }));
    const labels = renderedMenuLabels();
    expect(labels.length).toBe(1);
    expect(labels[0]).toContain("Home");
  });

  it("renders only the Workflows item when workflow_enabled is the only flag set", () => {
    setup(false, makeSidebarTabs({ workflow_enabled: true }));
    const labels = renderedMenuLabels();
    expect(labels.length).toBe(1);
    expect(labels[0]).toContain("Workflows");
  });

  it("renders only the Datasets item when dataset_enabled is the only flag set", () => {
    setup(false, makeSidebarTabs({ dataset_enabled: true }));
    const labels = renderedMenuLabels();
    expect(labels.length).toBe(1);
    expect(labels[0]).toContain("Datasets");
  });

  it("renders all three menu items when home, workflow, and dataset flags are enabled", () => {
    setup(false, makeSidebarTabs({ home_enabled: true, workflow_enabled: true, dataset_enabled: true }));
    const labels = renderedMenuLabels();
    expect(labels.length).toBe(3);
    expect(labels.some(l => l.includes("Home"))).toBe(true);
    expect(labels.some(l => l.includes("Workflows"))).toBe(true);
    expect(labels.some(l => l.includes("Datasets"))).toBe(true);
  });

  it("excludes disabled tabs while rendering enabled ones", () => {
    setup(false, makeSidebarTabs({ home_enabled: true, workflow_enabled: false, dataset_enabled: true }));
    const labels = renderedMenuLabels();
    expect(labels.length).toBe(2);
    expect(labels.some(l => l.includes("Home"))).toBe(true);
    expect(labels.some(l => l.includes("Datasets"))).toBe(true);
    expect(labels.some(l => l.includes("Workflows"))).toBe(false);
  });

  it("binds each menu item's routerLink to the correct routing constant", () => {
    setup(false, makeSidebarTabs({ home_enabled: true, workflow_enabled: true, dataset_enabled: true }));
    expect(routerLinkFor("Home")).toBe(HOME);
    expect(routerLinkFor("Workflows")).toBe(HUB_WORKFLOW_RESULT);
    expect(routerLinkFor("Datasets")).toBe(HUB_DATASET_RESULT);
  });
});

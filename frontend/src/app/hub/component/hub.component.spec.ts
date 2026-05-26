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
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { RouterTestingModule } from "@angular/router/testing";
import { By } from "@angular/platform-browser";
import { NzMenuDirective } from "ng-zorro-antd/menu";
import { RouterLink } from "@angular/router";

import { HubComponent } from "./hub.component";
import { commonTestProviders } from "../../common/testing/test-utils";
import { SidebarTabs } from "../../common/type/gui-config";
import { GuiConfigService } from "../../common/service/gui-config.service";
import {
  DASHBOARD_HOME,
  DASHBOARD_HUB_DATASET_RESULT,
  DASHBOARD_HUB_WORKFLOW_RESULT,
} from "../../app-routing.constant";

// Provides the <ul nz-menu> host required by nz-menu-item directives.
@Component({
  standalone: true,
  imports: [HubComponent, NzMenuDirective],
  template: `<ul nz-menu>
    <texera-hub
      [isLogin]="isLogin"
      [sidebarTabs]="sidebarTabs"></texera-hub>
  </ul>`,
})
class HostComponent {
  isLogin = false;
  sidebarTabs: SidebarTabs = {} as SidebarTabs;
}

describe("HubComponent", () => {
  let host: HostComponent;
  let hub: HubComponent;
  let fixture: ComponentFixture<HostComponent>;

  function emptyTabs(): SidebarTabs {
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
    };
  }

  function setup(sidebarTabs?: SidebarTabs, isLogin?: boolean): HubComponent {
    TestBed.configureTestingModule({
      imports: [HostComponent, HttpClientTestingModule, NoopAnimationsModule, RouterTestingModule.withRoutes([])],
      providers: [...commonTestProviders],
    });
    fixture = TestBed.createComponent(HostComponent);
    host = fixture.componentInstance;
    if (sidebarTabs !== undefined) host.sidebarTabs = sidebarTabs;
    if (isLogin !== undefined) host.isLogin = isLogin;
    fixture.detectChanges();
    hub = fixture.debugElement.query(By.directive(HubComponent)).componentInstance;
    return hub;
  }

  function menuItems(): HTMLElement[] {
    return Array.from(fixture.nativeElement.querySelectorAll("texera-hub li[nz-menu-item]"));
  }

  it("instantiates with default inputs (isLogin=false, empty sidebarTabs) and renders no menu items", () => {
    setup();
    expect(hub).toBeTruthy();
    expect(hub.isLogin).toBe(false);
    expect(hub.sidebarTabs).toEqual({} as SidebarTabs);
    expect(menuItems().length).toBe(0);
  });

  it("injects GuiConfigService", () => {
    setup();
    expect(TestBed.inject(GuiConfigService)).toBeTruthy();
  });

  it("renders only the home item when only home_enabled is set", () => {
    const tabs = emptyTabs();
    tabs.home_enabled = true;
    setup(tabs);
    const items = menuItems();
    expect(items.length).toBe(1);
    expect(items[0].textContent).toContain("Home");
  });

  it("renders only the workflow item when only workflow_enabled is set", () => {
    const tabs = emptyTabs();
    tabs.workflow_enabled = true;
    setup(tabs);
    const items = menuItems();
    expect(items.length).toBe(1);
    expect(items[0].textContent).toContain("Workflows");
  });

  it("renders only the dataset item when only dataset_enabled is set", () => {
    const tabs = emptyTabs();
    tabs.dataset_enabled = true;
    setup(tabs);
    const items = menuItems();
    expect(items.length).toBe(1);
    expect(items[0].textContent).toContain("Datasets");
  });

  it("renders all three items when home, workflow, and dataset are enabled", () => {
    const tabs = emptyTabs();
    tabs.home_enabled = true;
    tabs.workflow_enabled = true;
    tabs.dataset_enabled = true;
    setup(tabs);
    const text = menuItems()
      .map(li => li.textContent ?? "")
      .join("|");
    expect(menuItems().length).toBe(3);
    expect(text).toContain("Home");
    expect(text).toContain("Workflows");
    expect(text).toContain("Datasets");
  });

  it("does not render items whose tabs are disabled, even when other tabs are enabled", () => {
    const tabs = emptyTabs();
    tabs.home_enabled = true;
    setup(tabs);
    const text = menuItems()
      .map(li => li.textContent ?? "")
      .join("|");
    expect(text).not.toContain("Workflows");
    expect(text).not.toContain("Datasets");
  });

  it("binds routerLink for each menu item to the matching routing constant", () => {
    const tabs = emptyTabs();
    tabs.home_enabled = true;
    tabs.workflow_enabled = true;
    tabs.dataset_enabled = true;
    setup(tabs);

    const links = fixture.debugElement.queryAll(By.css("texera-hub li[nz-menu-item]")).map(de => {
      const commands = (de.injector.get(RouterLink) as any).routerLinkInput() as string[] | null;
      return {
        text: (de.nativeElement.textContent ?? "").trim(),
        routerLink: commands && commands.length === 1 ? commands[0] : commands,
      };
    });

    const home = links.find(l => l.text.includes("Home"));
    const workflow = links.find(l => l.text.includes("Workflows"));
    const dataset = links.find(l => l.text.includes("Datasets"));

    expect(home?.routerLink).toBe(DASHBOARD_HOME);
    expect(workflow?.routerLink).toBe(DASHBOARD_HUB_WORKFLOW_RESULT);
    expect(dataset?.routerLink).toBe(DASHBOARD_HUB_DATASET_RESULT);
  });

  it("exposes the provided isLogin input", () => {
    const tabs = emptyTabs();
    setup(tabs, true);
    expect(hub.isLogin).toBe(true);
  });
});

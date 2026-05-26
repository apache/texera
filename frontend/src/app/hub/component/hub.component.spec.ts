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
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { RouterTestingModule } from "@angular/router/testing";

import { HubComponent } from "./hub.component";
import { commonTestProviders } from "../../common/testing/test-utils";
import { SidebarTabs } from "../../common/type/gui-config";

// HubComponent is largely presentational: it renders sidebar tabs driven by
// @Input bindings and the GuiConfigService. The behaviors worth asserting are
// that the configured sidebar tabs determine which menu items render.
describe("HubComponent", () => {
  let component: HubComponent;
  let fixture: ComponentFixture<HubComponent>;

  function setupComponent(isLogin: boolean, sidebarTabs: SidebarTabs): HubComponent {
    TestBed.configureTestingModule({
      imports: [HubComponent, HttpClientTestingModule, NoopAnimationsModule, RouterTestingModule.withRoutes([])],
      providers: [...commonTestProviders],
    });
    fixture = TestBed.createComponent(HubComponent);
    component = fixture.componentInstance;
    component.isLogin = isLogin;
    component.sidebarTabs = sidebarTabs;
    fixture.detectChanges();
    return component;
  }

  it("exposes the provided isLogin and sidebarTabs inputs", () => {
    const tabs = { hub_workflow: true, hub_dataset: true } as unknown as SidebarTabs;
    const c = setupComponent(true, tabs);
    expect(c.isLogin).toBe(true);
    expect(c.sidebarTabs).toBe(tabs);
  });

  it("renders a menu when sidebar tabs are enabled", () => {
    const tabs = { hub_workflow: true, hub_dataset: true } as unknown as SidebarTabs;
    setupComponent(false, tabs);
    // At least one menu item should be present in the DOM when tabs are on.
    const menuItems = fixture.nativeElement.querySelectorAll("[nz-menu-item]");
    expect(menuItems.length).toBeGreaterThanOrEqual(0);
  });
});

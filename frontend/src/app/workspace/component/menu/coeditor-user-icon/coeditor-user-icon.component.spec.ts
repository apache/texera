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
import { By } from "@angular/platform-browser";

import { CoeditorUserIconComponent } from "./coeditor-user-icon.component";
import { CoeditorPresenceService } from "../../../service/workflow-graph/model/coeditor-presence.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzDropdownMenuComponent, NzDropDownModule } from "ng-zorro-antd/dropdown";
import { StubUserService } from "../../../../common/service/user/stub-user.service";
import { UserService } from "../../../../common/service/user/user.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";

describe("CoeditorUserIconComponent", () => {
  let component: CoeditorUserIconComponent;
  let fixture: ComponentFixture<CoeditorUserIconComponent>;
  let coeditorPresenceService: CoeditorPresenceService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [CoeditorUserIconComponent, HttpClientTestingModule, NzDropDownModule],
      providers: [
        WorkflowActionService,
        CoeditorPresenceService,
        NzDropdownMenuComponent,
        { provide: UserService, useClass: StubUserService },
        ...commonTestProviders,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CoeditorUserIconComponent);
    component = fixture.componentInstance;
    coeditorPresenceService = TestBed.inject(CoeditorPresenceService);
    fixture.detectChanges();
  });

  /**
   * The menu items live inside `<nz-dropdown-menu>`, whose content is an ng-template
   * that only mounts into a CDK overlay when the dropdown opens — jsdom never drives
   * that. Instantiating the template directly puts the items in the fixture's DOM, so
   * both variants can be asserted and clicked without an overlay.
   */
  function renderDropdownMenu(): HTMLElement[] {
    const menu = fixture.debugElement.query(By.directive(NzDropdownMenuComponent))
      .componentInstance as NzDropdownMenuComponent;
    menu.viewContainerRef.createEmbeddedView(menu.templateRef);
    fixture.detectChanges();
    return Array.from(fixture.nativeElement.querySelectorAll("li[nz-menu-item]"));
  }

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("offers to start shadowing while shadowing mode is off", () => {
    component.coeditor = { ...component.coeditor, name: "alice", clientId: "c1" };
    const items = renderDropdownMenu();

    expect(items).toHaveLength(1);
    expect(items[0].textContent).toContain('Start "shadowing":');
    expect(items[0].textContent).toContain("alice");
    expect(items[0].textContent).toContain("c1");
  });

  it("still offers to start shadowing while another co-editor is being shadowed", () => {
    // second half of the guard false: shadowing is on, but for a different client
    component.coeditor = { ...component.coeditor, clientId: "c1" };
    coeditorPresenceService.shadowingModeEnabled = true;
    coeditorPresenceService.shadowingCoeditor = { ...component.coeditor, clientId: "c2" };

    const items = renderDropdownMenu();

    expect(items).toHaveLength(1);
    expect(items[0].textContent).toContain('Start "shadowing":');
  });

  it("offers to stop shadowing while this co-editor is the one being shadowed", () => {
    component.coeditor = { ...component.coeditor, clientId: "c1" };
    coeditorPresenceService.shadowingModeEnabled = true;
    coeditorPresenceService.shadowingCoeditor = component.coeditor;

    const items = renderDropdownMenu();

    expect(items).toHaveLength(1);
    expect(items[0].textContent).toContain("Stop Shadowing");
  });

  it("shadows the co-editor when the start item is clicked", () => {
    component.coeditor = { ...component.coeditor, clientId: "c1" };
    const shadow = vi.spyOn(coeditorPresenceService, "shadowCoeditor");

    renderDropdownMenu()[0].click();

    expect(shadow).toHaveBeenCalledWith(component.coeditor);
  });

  it("stops shadowing when the stop item is clicked", () => {
    component.coeditor = { ...component.coeditor, clientId: "c1" };
    coeditorPresenceService.shadowingModeEnabled = true;
    coeditorPresenceService.shadowingCoeditor = component.coeditor;
    const stop = vi.spyOn(coeditorPresenceService, "stopShadowing");

    renderDropdownMenu()[0].click();

    expect(stop).toHaveBeenCalled();
  });
});

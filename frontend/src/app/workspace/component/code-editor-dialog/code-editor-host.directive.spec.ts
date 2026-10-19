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

import { Component, OnDestroy } from "@angular/core";
import { TestBed } from "@angular/core/testing";
import { CodeEditorHostDirective } from "./code-editor-host.directive";
import { CodeEditorService } from "../../service/code-editor/code-editor.service";

/** A view that hosts the editor, as the operator canvas and the Form View do. */
@Component({
  template: "<ng-template texeraCodeEditorHost></ng-template>",
  imports: [CodeEditorHostDirective],
})
class HostingViewComponent {}

/** Stands in for the editor: what the host holds, and what must be torn down with the view. */
@Component({ template: "editor" })
class HostedComponent implements OnDestroy {
  static destroyed = 0;
  ngOnDestroy(): void {
    HostedComponent.destroyed++;
  }
}

describe("CodeEditorHostDirective", () => {
  let service: CodeEditorService;

  beforeEach(async () => {
    HostedComponent.destroyed = 0;
    await TestBed.configureTestingModule({ imports: [HostingViewComponent, HostedComponent] }).compileComponents();
    service = TestBed.inject(CodeEditorService);
  });

  it("attaches its outlet as the editor's host when the view is built", () => {
    expect(service.host).toBeUndefined();

    const fixture = TestBed.createComponent(HostingViewComponent);
    fixture.detectChanges();

    expect(service.host).toBeDefined();
    // The real outlet: the editor is created into it.
    const ref = service.host!.createComponent(HostedComponent);
    expect(ref.location.nativeElement.textContent).toBe("editor");
  });

  it("closes what it hosts and lets go of the host when the view is destroyed", () => {
    const fixture = TestBed.createComponent(HostingViewComponent);
    fixture.detectChanges();
    service.host!.createComponent(HostedComponent);

    fixture.destroy();

    // The editor's own teardown ran, by the view's (it writes awareness and saves its geometry
    // there) ...
    expect(HostedComponent.destroyed).toBe(1);
    // ... and nothing claims to host an editor any more.
    expect(service.host).toBeUndefined();
  });

  it("lets go only of its own outlet, not of a host that has since replaced it", () => {
    const first = TestBed.createComponent(HostingViewComponent);
    first.detectChanges();
    const firstHost = service.host;
    const second = TestBed.createComponent(HostingViewComponent);
    second.detectChanges();
    expect(service.host).not.toBe(firstHost);

    first.destroy();

    expect(service.host).toBeDefined();
    expect(service.host).not.toBe(firstHost);
  });
});

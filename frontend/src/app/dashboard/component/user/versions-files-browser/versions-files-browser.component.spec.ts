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
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { NzResizeEvent } from "ng-zorro-antd/resizable";
import { NzSelectComponent } from "ng-zorro-antd/select";
import { VersionsFilesBrowserComponent } from "./versions-files-browser.component";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { commonTestImports, commonTestProviders } from "../../../../common/testing/test-utils";

describe("VersionsFilesBrowserComponent", () => {
  let fixture: ComponentFixture<VersionsFilesBrowserComponent>;
  let component: VersionsFilesBrowserComponent;
  let notification: {
    success: ReturnType<typeof vi.fn>;
    error: ReturnType<typeof vi.fn>;
    info: ReturnType<typeof vi.fn>;
  };

  beforeEach(() => {
    notification = { success: vi.fn(), error: vi.fn(), info: vi.fn() };
    TestBed.configureTestingModule({
      imports: [VersionsFilesBrowserComponent, NoopAnimationsModule, ...commonTestImports],
      providers: [{ provide: NotificationService, useValue: notification }, ...commonTestProviders],
    });
    fixture = TestBed.createComponent(VersionsFilesBrowserComponent);
    component = fixture.componentInstance;
  });

  it("starts with the default sider bounds and width", () => {
    expect(component.siderWidth).toBe(400);
    expect(component.MIN_SIDER_WIDTH).toBe(150);
    expect(component.MAX_SIDER_WIDTH).toBe(600);
    expect(component.MIN_SIDER_WIDTH).toBeLessThan(component.MAX_SIDER_WIDTH);
  });

  // The parent owns both toolbar flags: the browser reports a toggle and never mutates its input.
  it("reports a maximize toggle instead of owning the flag", () => {
    const emitted: boolean[] = [];
    component.isMaximizedChange.subscribe(v => emitted.push(v));

    component.isMaximized = false;
    component.onClickScaleTheView();
    component.isMaximized = true;
    component.onClickScaleTheView();

    expect(emitted).toEqual([true, false]);
    expect(component.isMaximized).toBe(true); // input left untouched
  });

  it("reports a right-bar toggle instead of owning the flag", () => {
    const emitted: boolean[] = [];
    component.isRightBarCollapsedChange.subscribe(v => emitted.push(v));

    component.isRightBarCollapsed = false;
    component.onClickHideRightBar();

    expect(emitted).toEqual([true]);
    expect(component.isRightBarCollapsed).toBe(false);
  });

  it("applies the dragged sider width on the next animation frame", async () => {
    component.onSideResize({ width: 321 } as NzResizeEvent);
    // the handler defers to requestAnimationFrame; a synchronous write would land here
    expect(component.siderWidth).toBe(400);
    await new Promise(resolve => requestAnimationFrame(() => resolve(null)));

    expect(component.siderWidth).toBe(321);
  });

  it("cancels the frame the previous resize scheduled", () => {
    const request = vi.spyOn(globalThis, "requestAnimationFrame").mockReturnValue(100);
    const cancel = vi.spyOn(globalThis, "cancelAnimationFrame");
    try {
      component.onSideResize({ width: 100 } as NzResizeEvent);
      cancel.mockClear(); // drop the initial cancel of the sentinel id

      component.onSideResize({ width: 200 } as NzResizeEvent);

      expect(cancel).toHaveBeenCalledWith(100);
    } finally {
      cancel.mockRestore();
      request.mockRestore();
    }
  });

  describe("copyCurrentFilePath", () => {
    let originalClipboardDescriptor: PropertyDescriptor | undefined;
    let writeText: ReturnType<typeof vi.fn>;

    beforeEach(() => {
      originalClipboardDescriptor = Object.getOwnPropertyDescriptor(navigator, "clipboard");
      writeText = vi.fn().mockResolvedValue(undefined);
      Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });
    });

    afterEach(() => {
      if (originalClipboardDescriptor) {
        Object.defineProperty(navigator, "clipboard", originalClipboardDescriptor);
      } else {
        delete (navigator as any).clipboard;
      }
    });

    it("writes the displayed path to the clipboard and toasts success", async () => {
      component.currentDisplayedFileName = "/a/b/c.txt";

      await component.copyCurrentFilePath();

      expect(writeText).toHaveBeenCalledWith("/a/b/c.txt");
      expect(notification.success).toHaveBeenCalledWith("File path copied to clipboard");
    });

    it("does nothing when no file is displayed", async () => {
      component.currentDisplayedFileName = "";

      await component.copyCurrentFilePath();

      expect(writeText).not.toHaveBeenCalled();
    });

    it("toasts an error when the clipboard write rejects", async () => {
      writeText.mockRejectedValue(new Error("denied"));
      component.currentDisplayedFileName = "/a/b/c.txt";

      await component.copyCurrentFilePath();

      expect(notification.error).toHaveBeenCalledWith("Failed to copy file path");
    });
  });

  it("delegates notePathStaged to the embedded uploader", () => {
    const uploader = { notePathStaged: vi.fn() };
    (component as any).versionUploader = uploader;

    component.notePathStaged("owner/ds/v1/a.txt");

    expect(uploader.notePathStaged).toHaveBeenCalledWith("owner/ds/v1/a.txt");
  });

  it("emits the version chosen in the picker", () => {
    const v1 = { name: "v1" };
    const v2 = { name: "v2" };
    const picked: Array<{ name: string } | undefined> = [];
    component.versions = [v1, v2];
    component.versionSelected.subscribe(v => picked.push(v));
    fixture.detectChanges();

    const select = fixture.debugElement.query(By.directive(NzSelectComponent));
    select.triggerEventHandler("ngModelChange", v2);

    expect(picked).toEqual([v2]);
  });

  it("shows the empty-state message until a version is selected", () => {
    component.emptyMessage = "No version is selected";
    component.selectedVersion = undefined;
    fixture.detectChanges();

    const empty = (fixture.nativeElement as HTMLElement).querySelector(".empty-version-indicator");
    expect(empty).not.toBeNull();
    expect((fixture.nativeElement as HTMLElement).textContent ?? "").toContain("No version is selected");
  });
});

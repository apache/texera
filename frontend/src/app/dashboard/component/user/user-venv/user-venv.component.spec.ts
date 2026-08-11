/*
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

import { ApplicationRef } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { By } from "@angular/platform-browser";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { DeleteOutline, FileAddOutline, PlusOutline } from "@ant-design/icons-angular/icons";
import { NzIconModule } from "ng-zorro-antd/icon";
import { ModalOptions, NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import { of, throwError } from "rxjs";

import { UserVenvComponent } from "./user-venv.component";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import {
  UserPveRecord,
  WorkflowPveService,
} from "../../../../workspace/service/virtual-environment/virtual-environment.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";

describe("UserVenvComponent", () => {
  let component: UserVenvComponent;
  let fixture: ComponentFixture<UserVenvComponent>;

  let pveServiceSpy: {
    listUserPves: ReturnType<typeof vi.fn>;
    savePve: ReturnType<typeof vi.fn>;
    updateUserPve: ReturnType<typeof vi.fn>;
    deleteUserPve: ReturnType<typeof vi.fn>;
  };
  let notificationSpy: {
    error: ReturnType<typeof vi.fn>;
    success: ReturnType<typeof vi.fn>;
    info: ReturnType<typeof vi.fn>;
    warning: ReturnType<typeof vi.fn>;
  };
  let confirmSpy: ReturnType<typeof vi.spyOn>;
  let capturedConfirmConfig: ModalOptions | undefined;
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    pveServiceSpy = {
      listUserPves: vi.fn().mockReturnValue(of([])),
      savePve: vi.fn().mockReturnValue(of({ veid: 1 })),
      updateUserPve: vi.fn().mockReturnValue(of({ veid: 1 })),
      deleteUserPve: vi.fn().mockReturnValue(of(undefined)),
    };
    notificationSpy = {
      error: vi.fn(),
      success: vi.fn(),
      info: vi.fn(),
      warning: vi.fn(),
    };
    consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);

    await TestBed.configureTestingModule({
      imports: [
        UserVenvComponent,
        NoopAnimationsModule,
        NzIconModule.forChild([FileAddOutline, DeleteOutline, PlusOutline]),
      ],
      providers: [
        NzModalService,
        { provide: WorkflowPveService, useValue: pveServiceSpy as unknown as WorkflowPveService },
        { provide: NotificationService, useValue: notificationSpy as unknown as NotificationService },
        ...commonTestProviders,
      ],
    }).compileComponents();

    // Use the real NzModalService (the rendered <nz-modal> relies on it) but capture the
    // confirm() config so tests can drive its nzOnOk callback without opening an overlay.
    capturedConfirmConfig = undefined;
    confirmSpy = vi.spyOn(TestBed.inject(NzModalService), "confirm").mockImplementation((options?: ModalOptions) => {
      capturedConfirmConfig = options;
      return {} as NzModalRef;
    });

    fixture = TestBed.createComponent(UserVenvComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    consoleErrorSpy?.mockRestore();
    confirmSpy?.mockRestore();
    fixture?.destroy();
    document.querySelectorAll(".cdk-overlay-container").forEach(c => (c.innerHTML = ""));
  });

  it("creates the component", () => {
    expect(component).toBeTruthy();
  });

  describe("ngOnInit / refreshPves", () => {
    it("maps records to drafts, parsing every version-op form, and renders the list", () => {
      const records: UserPveRecord[] = [
        {
          veid: 1,
          name: "envA",
          packages: { numpy: "==1.2", pandas: ">=2.0", scipy: "<=3.1", bare: "1.5", empty: "" },
        },
      ];
      pveServiceSpy.listUserPves.mockReturnValue(of(records));

      fixture.detectChanges(); // triggers ngOnInit -> refreshPves

      expect(pveServiceSpy.listUserPves).toHaveBeenCalledTimes(1);
      expect(component.pves.length).toBe(1);

      const rows = component.pves[0].newPackages;
      const byName = (name: string) => rows.find(r => r.name === name);
      expect(byName("numpy")).toEqual({ name: "numpy", versionOp: "==", version: "1.2" });
      expect(byName("pandas")).toEqual({ name: "pandas", versionOp: ">=", version: "2.0" });
      expect(byName("scipy")).toEqual({ name: "scipy", versionOp: "<=", version: "3.1" });
      // bare value (no operator prefix) falls back to "==" with the whole string as the version
      expect(byName("bare")).toEqual({ name: "bare", versionOp: "==", version: "1.5" });
      // empty value stays empty with the default "==" operator
      expect(byName("empty")).toEqual({ name: "empty", versionOp: "==", version: "" });

      const host = fixture.nativeElement as HTMLElement;
      const items = host.querySelectorAll("li.python-env-page-item");
      expect(items.length).toBe(1);
      expect(host.querySelector(".python-env-name")?.textContent).toContain("envA");
    });

    it("logs and notifies on the error path, leaving the list empty", () => {
      pveServiceSpy.listUserPves.mockReturnValue(throwError(() => new Error("boom")));

      fixture.detectChanges(); // triggers ngOnInit -> refreshPves error

      expect(consoleErrorSpy).toHaveBeenCalled();
      expect(notificationSpy.error).toHaveBeenCalledWith("Failed to fetch Python environments.");
      expect(component.pves).toEqual([]);
    });
  });

  describe("modal open/close and package editing", () => {
    it("showPveModal opens an empty draft", () => {
      component.showPveModal();
      expect(component.currentDraft).toEqual({ name: "", newPackages: [] });
      expect(component.pveModalVisible).toBe(true);
    });

    it("openExistingPve deep-copies the packages of the selected row", () => {
      component.pves = [{ veid: 9, name: "envX", newPackages: [{ name: "numpy", versionOp: "==", version: "1.0" }] }];

      component.openExistingPve(0);

      expect(component.pveModalVisible).toBe(true);
      expect(component.currentDraft?.veid).toBe(9);
      expect(component.currentDraft?.name).toBe("envX");
      // deep copy: neither the array nor the row objects are shared with the source
      expect(component.currentDraft?.newPackages).not.toBe(component.pves[0].newPackages);
      expect(component.currentDraft?.newPackages[0]).not.toBe(component.pves[0].newPackages[0]);

      component.currentDraft!.newPackages[0].version = "changed";
      expect(component.pves[0].newPackages[0].version).toBe("1.0");
    });

    it("openExistingPve with an out-of-range index is a no-op", () => {
      component.pves = [];
      component.openExistingPve(3);
      expect(component.currentDraft).toBeNull();
      expect(component.pveModalVisible).toBe(false);
    });

    it("closePveModal clears the draft and hides the modal", () => {
      component.currentDraft = { name: "envY", newPackages: [] };
      component.pveModalVisible = true;

      component.closePveModal();

      expect(component.currentDraft).toBeNull();
      expect(component.pveModalVisible).toBe(false);
    });

    it("addPackage is a no-op when there is no draft", () => {
      component.currentDraft = null;
      expect(() => component.addPackage()).not.toThrow();
      expect(component.currentDraft).toBeNull();
    });

    it("addPackage appends a blank row when a draft exists", () => {
      component.showPveModal();
      component.addPackage();
      expect(component.currentDraft?.newPackages).toEqual([{ name: "", versionOp: "==", version: "" }]);
    });

    it("removePackage is a no-op when there is no draft", () => {
      component.currentDraft = null;
      expect(() => component.removePackage(0)).not.toThrow();
    });

    it("removePackage removes the row at the given index", () => {
      component.showPveModal();
      component.addPackage();
      component.addPackage();
      component.currentDraft!.newPackages[0].name = "x";
      component.currentDraft!.newPackages[1].name = "y";

      component.removePackage(0);

      expect(component.currentDraft?.newPackages).toEqual([{ name: "y", versionOp: "==", version: "" }]);
    });
  });

  describe("saveEnvironment", () => {
    it("rejects an empty / whitespace name without calling the service", () => {
      component.currentDraft = { name: "   ", newPackages: [] };
      component.saveEnvironment();
      expect(notificationSpy.error).toHaveBeenCalledWith("Environment name is required.");
      expect(pveServiceSpy.savePve).not.toHaveBeenCalled();
      expect(pveServiceSpy.updateUserPve).not.toHaveBeenCalled();
      expect(component.saving).toBe(false);
    });

    it("rejects a non-alphanumeric name", () => {
      component.currentDraft = { name: "env 1", newPackages: [] };
      component.saveEnvironment();
      expect(notificationSpy.error).toHaveBeenCalledWith("Environment name must contain only letters and numbers.");
      expect(pveServiceSpy.savePve).not.toHaveBeenCalled();
    });

    it("rejects a name that duplicates a different environment", () => {
      component.pves = [{ veid: 1, name: "foo", newPackages: [] }];
      component.currentDraft = { name: "foo", newPackages: [] };
      component.saveEnvironment();
      expect(notificationSpy.error).toHaveBeenCalledWith('An environment named "foo" already exists.');
      expect(pveServiceSpy.savePve).not.toHaveBeenCalled();
    });

    it("rejects a draft with duplicate package names", () => {
      component.currentDraft = {
        name: "envZ",
        newPackages: [
          { name: "x", versionOp: "==", version: "1" },
          { name: "x", versionOp: ">=", version: "2" },
        ],
      };
      component.saveEnvironment();
      expect(notificationSpy.error).toHaveBeenCalledWith('Duplicate package "x".');
      expect(pveServiceSpy.savePve).not.toHaveBeenCalled();
    });

    it("creates a new environment: formats versions, skips blank rows, then succeeds", () => {
      component.currentDraft = {
        name: "envNew",
        newPackages: [
          { name: "a", versionOp: ">=", version: "1.0" },
          { name: "b", versionOp: "==", version: "" },
          { name: "  ", versionOp: "==", version: "9" },
          { name: "c", versionOp: "<=", version: "  " },
        ],
      };
      component.pveModalVisible = true;
      pveServiceSpy.savePve.mockReturnValue(of({ veid: 5 }));
      pveServiceSpy.listUserPves.mockReturnValue(of([]));

      component.saveEnvironment();

      // blank-name row skipped, empty/whitespace versions -> "",
      // non-empty version formatted as "<op><version>"
      expect(pveServiceSpy.savePve).toHaveBeenCalledWith("envNew", { a: ">=1.0", b: "", c: "" });
      expect(pveServiceSpy.updateUserPve).not.toHaveBeenCalled();
      expect(notificationSpy.success).toHaveBeenCalledWith('Saved environment "envNew".');
      expect(component.saving).toBe(false);
      expect(component.currentDraft).toBeNull();
      expect(component.pveModalVisible).toBe(false);
      expect(pveServiceSpy.listUserPves).toHaveBeenCalledTimes(1); // refresh after save
    });

    it("updates an existing environment when the draft carries a veid", () => {
      component.pves = [{ veid: 7, name: "envU", newPackages: [] }];
      component.currentDraft = {
        veid: 7,
        name: "envU",
        newPackages: [{ name: "numpy", versionOp: "==", version: "1.2" }],
      };
      pveServiceSpy.updateUserPve.mockReturnValue(of({ veid: 7 }));
      pveServiceSpy.listUserPves.mockReturnValue(of([]));

      component.saveEnvironment();

      // same name on the same veid is not treated as a conflict
      expect(pveServiceSpy.updateUserPve).toHaveBeenCalledWith(7, "envU", { numpy: "==1.2" });
      expect(pveServiceSpy.savePve).not.toHaveBeenCalled();
      expect(notificationSpy.success).toHaveBeenCalledWith('Saved environment "envU".');
      expect(component.currentDraft).toBeNull();
    });

    it("handles the save error path: stops the spinner, notifies, keeps the modal open", () => {
      component.currentDraft = { name: "envErr", newPackages: [] };
      component.pveModalVisible = true;
      pveServiceSpy.savePve.mockReturnValue(throwError(() => new Error("nope")));

      component.saveEnvironment();

      expect(consoleErrorSpy).toHaveBeenCalled();
      expect(notificationSpy.error).toHaveBeenCalledWith("Failed to save Python environment.");
      expect(component.saving).toBe(false);
      expect(component.currentDraft).not.toBeNull();
      expect(component.pveModalVisible).toBe(true);
    });

    it("is a no-op when there is no draft", () => {
      component.currentDraft = null;
      component.saveEnvironment();
      expect(pveServiceSpy.savePve).not.toHaveBeenCalled();
      expect(pveServiceSpy.updateUserPve).not.toHaveBeenCalled();
      expect(component.saving).toBe(false);
    });
  });

  describe("confirmDeletePve", () => {
    it("builds a danger confirm config whose nzOnOk deletes the environment", () => {
      component.pves = [{ veid: 3, name: "foo", newPackages: [] }];
      pveServiceSpy.deleteUserPve.mockReturnValue(of(undefined));
      pveServiceSpy.listUserPves.mockReturnValue(of([]));

      component.confirmDeletePve(0);

      expect(confirmSpy).toHaveBeenCalledTimes(1);
      expect(capturedConfirmConfig?.nzTitle).toBe('Delete environment "foo"?');
      expect(capturedConfirmConfig?.nzOkText).toBe("Delete");
      expect(capturedConfirmConfig?.nzOkDanger).toBe(true);
      expect(typeof capturedConfirmConfig?.nzOnOk).toBe("function");

      (capturedConfirmConfig?.nzOnOk as () => void)();

      expect(pveServiceSpy.deleteUserPve).toHaveBeenCalledWith(3);
      expect(notificationSpy.success).toHaveBeenCalledWith('Deleted environment "foo".');
    });

    it("is a no-op for an out-of-range index", () => {
      component.pves = [];
      component.confirmDeletePve(5);
      expect(confirmSpy).not.toHaveBeenCalled();
    });
  });

  describe("deletePve", () => {
    it("does nothing when the target row is missing", () => {
      component.pves = [];
      component.deletePve(0);
      expect(pveServiceSpy.deleteUserPve).not.toHaveBeenCalled();
    });

    it("does nothing when the target has no veid", () => {
      component.pves = [{ name: "x", newPackages: [] }];
      component.deletePve(0);
      expect(pveServiceSpy.deleteUserPve).not.toHaveBeenCalled();
    });

    it("notifies success and refreshes on a successful delete", () => {
      component.pves = [{ veid: 7, name: "z", newPackages: [] }];
      pveServiceSpy.deleteUserPve.mockReturnValue(of(undefined));
      pveServiceSpy.listUserPves.mockReturnValue(of([]));

      component.deletePve(0);

      expect(pveServiceSpy.deleteUserPve).toHaveBeenCalledWith(7);
      expect(notificationSpy.success).toHaveBeenCalledWith('Deleted environment "z".');
      expect(pveServiceSpy.listUserPves).toHaveBeenCalledTimes(1); // refresh
    });

    it("logs and notifies on a failed delete", () => {
      component.pves = [{ veid: 8, name: "y", newPackages: [] }];
      pveServiceSpy.deleteUserPve.mockReturnValue(throwError(() => new Error("x")));

      component.deletePve(0);

      expect(consoleErrorSpy).toHaveBeenCalled();
      expect(notificationSpy.error).toHaveBeenCalledWith("Failed to delete Python environment.");
    });
  });

  describe("trackByVeid", () => {
    it("returns the veid, or undefined when absent", () => {
      expect(component.trackByVeid(0, { veid: 42, name: "", newPackages: [] })).toBe(42);
      expect(component.trackByVeid(1, { name: "", newPackages: [] })).toBeUndefined();
    });
  });

  // The class is well covered above; these exercise the template itself — the list
  // branches and the nz-modal body/footer, which render into the CDK overlay.
  describe("template rendering", () => {
    type Draft = NonNullable<UserVenvComponent["currentDraft"]>;

    // nz-modal renders into the overlay attached to ApplicationRef, so tick() after
    // detectChanges to flush its embedded view.
    const flushOverlay = (): void => {
      fixture.detectChanges();
      TestBed.inject(ApplicationRef).tick();
    };
    // Assert an element exists so a bad selector fails as "not found" instead of a null deref.
    const q = <E extends Element>(root: ParentNode, selector: string): E => {
      const el = root.querySelector(selector);
      expect(el, `expected to find "${selector}"`).not.toBeNull();
      return el as unknown as E;
    };
    const overlay = (): HTMLElement => q<HTMLElement>(document, ".cdk-overlay-container");
    // Pick a footer button by its label so the tests survive button reordering.
    const footerButton = (root: ParentNode, label: string): HTMLButtonElement => {
      const btn = Array.from(root.querySelectorAll<HTMLButtonElement>(".footer-all button")).find(
        b => b.textContent?.trim() === label
      );
      expect(btn, `expected a footer button labelled "${label}"`).toBeDefined();
      return btn as HTMLButtonElement;
    };

    const openModalWith = (draft: Draft): HTMLElement => {
      component.currentDraft = draft;
      component.pveModalVisible = true;
      flushOverlay();
      return overlay();
    };

    const seedList = (records: UserPveRecord[]): void => {
      pveServiceSpy.listUserPves.mockReturnValue(of(records));
      fixture.detectChanges();
    };

    it("shows the empty-state message and no list when there are no environments", () => {
      seedList([]);
      const host = fixture.nativeElement as HTMLElement;
      expect(host.querySelector(".python-env-page-empty")?.textContent).toContain("No environments yet");
      expect(host.querySelector("ul.python-env-page-list")).toBeNull();
    });

    it("opens an empty draft modal from the Create button", () => {
      fixture.detectChanges();
      fixture.debugElement.query(By.css(".create-btn")).triggerEventHandler("click", {});
      expect(component.pveModalVisible).toBe(true);
      expect(component.currentDraft).toEqual({ name: "", newPackages: [] });
    });

    it("renders a row per environment (with the unnamed fallback) and opens the row on click", () => {
      seedList([
        { veid: 1, name: "envA", packages: {} },
        { veid: 2, name: "", packages: {} },
      ] as UserPveRecord[]);

      const rows = fixture.debugElement.queryAll(By.css("li.python-env-page-item"));
      expect(rows.length).toBe(2);
      expect((fixture.nativeElement as HTMLElement).textContent).toContain("(unnamed)");

      rows[0].triggerEventHandler("click", {});
      expect(component.pveModalVisible).toBe(true);
      expect(component.currentDraft?.name).toBe("envA");
    });

    it("fires confirmDeletePve from the row delete icon and stops row-open propagation", () => {
      seedList([{ veid: 3, name: "envDel", packages: {} }] as UserPveRecord[]);
      const stopPropagation = vi.fn();
      fixture.debugElement.query(By.css(".python-env-delete-icon")).triggerEventHandler("click", { stopPropagation });
      expect(stopPropagation).toHaveBeenCalled();
      expect(confirmSpy).toHaveBeenCalledTimes(1);
      expect(component.pveModalVisible).toBe(false);
    });

    it("renders the modal form, package header, one row per package, and the footer when open", () => {
      fixture.detectChanges();
      const o = openModalWith({
        name: "envForm",
        newPackages: [
          { name: "numpy", versionOp: "==", version: "1.2" },
          { name: "pandas", versionOp: ">=", version: "2.0" },
        ],
      });

      expect(o.querySelector(".ve-form")).not.toBeNull();
      // header row (*ngIf newPackages.length > 0) + one row per package
      expect(o.querySelectorAll(".package-row").length).toBe(3);
      expect(o.querySelector(".add-btn button")).not.toBeNull();
      expect(o.querySelectorAll(".footer-all button").length).toBe(2);
    });

    it("drives the modal package controls and the Save footer button through the DOM", () => {
      fixture.detectChanges();
      const o = openModalWith({ name: "envDrive", newPackages: [{ name: "x", versionOp: "==", version: "1" }] });

      q<HTMLButtonElement>(o, ".add-btn button").click();
      flushOverlay();
      expect(component.currentDraft?.newPackages.length).toBe(2);

      q<HTMLButtonElement>(o, ".package-row .user-package-inputs button").click();
      expect(component.currentDraft?.newPackages[0].deleteToggle).toBe(true);

      footerButton(o, "Save").click();
      expect(pveServiceSpy.savePve).toHaveBeenCalledWith("envDrive", {});
    });

    it("closes the modal from the footer Close button", () => {
      fixture.detectChanges();
      const o = openModalWith({ name: "envClose", newPackages: [] });
      footerButton(o, "Close").click();
      expect(component.pveModalVisible).toBe(false);
      expect(component.currentDraft).toBeNull();
    });

    it("closes the modal on the nz-modal cancel (X / mask) output", () => {
      fixture.detectChanges();
      openModalWith({ name: "envCancel", newPackages: [] });
      fixture.debugElement.query(By.css("nz-modal")).triggerEventHandler("nzOnCancel", null);
      expect(component.pveModalVisible).toBe(false);
      expect(component.currentDraft).toBeNull();
    });
  });
});

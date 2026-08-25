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
import { BehaviorSubject, of, throwError } from "rxjs";

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

    it("treats a record with no packages as an empty package list", () => {
      pveServiceSpy.listUserPves.mockReturnValue(of([{ veid: 1, name: "envA" } as UserPveRecord]));

      fixture.detectChanges();

      expect(component.pves[0].newPackages).toEqual([]);
    });

    it("falls back to an empty version when the stored value is nullish", () => {
      // distinct from the `empty: ""` case above: "" is not nullish, so only a null
      // value reaches the `?? ""` arm
      pveServiceSpy.listUserPves.mockReturnValue(
        of([{ veid: 1, name: "envA", packages: { ghost: null } } as unknown as UserPveRecord])
      );

      fixture.detectChanges();

      expect(component.pves[0].newPackages).toEqual([{ name: "ghost", versionOp: "==", version: "" }]);
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

    it("treats a row whose version is nullish as an empty version", () => {
      component.currentDraft = {
        name: "envNull",
        newPackages: [{ name: "a", versionOp: ">=", version: null as unknown as string }],
      };
      pveServiceSpy.savePve.mockReturnValue(of({ veid: 6 }));
      pveServiceSpy.listUserPves.mockReturnValue(of([]));

      component.saveEnvironment();

      expect(pveServiceSpy.savePve).toHaveBeenCalledWith("envNull", { a: "" });
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

    it("names an environment with a blank name as (unnamed) in the confirm title", () => {
      component.pves = [{ veid: 3, name: "", newPackages: [] }];

      component.confirmDeletePve(0);

      expect(capturedConfirmConfig?.nzTitle).toBe('Delete environment "(unnamed)"?');
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

    it("reports a blank-named environment as (unnamed) on success", () => {
      component.pves = [{ veid: 9, name: "", newPackages: [] }];
      pveServiceSpy.deleteUserPve.mockReturnValue(of(undefined));
      pveServiceSpy.listUserPves.mockReturnValue(of([]));

      component.deletePve(0);

      expect(notificationSpy.success).toHaveBeenCalledWith('Deleted environment "(unnamed)".');
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
    /** Types `text` into `element` the way a user would, so [(ngModel)] writes back. */
    const type = (element: HTMLInputElement, text: string): void => {
      element.value = text;
      element.dispatchEvent(new Event("input"));
    };
    /**
     * Sends one keystroke to a single operator select. The option list is a cdk
     * virtual-scroll viewport whose elements all measure 0px in jsdom, so no
     * `.ant-select-item-option` is ever laid out to click: the keyboard is the only
     * route -- and ng-zorro reads the legacy `keyCode`, which the KeyboardEvent
     * constructor does not populate.
     */
    const pressOn = (select: HTMLElement, keyCode: number): void => {
      const event = new KeyboardEvent("keydown", { bubbles: true });
      Object.defineProperty(event, "keyCode", { get: () => keyCode });
      q<HTMLElement>(select, "nz-select-top-control").dispatchEvent(event);
      flushOverlay();
    };
    /** Opens one operator select's option panel. */
    const openSelect = (select: HTMLElement): void => {
      select.click();
      flushOverlay();
    };
    /** Steps an already-open select down one option and commits it with Enter. */
    const commitNextOption = (select: HTMLElement): void => {
      pressOn(select, 40); // ArrowDown
      pressOn(select, 13); // Enter commits
    };
    /**
     * The label ng-zorro shows for the value one select holds. Only valid once the
     * panel has been opened: ng-zorro resolves a pre-set value against its options
     * when the option list first registers, and until then the box shows its
     * placeholder instead of any label.
     */
    const selectedOperatorLabel = (select: HTMLElement): string | undefined =>
      q<HTMLElement>(select, "nz-select-item.ant-select-selection-item").textContent?.trim();

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
      // The empty-state banner is the complementary arm of the same *ngIf pair: it must
      // be gone, or a widened predicate would print "No environments yet" above the list.
      expect((fixture.nativeElement as HTMLElement).querySelector(".python-env-page-empty")).toBeNull();

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
      // Save is the primary affordance and Close the secondary one. The footer helper
      // finds buttons by label, so exchanging the two nzTypes would leave the discard
      // button looking like the one to press and nothing would notice.
      expect(footerButton(o, "Save").classList.contains("ant-btn-primary")).toBe(true);
      expect(footerButton(o, "Close").classList.contains("ant-btn-primary")).toBe(false);

      // The column headings, in order. Every other test finds the boxes by placeholder,
      // so nothing observed the headings themselves: swapping the two labels would leave
      // the Version column captioned "Package".
      expect(
        Array.from(o.querySelectorAll(".user-package-header-row .package-column-label")).map(e => e.textContent?.trim())
      ).toEqual(["Package", "Version"]);

      // ...and the columns those headings caption, in the same order. `nz-select` renders
      // an `<input>` of its own (.ant-select-selection-search-input), so the three columns
      // are identified by their wrapper `.field` rather than by counting inputs.
      const dataRow = q<HTMLElement>(o, ".package-row:not(.user-package-header-row)");
      const columns = Array.from(dataRow.querySelectorAll(".user-package-inputs > .field")).map(field =>
        field.querySelector("nz-select") ? "operator" : field.querySelector("input")?.getAttribute("placeholder")
      );
      expect(columns).toEqual(["Package Name", "operator", "Package Version"]);
    });

    it("drives the modal package controls and the Save footer button through the DOM", () => {
      fixture.detectChanges();
      const o = openModalWith({ name: "envDrive", newPackages: [{ name: "x", versionOp: "==", version: "1" }] });

      q<HTMLButtonElement>(o, ".add-btn button").click();
      flushOverlay();
      expect(component.currentDraft?.newPackages.length).toBe(2);

      // The row delete button is behind a popconfirm: the first click only opens the
      // popover, and the row survives until the confirm button is clicked.
      q<HTMLButtonElement>(o, ".package-row .user-package-inputs button").click();
      flushOverlay();
      expect(component.currentDraft?.newPackages.length).toBe(2);

      q<HTMLButtonElement>(overlay(), ".ant-popover-buttons button.ant-btn-primary").click();
      flushOverlay();
      expect(component.currentDraft?.newPackages.length).toBe(1);

      footerButton(o, "Save").click();
      expect(pveServiceSpy.savePve).toHaveBeenCalledWith("envDrive", {});
    });

    /**
     * The discard paths need the negative assertion as much as the positive one: a
     * successful saveEnvironment() also clears the draft and hides the dialog, so
     * "the modal closed" alone does not distinguish Close from Save. Without
     * `savePve`/`updateUserPve` asserted un-called, dismissing the dialog could
     * silently persist the edits and this suite would stay green.
     */
    it("closes the modal from the footer Close button without persisting anything", () => {
      fixture.detectChanges();
      const o = openModalWith({ name: "envClose", newPackages: [] });
      footerButton(o, "Close").click();
      flushOverlay();
      expect(component.pveModalVisible).toBe(false);
      expect(component.currentDraft).toBeNull();
      // The dialog must actually leave the DOM, not merely flip the field.
      expect(document.querySelector(".footer-all")).toBeNull();
      expect(pveServiceSpy.savePve).not.toHaveBeenCalled();
      expect(pveServiceSpy.updateUserPve).not.toHaveBeenCalled();
    });

    it("closes the modal on the nz-modal cancel (X / mask) output without persisting anything", () => {
      fixture.detectChanges();
      openModalWith({ name: "envCancel", newPackages: [] });
      fixture.debugElement.query(By.css("nz-modal")).triggerEventHandler("nzOnCancel", null);
      flushOverlay();
      expect(component.pveModalVisible).toBe(false);
      expect(component.currentDraft).toBeNull();
      expect(document.querySelector(".footer-all")).toBeNull();
      expect(pveServiceSpy.savePve).not.toHaveBeenCalled();
      expect(pveServiceSpy.updateUserPve).not.toHaveBeenCalled();
    });

    /**
     * Every test above sets the draft on the instance, so none of the [(ngModel)]
     * boxes in the modal body had ever been typed into: the write-back direction of
     * the name, package-name, version and operator controls was entirely unpinned,
     * and a binding pointed at the wrong field would have gone unnoticed.
     */
    it("writes the typed environment name into the draft, enabling Save", () => {
      fixture.detectChanges();
      const o = openModalWith({ name: "   ", newPackages: [] });

      // A whitespace-only name is the state showPveModal() leaves behind, and
      // saveEnvironment() rejects it, so Save must not be offered yet.
      expect(footerButton(o, "Save").disabled).toBe(true);

      // With no packages the column-header row must not render at all.
      expect(o.querySelectorAll(".package-row").length).toBe(0);
      expect(o.querySelector(".user-package-header-row")).toBeNull();

      const nameInput = q<HTMLInputElement>(o, ".ve-form input.fieldInput");
      expect(nameInput.getAttribute("placeholder")).toBe("Environment Name");
      type(nameInput, "envTyped");
      flushOverlay();

      expect(component.currentDraft?.name).toBe("envTyped");
      expect(footerButton(overlay(), "Save").disabled).toBe(false);
    });

    it("writes the typed package name and version into the row", () => {
      fixture.detectChanges();
      const o = openModalWith({ name: "envRow", newPackages: [{ name: "", versionOp: "==", version: "" }] });

      // Distinct values that could not be confused, so a name/version swap fails.
      type(q<HTMLInputElement>(o, 'input[placeholder="Package Name"]'), "pandas");
      type(q<HTMLInputElement>(o, 'input[placeholder="Package Version"]'), "2.5");
      flushOverlay();

      expect(component.currentDraft?.newPackages).toEqual([{ name: "pandas", versionOp: "==", version: "2.5" }]);
      // The environment name is a different binding and must be untouched.
      expect(component.currentDraft?.name).toBe("envRow");
    });

    it("picks a version operator from the select with the keyboard, showing the label it stored", () => {
      fixture.detectChanges();
      const o = openModalWith({ name: "envOp", newPackages: [{ name: "pandas", versionOp: "==", version: "2.5" }] });
      const select = q<HTMLElement>(o, "nz-select");

      // Each step pairs the value written to the model with the label the user reads
      // back, walking all three options. Asserting the model alone pins the nzValue
      // order but says nothing about nzLabel: exchanging two options' labels (values
      // untouched) would show "==" in the box while ">=" went into the requirement
      // string, which is the entire user-visible meaning of this control.
      openSelect(select);
      expect(selectedOperatorLabel(select)).toBe("==");

      commitNextOption(select); // "==" -> ">="
      expect(component.currentDraft?.newPackages[0].versionOp).toBe(">=");
      expect(selectedOperatorLabel(select)).toBe(">=");

      openSelect(select);
      commitNextOption(select); // ">=" -> "<="
      expect(component.currentDraft?.newPackages[0].versionOp).toBe("<=");
      expect(selectedOperatorLabel(select)).toBe("<=");

      footerButton(overlay(), "Save").click();
      expect(pveServiceSpy.savePve).toHaveBeenCalledWith("envOp", { pandas: "<=2.5" });
    });

    it("opens and deletes the row that was clicked, not the first one", () => {
      seedList([
        { veid: 1, name: "envFirst", packages: {} },
        { veid: 2, name: "envSecond", packages: { numpy: "==1.0" } },
      ] as UserPveRecord[]);
      const rows = fixture.debugElement.queryAll(By.css("li.python-env-page-item"));
      expect(rows.length).toBe(2);

      // Clicking the second row must open the second environment, not index 0.
      rows[1].triggerEventHandler("click", {});
      expect(component.currentDraft?.veid).toBe(2);
      expect(component.currentDraft?.name).toBe("envSecond");
      component.closePveModal();
      flushOverlay();

      // The second row's delete icon must target the second environment.
      const icons = fixture.debugElement.queryAll(By.css(".python-env-delete-icon"));
      expect(icons.length).toBe(2);
      icons[1].triggerEventHandler("click", { stopPropagation: vi.fn() });
      expect(capturedConfirmConfig?.nzTitle).toBe('Delete environment "envSecond"?');
      (capturedConfirmConfig?.nzOnOk as () => void)();
      expect(pveServiceSpy.deleteUserPve).toHaveBeenCalledWith(2);
    });

    it("reuses the row element for an unchanged veid across a refresh (trackBy)", () => {
      const feed = new BehaviorSubject<UserPveRecord[]>([
        { veid: 1, name: "envKeep", packages: {} },
        { veid: 2, name: "envAlso", packages: {} },
      ] as UserPveRecord[]);
      pveServiceSpy.listUserPves.mockReturnValue(feed);
      fixture.detectChanges();
      const before = fixture.debugElement.queryAll(By.css("li.python-env-page-item"))[0].nativeElement;

      // Fresh record objects, same veids: trackByVeid must keep the existing DOM node.
      feed.next([
        { veid: 1, name: "envKeep", packages: {} },
        { veid: 2, name: "envAlso", packages: {} },
      ] as UserPveRecord[]);
      flushOverlay();
      const after = fixture.debugElement.queryAll(By.css("li.python-env-page-item"))[0].nativeElement;
      expect(after).toBe(before);
    });

    it("writes each package row through its own index, and removes the row that was confirmed", () => {
      fixture.detectChanges();
      const o = openModalWith({
        name: "envRows",
        newPackages: [
          { name: "first", versionOp: "==", version: "1" },
          { name: "", versionOp: "==", version: "" },
        ],
      });

      const nameBoxes = o.querySelectorAll<HTMLInputElement>('input[placeholder="Package Name"]');
      const versionBoxes = o.querySelectorAll<HTMLInputElement>('input[placeholder="Package Version"]');
      expect(nameBoxes.length).toBe(2);
      // Typing in the SECOND row must not land in the first.
      type(nameBoxes[1], "second");
      type(versionBoxes[1], "2");
      flushOverlay();
      expect(component.currentDraft?.newPackages).toEqual([
        { name: "first", versionOp: "==", version: "1" },
        { name: "second", versionOp: "==", version: "2" },
      ]);

      // The operator select is the one control whose per-index binding a single-row
      // draft cannot check, because there index 0 and a hardcoded [0] agree. Drive the
      // SECOND row's select; the first row must not move, in the model or on screen.
      const selects = o.querySelectorAll<HTMLElement>(".package-row:not(.user-package-header-row) nz-select");
      expect(selects.length).toBe(2);
      openSelect(selects[1]);
      commitNextOption(selects[1]); // "==" -> ">=" on the second row only
      expect(component.currentDraft?.newPackages[1].versionOp).toBe(">=");
      expect(selectedOperatorLabel(selects[1])).toBe(">=");
      // The row the user did not touch keeps its own operator: a select bound to a
      // hardcoded row index instead of `i` fails here.
      expect(component.currentDraft?.newPackages[0].versionOp).toBe("==");

      // Confirming the SECOND row's delete must remove that row, not the first.
      const removeButtons = o.querySelectorAll<HTMLButtonElement>(".package-row .user-package-inputs button");
      expect(removeButtons.length).toBe(2);
      removeButtons[1].click();
      flushOverlay();
      q<HTMLButtonElement>(overlay(), ".ant-popover-buttons button.ant-btn-primary").click();
      flushOverlay();
      expect(component.currentDraft?.newPackages).toEqual([{ name: "first", versionOp: "==", version: "1" }]);
    });

    it("shows the Save button's loading state while a save is in flight", () => {
      fixture.detectChanges();
      const o = openModalWith({ name: "envSaving", newPackages: [] });
      expect(footerButton(o, "Save").classList.contains("ant-btn-loading")).toBe(false);
      component.saving = true;
      flushOverlay();
      expect(footerButton(overlay(), "Save").classList.contains("ant-btn-loading")).toBe(true);
    });

    /**
     * DOCUMENTATION, NOT COVERAGE. `saveEnvironment()` (user-venv.component.ts:173) calls
     * `draft.name.trim()` with no guard, so the `?.` chain in the Save button's
     * `[disabled]` is the only thing standing between a name-less draft and a TypeError
     * -- the template defends against a state the class does not. This test pins the
     * defence, which is the correct behaviour; it deliberately does NOT assert the
     * TypeError, which would cement the defect.
     *
     * The state it needs is unreachable through the product: `virtual_environments.name`
     * is `VARCHAR(128) NOT NULL` (sql/texera_ddl.sql:279) and `UserPveRecord.name` is typed
     * `string`, hence the off-type cast below. So the branch arm this reaches is an
     * unreachable partial -- a null guard on a non-nullable value -- and the line and
     * arm it moves are excluded from this bundle's claimed coverage gain. Everything
     * else about that line (the trim, disabled-vs-enabled) is already pinned by the
     * whitespace-name test above.
     */
    it("keeps Save disabled when the stored environment name is null", () => {
      seedList([{ veid: 1, name: null, packages: {} } as unknown as UserPveRecord]);
      fixture.debugElement.query(By.css("li.python-env-page-item")).triggerEventHandler("click", {});
      flushOverlay();

      expect(component.currentDraft?.name).toBeNull();
      expect(footerButton(overlay(), "Save").disabled).toBe(true);
      // Close stays available, so the user is not trapped in the dialog.
      expect(footerButton(overlay(), "Close").disabled).toBe(false);
    });
  });
});

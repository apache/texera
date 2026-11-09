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

import { ComponentFixture, fakeAsync, TestBed, tick } from "@angular/core/testing";
import { By } from "@angular/platform-browser";
import { DebugElement } from "@angular/core";
import { LeftPanelComponent } from "./left-panel.component";
import { mockPoint, mockScanPredicate } from "../../service/workflow-graph/model/mock-workflow-data";
import { VersionsListComponent } from "./versions-list/versions-list.component";
import { OperatorMenuComponent } from "./operator-menu/operator-menu.component";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { RouterTestingModule } from "@angular/router/testing";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { PanelService } from "../../service/panel/panel.service";
import { NzResizeEvent } from "ng-zorro-antd/resizable";
import { CdkDragDrop } from "@angular/cdk/drag-drop";

const PANEL_LOCAL_STORAGE_KEYS = [
  "left-panel-width",
  "left-panel-height",
  "left-panel-order",
  "left-panel-index",
  "left-panel-style",
];

describe("LeftPanelComponent", () => {
  let component: LeftPanelComponent;

  let workflowActionService: WorkflowActionService;
  let fixture: ComponentFixture<LeftPanelComponent>;

  const clearPanelStorage = () => PANEL_LOCAL_STORAGE_KEYS.forEach(key => localStorage.removeItem(key));

  // Ensure the constructor reads a clean slate so order/index/width defaults are deterministic.
  beforeEach(() => clearPanelStorage());

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [LeftPanelComponent, HttpClientTestingModule, RouterTestingModule.withRoutes([])],
      providers: [
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        ...commonTestProviders,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LeftPanelComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.inject(WorkflowActionService);
    fixture.detectChanges();
  });

  afterEach(() => {
    fixture.destroy();
    clearPanelStorage();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("should switch to versions frame component when get all versions is clicked", fakeAsync(() => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add one operator
    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    // highlight the first operator
    jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);

    //the operator shall be highlighted
    expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs().length).toBe(1);

    // click on versions display
    component.openFrame(2);
    new VersionsListComponent(workflowActionService, {} as never, { snapshot: { params: {} } } as never).ngOnInit();

    // all the elements shall be un-highlighted
    expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs().length).toBe(0);
    expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedLinkIDs().length).toBe(0);

    // the component should switch to versions display
    expect(component.currentComponent).toBe(VersionsListComponent);
  }));

  it("openFrame(0) collapses the panel to the docked bar", () => {
    // simulate an already-open panel
    component.width = 250;
    component.height = 500;

    component.openFrame(0);

    expect(component.width).toBe(0);
    expect(component.height).toBe(65);
    expect(component.currentIndex).toBe(0);
    expect(component.currentComponent).toBeNull();
    expect(component.title).toBe("");
  });

  it("openFrame re-opens a collapsed panel using MIN_PANEL_WIDTH and minPanelHeight", () => {
    // start collapsed (width 0)
    component.openFrame(0);
    expect(component.width).toBe(0);

    component.minPanelHeight = 333;
    component.openFrame(1);

    // the collapsed -> open branch restores default width and uses minPanelHeight
    expect(component.width).toBe(230);
    expect(component.height).toBe(333);
    expect(component.currentIndex).toBe(1);
    expect(component.currentComponent).toBe(component.items[1].component);
    expect(component.title).toBe("Operators");
  });

  it("openFrame preserves the current width when switching frames on an already-open panel", () => {
    // open the panel, then simulate a user-resized width
    component.openFrame(1);
    component.width = 400;

    component.openFrame(3);

    // width must be left untouched because the panel is already open
    expect(component.width).toBe(400);
    expect(component.currentIndex).toBe(3);
    expect(component.currentComponent).toBe(component.items[3].component);
    expect(component.title).toBe(component.items[3].title);
  });

  it("constructor falls back to the Operators frame when the saved index points to a disabled tab", () => {
    // index 4 (Execution History) is disabled in the mock GUI config
    localStorage.setItem("left-panel-index", "4");

    const freshFixture = TestBed.createComponent(LeftPanelComponent);
    const fresh = freshFixture.componentInstance;

    expect(fresh.currentIndex).toBe(1);
    expect(fresh.currentComponent).toBe(fresh.items[1].component);

    freshFixture.destroy();
  });

  it("onDrop reorders the tab order array in place", () => {
    // default order is [1, 2, 3, 4, 5]
    expect(component.order).toEqual([1, 2, 3, 4, 5]);

    component.onDrop({ previousIndex: 0, currentIndex: 2 } as CdkDragDrop<string[]>);

    expect(component.order).toEqual([2, 3, 1, 4, 5]);
  });

  it("onResize applies the new dimensions through requestAnimationFrame", () => {
    const rafSpy = vi.spyOn(window, "requestAnimationFrame").mockImplementation((cb: FrameRequestCallback): number => {
      cb(0);
      return 1;
    });
    const cafSpy = vi.spyOn(window, "cancelAnimationFrame").mockImplementation(() => {});
    try {
      component.onResize({ width: 321, height: 654 } as NzResizeEvent);

      expect(component.width).toBe(321);
      expect(component.height).toBe(654);
    } finally {
      rafSpy.mockRestore();
      cafSpy.mockRestore();
    }
  });

  it("resetPanelPosition docks the panel back onto its return position", () => {
    component.returnPosition = { x: 12, y: 34 };
    component.dragPosition = { x: 99, y: 99 };
    component.isDocked = false;

    component.resetPanelPosition();

    expect(component.dragPosition).toEqual({ x: 12, y: 34 });
    expect(component.isDocked).toBe(true);
  });

  it("handleDragStart marks the panel as undocked", () => {
    component.isDocked = true;

    component.handleDragStart();

    expect(component.isDocked).toBe(false);
  });

  it("closePanelStream collapses the panel via openFrame(0)", () => {
    const panelService = TestBed.inject(PanelService);
    component.openFrame(1);
    expect(component.width).toBeGreaterThan(0);

    panelService.closePanels();

    expect(component.width).toBe(0);
    expect(component.currentIndex).toBe(0);
    expect(component.currentComponent).toBeNull();
  });

  it("resetPanelStream resets the position and re-opens the Operators frame", () => {
    const panelService = TestBed.inject(PanelService);
    component.returnPosition = { x: 5, y: 6 };
    component.dragPosition = { x: 50, y: 60 };
    component.isDocked = false;

    panelService.resetPanels();

    expect(component.isDocked).toBe(true);
    expect(component.dragPosition).toEqual({ x: 5, y: 6 });
    expect(component.currentIndex).toBe(1);
    expect(component.currentComponent).toBe(OperatorMenuComponent);
  });

  it("ngAfterViewInit sizes minPanelHeight/height from the top-level operator categories", fakeAsync(() => {
    const contentEl = component.content.nativeElement;
    // inject two top-level category panels with a known clientHeight
    for (let i = 0; i < 2; i++) {
      const panel = document.createElement("nz-collapse-panel");
      panel.classList.add("operator-group");
      panel.setAttribute("data-depth", "0");
      Object.defineProperty(panel, "clientHeight", { value: 100, configurable: true });
      contentEl.appendChild(panel);
    }

    component.ngAfterViewInit();
    tick(); // flush the setTimeout(..., 0)

    // 100 + 100 = 200 measured height, + 90 padding
    expect(component.minPanelHeight).toBe(290);
    expect(component.height).toBe(290);
  }));

  it("ngAfterViewInit leaves the height untouched when there are no top-level categories", fakeAsync(() => {
    const originalMin = component.minPanelHeight;
    const originalHeight = component.height;

    // content has no matching nz-collapse-panel[data-depth="0"] elements
    component.ngAfterViewInit();
    tick();

    expect(component.minPanelHeight).toBe(originalMin);
    expect(component.height).toBe(originalHeight);
  }));

  it("persists panel state to localStorage on the beforeunload host listener", () => {
    component.width = 111;
    component.height = 222;
    component.currentIndex = 2;

    window.dispatchEvent(new Event("beforeunload"));

    expect(localStorage.getItem("left-panel-width")).toBe("111");
    expect(localStorage.getItem("left-panel-height")).toBe("222");
    expect(localStorage.getItem("left-panel-index")).toBe("2");
    expect(localStorage.getItem("left-panel-order")).toBe(String(component.order));
  });

  it("ngOnDestroy skips style persistence when the left-container element is absent", () => {
    localStorage.removeItem("left-panel-style");
    const getByIdSpy = vi.spyOn(document, "getElementById").mockReturnValue(null);

    component.ngOnDestroy();

    // style is only written when the container exists
    expect(localStorage.getItem("left-panel-style")).toBeNull();
    // the remaining keys are still persisted unconditionally
    expect(localStorage.getItem("left-panel-width")).not.toBeNull();
    expect(localStorage.getItem("left-panel-index")).not.toBeNull();

    getByIdSpy.mockRestore();
  });

  it("constructor restores a valid saved tab order from localStorage", () => {
    // a permutation whose value-set matches the default order's set is accepted
    localStorage.setItem("left-panel-order", "5,4,3,2,1");

    const freshFixture = TestBed.createComponent(LeftPanelComponent);
    const fresh = freshFixture.componentInstance;

    expect(fresh.order).toEqual([5, 4, 3, 2, 1]);

    freshFixture.destroy();
  });

  it("constructor ignores a saved tab order whose unique-entry count differs from the default", () => {
    // the guard compares Set sizes only: a duplicate/short set has a different unique count -> fall back to the default order
    localStorage.setItem("left-panel-order", "1,1,1");

    const freshFixture = TestBed.createComponent(LeftPanelComponent);
    const fresh = freshFixture.componentInstance;

    expect(fresh.order).toEqual([1, 2, 3, 4, 5]);

    freshFixture.destroy();
  });

  // ── Rendered template: tab lists + their (click)/event bindings ──
  describe("template tab rendering & interactions", () => {
    // Draggable tabs render with the CDK `cdk-drag` class; the collapse "minus"
    // button does not. Enabled tabs render in `order` sequence, so the frame's
    // list position is its index among the currently-enabled frames.
    const tabForFrame = (containerId: string, frame: number): DebugElement | undefined => {
      const tabs = fixture.debugElement.queryAll(By.css(`#${containerId} li[nz-menu-item].cdk-drag`));
      const pos = component.order.filter(i => component.items[i].enabled).indexOf(frame);
      return pos >= 0 ? tabs[pos] : undefined;
    };
    const minusOf = (containerId: string): DebugElement | null =>
      fixture.debugElement.query(By.css(`#${containerId} li[nz-menu-item]:not(.cdk-drag)`));

    it("clicking a tab in the collapsed dock opens that frame", () => {
      component.width = 0;
      fixture.detectChanges();

      // collapsed state (width 0) -> #docked-buttons shows the enabled tabs
      const versionsTab = tabForFrame("docked-buttons", 2);
      expect(versionsTab).toBeTruthy();

      versionsTab!.triggerEventHandler("click", null);

      expect(component.currentIndex).toBe(2);
      expect(component.width).toBe(230); // collapsed -> re-opened
    });

    it("the collapsed dock renders enabled tabs and omits disabled ones", () => {
      // 1/2/3 are enabled; 4 (Execution History) is disabled in the mock GUI config
      expect(fixture.debugElement.queryAll(By.css("#docked-buttons li[nz-menu-item].cdk-drag")).length).toBe(3);
      expect(tabForFrame("docked-buttons", 1)).toBeTruthy();
      expect(tabForFrame("docked-buttons", 4)).toBeUndefined();
    });

    it("the docked-bar minus collapses the panel when it is open and undocked", () => {
      component.width = 300;
      component.isDocked = false;
      fixture.detectChanges();

      const minus = minusOf("docked-buttons");
      expect(minus).toBeTruthy();
      minus!.triggerEventHandler("click", null);

      expect(component.width).toBe(0);
      expect(component.currentIndex).toBe(0);
    });

    it("clicking a tab in the expanded dock switches frames", () => {
      component.width = 300;
      component.isDocked = false;
      fixture.detectChanges();

      const settingsTab = tabForFrame("dock", 3);
      expect(settingsTab).toBeTruthy();
      settingsTab!.triggerEventHandler("click", null);

      expect(component.currentIndex).toBe(3);
    });

    it("the expanded dock's minus collapses the panel when docked", () => {
      component.width = 300;
      component.isDocked = true;
      fixture.detectChanges();

      const minus = minusOf("dock");
      expect(minus).toBeTruthy();
      minus!.triggerEventHandler("click", null);

      expect(component.width).toBe(0);
      expect(component.currentIndex).toBe(0);
    });

    it("the return button re-docks the panel to its return position", () => {
      component.width = 300;
      component.returnPosition = { x: 7, y: 8 };
      component.dragPosition = { x: 70, y: 80 };
      component.isDocked = false;
      fixture.detectChanges();

      const resetBtn = fixture.debugElement.query(By.css("#return-button button"));
      expect(resetBtn).toBeTruthy();
      resetBtn.triggerEventHandler("click", null);

      expect(component.isDocked).toBe(true);
      expect(component.dragPosition).toEqual({ x: 7, y: 8 });
    });

    it("the return bar's minus collapses the panel", () => {
      component.width = 300;
      fixture.detectChanges();

      const minus = minusOf("return-button");
      expect(minus).toBeTruthy();
      minus!.triggerEventHandler("click", null);

      expect(component.width).toBe(0);
      expect(component.currentIndex).toBe(0);
    });

    it("wires the drop-list reorder from every rendered menu list", () => {
      component.width = 300;
      fixture.detectChanges();

      for (const id of ["docked-buttons", "dock", "return-button"]) {
        component.order = [1, 2, 3, 4, 5];
        fixture.debugElement
          .query(By.css(`#${id}`))
          .triggerEventHandler("cdkDropListDropped", { previousIndex: 0, currentIndex: 1 });
        expect(component.order).toEqual([2, 1, 3, 4, 5]);
      }
    });

    it("wires the resize and drag-start events from the left container", () => {
      component.width = 300;
      fixture.detectChanges();
      const container = fixture.debugElement.query(By.css("#left-container"));

      container.triggerEventHandler("cdkDragStarted", null);
      expect(component.isDocked).toBe(false);

      const rafSpy = vi
        .spyOn(window, "requestAnimationFrame")
        .mockImplementation((cb: FrameRequestCallback): number => {
          cb(0);
          return 1;
        });
      const cafSpy = vi.spyOn(window, "cancelAnimationFrame").mockImplementation(() => {});
      try {
        container.triggerEventHandler("nzResize", { width: 345, height: 678 });
        expect(component.width).toBe(345);
        expect(component.height).toBe(678);
      } finally {
        rafSpy.mockRestore();
        cafSpy.mockRestore();
      }
    });
  });

  it("restores the saved left-container style on its first ngOnInit", () => {
    // Destroy the default fixture so a single #left-container lives in the document, set the
    // saved style, then create a fresh component and attach it before its FIRST ngOnInit runs
    // (mirroring real usage where the value exists before init). Assert the immediate restore:
    // a subsequent change-detection pass re-applies the template's [style.width]/[style.height]
    // bindings, which would overwrite this cssText, so the restore is observed at ngOnInit time.
    fixture.destroy();
    localStorage.setItem("left-panel-style", "width: 123px;");

    const freshFixture = TestBed.createComponent(LeftPanelComponent);
    document.body.appendChild(freshFixture.nativeElement);
    try {
      freshFixture.componentInstance.ngOnInit();
      expect(document.getElementById("left-container")!.style.cssText).toContain("width: 123px");
    } finally {
      document.body.removeChild(freshFixture.nativeElement);
      freshFixture.destroy();
    }
  });
});

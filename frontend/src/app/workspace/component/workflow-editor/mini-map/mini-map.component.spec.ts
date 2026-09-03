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
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { ReplaySubject } from "rxjs";
import * as joint from "jointjs";
import { MiniMapComponent } from "./mini-map.component";
import { MAIN_CANVAS } from "../workflow-editor.component";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { JointGraphWrapper } from "../../../service/workflow-graph/model/joint-graph-wrapper";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { JointUIService } from "../../../service/joint-ui/joint-ui.service";
import { PanelService } from "../../../service/panel/panel.service";
import { UndoRedoService } from "../../../service/undo-redo/undo-redo.service";
import { WorkflowUtilService } from "../../../service/workflow-graph/util/workflow-util.service";
import { DragDropModule } from "@angular/cdk/drag-drop";
import { commonTestProviders } from "../../../../common/testing/test-utils";

/**
 * Stand-in for the main workflow-editor paper the mini-map mirrors.
 *
 * jsdom has no layout engine: `src/jsdom-svg-polyfill.ts` stubs `getScreenCTM`
 * / `getCTM` to the identity matrix and `getBBox` to a zero rect, so a real
 * jointjs paper reports zero-sized geometry and every navigator assertion would
 * read "0px" — passing because there is no layout rather than because the
 * formula is right. This stub reports explicit, distinguishable geometry
 * instead, and records the calls the component makes against it.
 */
class StubPaper {
  public readonly handlers: Record<string, () => void> = {};
  public readonly pageToLocalPointArgs: { x: number; y: number }[] = [];
  public readonly translateArgs: [number, number][] = [];
  /** Local-coordinate point the next `pageToLocalPoint` call resolves to. */
  public localPoint = { x: 0, y: 0 };
  /** Current paper offset reported by the no-arg `translate()` getter. */
  public offset = { tx: 0, ty: 0 };

  constructor(
    private readonly sx: number = 1,
    private readonly sy: number = 1
  ) {}

  on(event: string, handler: () => void): void {
    this.handlers[event] = handler;
  }

  scale(): { sx: number; sy: number } {
    return { sx: this.sx, sy: this.sy };
  }

  pageToLocalPoint(point: { x: number; y: number }): { x: number; y: number } {
    this.pageToLocalPointArgs.push(point);
    return this.localPoint;
  }

  translate(tx?: number, ty?: number): { tx: number; ty: number } | void {
    if (tx === undefined) return this.offset;
    this.translateArgs.push([tx, ty as number]);
  }
}

/**
 * Regression coverage for the mini-map: the persisted show/hide flag, the
 * navigator overlay geometry (which mirrors the main paper's viewport onto the
 * mini-map), drag-to-pan, and the zoom/center toolbar buttons.
 *
 * Breakage this catches: dropping/renaming the "mini-map" localStorage key so
 * the collapsed state no longer survives a reload; unsubscribing the mini-map
 * from the main paper's translate/scale/resize events so the navigator freezes;
 * sign or scale errors in the navigator-position and drag-to-pan formulas;
 * losing the zoom-limit guards so the toolbar can push the zoom ratio past
 * ZOOM_MINIMUM / ZOOM_MAXIMUM; and the panel service's close/reset streams no
 * longer hiding/showing the mini-map.
 */
describe("MiniMapComponent", () => {
  let fixture: ComponentFixture<MiniMapComponent>;
  let component: MiniMapComponent;
  let workflowActionService: WorkflowActionService;
  let panelService: PanelService;
  let editorStub: HTMLDivElement | undefined;
  let mainPaper$: ReplaySubject<joint.dia.Paper>;

  /**
   * `localStorage` is a jsdom global shared by every test in this file, and
   * Angular's automatic fixture teardown runs `ngOnDestroy` — which writes
   * `mini-map` — after each one. Seed and clear it explicitly so the persisted
   * flag under test is the one this test wrote, not the previous test's.
   */
  beforeEach(() => {
    localStorage.removeItem("mini-map");
  });

  afterEach(() => {
    localStorage.removeItem("mini-map");
    editorStub?.remove();
    editorStub = undefined;
  });

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      providers: [
        WorkflowActionService,
        WorkflowUtilService,
        JointUIService,
        UndoRedoService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        ...commonTestProviders,
      ],
      imports: [MiniMapComponent, HttpClientTestingModule, DragDropModule],
    }).compileComponents();
  });

  // The fixture is created but NOT change-detected here: ngAfterViewInit reads
  // the mini-map container's size and the persisted flag, so each test sets its
  // own environment up before triggering it.
  beforeEach(() => {
    fixture = TestBed.createComponent(MiniMapComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.inject(WorkflowActionService);
    panelService = TestBed.inject(PanelService);

    // Own the paper stream rather than casting the wrapper's getter back to a Subject.
    // `getMainJointPaperAttachedStream()` is declared `Observable<Paper>`, so calling
    // `.next()` on its result only works because it happens to be a ReplaySubject today;
    // switching it to `.asObservable()` would break the spec silently. Stubbing the
    // getter depends on the declared type only. ReplaySubject(1) so a paper attached
    // before the component subscribes in ngAfterViewInit is still delivered.
    mainPaper$ = new ReplaySubject<joint.dia.Paper>(1);
    vi.spyOn(workflowActionService.getJointGraphWrapper(), "getMainJointPaperAttachedStream").mockReturnValue(
      mainPaper$.asObservable()
    );
  });

  /** Gives the mini-map container a size, which jsdom otherwise reports as 0. */
  function sizeMiniMapContainer(width: number, height: number): HTMLElement {
    const map = fixture.nativeElement.querySelector("#mini-map") as HTMLElement;
    Object.defineProperty(map, "offsetWidth", { value: width, configurable: true });
    Object.defineProperty(map, "offsetHeight", { value: height, configurable: true });
    return map;
  }

  /**
   * The mini-map reads the main editor's element out of the document by id, so
   * mount a stand-in with an explicit size and viewport rect.
   */
  function mountWorkflowEditorStub(width: number, height: number, left: number, top: number): HTMLDivElement {
    const editor = document.createElement("div");
    editor.id = "workflow-editor";
    Object.defineProperty(editor, "offsetWidth", { value: width, configurable: true });
    Object.defineProperty(editor, "offsetHeight", { value: height, configurable: true });
    editor.getBoundingClientRect = () => ({ left, top, right: left + width, bottom: top + height }) as DOMRect;
    document.body.appendChild(editor);
    editorStub = editor;
    return editor;
  }

  /** Publishes `paper` on the stream the mini-map subscribes to in ngAfterViewInit. */
  function attachMainPaper(paper: StubPaper): void {
    mainPaper$.next(paper as unknown as joint.dia.Paper);
  }

  it("should create", () => {
    fixture.detectChanges();
    expect(fixture.componentInstance).toBeTruthy();
  });

  describe("mini-map paper", () => {
    it("fits the whole main canvas into the mini-map container", () => {
      // 912 / (2688 - -960) == 0.25; the height (100) is deliberately different
      // so a width/height mix-up in the scale formula cannot pass.
      const map = sizeMiniMapContainer(912, 100);

      fixture.detectChanges();

      expect(MAIN_CANVAS.xMax - MAIN_CANVAS.xMin).toBe(3648);
      expect(component.scale).toBe(0.25);
      // The paper is sized to the container it renders into ...
      expect(map.style.width).toBe("912px");
      expect(map.style.height).toBe("100px");
      // ... and shifted so the canvas' top-left corner (-960, -540) lands on the
      // container's origin: 960 * 0.25 == 240, 540 * 0.25 == 135.
      expect(map.querySelector("g.joint-layers")?.getAttribute("transform")).toBe("matrix(0.25,0,0,0.25,240,135)");
    });
  });

  describe("persisted hidden state", () => {
    it("starts visible when nothing has been persisted", () => {
      fixture.detectChanges();
      expect(component.hidden).toBe(false);
    });

    it("restores the collapsed state persisted by the previous session", () => {
      localStorage.setItem("mini-map", "true");

      // `hidden` is assigned inside ngAfterViewInit, i.e. after the template
      // that reads it has been checked, so the dev-mode verification pass would
      // report NG0100 for a state the component legitimately restores. Skip it.
      fixture.detectChanges(false);

      expect(component.hidden).toBe(true);
    });

    it("persists the collapsed state on destroy and restores it into a fresh instance", () => {
      fixture.detectChanges();
      component.hidden = true;

      fixture.destroy();

      // Round-trip through the real storage key: a rename on either the write
      // or the read side breaks this.
      expect(localStorage.getItem("mini-map")).toBe("true");
      const reopened = TestBed.createComponent(MiniMapComponent);
      reopened.detectChanges(false);
      expect(reopened.componentInstance.hidden).toBe(true);
    });

    it("persists the collapsed state when the window unloads", () => {
      fixture.detectChanges();
      component.hidden = true;

      // @HostListener("window:beforeunload") — the tab can close without Angular
      // ever destroying the component, so the flag has to be written here too.
      window.dispatchEvent(new Event("beforeunload"));

      expect(localStorage.getItem("mini-map")).toBe("true");
    });
  });

  describe("panel service integration", () => {
    it("hides on closePanels and shows again on resetPanels", () => {
      fixture.detectChanges();
      expect(component.hidden).toBe(false);

      panelService.closePanels();
      expect(component.hidden).toBe(true);

      panelService.resetPanels();
      expect(component.hidden).toBe(false);
    });
  });

  describe("navigator overlay", () => {
    const MINI_MAP_SCALE = 0.25;

    /**
     * Wires a stub main paper to a change-detected component and pins an
     * explicit mini-map scale (ngAfterViewInit computes 0 under jsdom, which
     * would make every "0px" assertion vacuously true).
     */
    function attachStubbedViewport(): { paper: StubPaper; navigator: HTMLElement } {
      mountWorkflowEditorStub(800, 600, 30, 40);
      fixture.detectChanges();
      component.scale = MINI_MAP_SCALE;

      const navigator = document.getElementById("mini-map-navigator") as HTMLElement;
      // cdkDrag leaves a transform behind after a drag; the component must clear
      // it before writing left/top, or the two offsets would stack.
      navigator.style.transform = "translate3d(11px, 13px, 0)";

      // sx and sy differ so a width/height mix-up cannot pass.
      const paper = new StubPaper(2, 4);
      paper.localPoint = { x: -160, y: -140 };
      attachMainPaper(paper);

      return { paper, navigator };
    }

    it("positions and sizes the navigator from the main paper's viewport", () => {
      const { paper, navigator } = attachStubbedViewport();

      expect(component.paper).toBe(paper as unknown as joint.dia.Paper);
      // The viewport origin is the editor's top-left corner in page coordinates.
      expect(paper.pageToLocalPointArgs).toEqual([{ x: 30, y: 40 }]);
      // (-160 - -960) * 0.25 and (-140 - -540) * 0.25
      expect(navigator.style.left).toBe("200px");
      expect(navigator.style.top).toBe("100px");
      // (800 / 2) * 0.25 and (600 / 4) * 0.25
      expect(navigator.style.width).toBe("100px");
      expect(navigator.style.height).toBe("37.5px");
      expect(navigator.style.transform).toBe("");
    });

    it("repositions the navigator when the main paper translates, scales or resizes", () => {
      const { paper, navigator } = attachStubbedViewport();
      expect(Object.keys(paper.handlers).sort()).toEqual(["resize", "scale", "translate"]);

      const movements: [string, number, string][] = [
        ["translate", -560, "100px"],
        ["scale", -360, "150px"],
        ["resize", -60, "225px"],
      ];
      for (const [event, localX, expectedLeft] of movements) {
        paper.localPoint = { x: localX, y: -140 };
        paper.handlers[event]();
        expect(navigator.style.left).toBe(expectedLeft);
      }
    });

    it("leaves the navigator alone while the user is dragging it", () => {
      const { paper, navigator } = attachStubbedViewport();
      expect(navigator.style.left).toBe("200px");

      // The drag itself is already moving the navigator; echoing the paper's
      // translate back onto it would fight the pointer.
      component.dragging = true;
      paper.localPoint = { x: -560, y: -140 };
      paper.handlers["translate"]();

      expect(navigator.style.left).toBe("200px");
    });
  });

  describe("drag to pan", () => {
    it("pans the main paper opposite the pointer, in main-canvas units", () => {
      fixture.detectChanges();
      const paper = new StubPaper();
      paper.offset = { tx: 100, ty: 50 };
      component.paper = paper as unknown as joint.dia.Paper;
      component.scale = 0.25;

      component.onDrag({ event: { movementX: 10, movementY: -20 } });

      // A pointer delta on the mini-map is worth 1/scale as much on the main
      // canvas, and the paper moves against the pointer.
      expect(paper.translateArgs).toEqual([[100 - 40, 50 + 80]]);
    });
  });

  describe("zoom buttons", () => {
    let jointGraphWrapper: JointGraphWrapper;

    beforeEach(() => {
      fixture.detectChanges();
      jointGraphWrapper = workflowActionService.getJointGraphWrapper();
    });

    it("zooms out by one click step", () => {
      jointGraphWrapper.setZoomProperty(1);

      component.onClickZoomOut();

      expect(jointGraphWrapper.getZoomRatio()).toBeCloseTo(1 - JointGraphWrapper.ZOOM_CLICK_DIFF, 10);
      expect(jointGraphWrapper.getZoomRatio()).toBeLessThan(1);
    });

    it("does not zoom out past the minimum ratio", () => {
      jointGraphWrapper.setZoomProperty(JointGraphWrapper.ZOOM_MINIMUM);
      const setZoomProperty = vi.spyOn(jointGraphWrapper, "setZoomProperty");

      component.onClickZoomOut();

      expect(setZoomProperty).not.toHaveBeenCalled();
      expect(jointGraphWrapper.getZoomRatio()).toBe(JointGraphWrapper.ZOOM_MINIMUM);
    });

    it("zooms in by one click step", () => {
      jointGraphWrapper.setZoomProperty(1);

      component.onClickZoomIn();

      expect(jointGraphWrapper.getZoomRatio()).toBeCloseTo(1 + JointGraphWrapper.ZOOM_CLICK_DIFF, 10);
      expect(jointGraphWrapper.getZoomRatio()).toBeGreaterThan(1);
    });

    it("does not zoom in past the maximum ratio", () => {
      jointGraphWrapper.setZoomProperty(JointGraphWrapper.ZOOM_MAXIMUM);
      const setZoomProperty = vi.spyOn(jointGraphWrapper, "setZoomProperty");

      component.onClickZoomIn();

      expect(setZoomProperty).not.toHaveBeenCalled();
      expect(jointGraphWrapper.getZoomRatio()).toBe(JointGraphWrapper.ZOOM_MAXIMUM);
    });
  });

  describe("center button", () => {
    it("broadcasts a center event and resets the navigator's drag offset", () => {
      fixture.detectChanges();
      const centerEvents: void[] = [];
      workflowActionService
        .getTexeraGraph()
        .getCenterEventStream()
        .subscribe(event => centerEvents.push(event));
      const reset = vi.spyOn(component.navigatorDrag, "reset");

      component.triggerCenter();

      expect(centerEvents).toHaveLength(1);
      // Without the reset the navigator keeps the cdkDrag transform from the
      // previous drag and lands off-centre.
      expect(reset).toHaveBeenCalledTimes(1);
    });
  });

  /**
   * Everything above drives the component's methods directly, so the template's
   * own wiring — which toolbar button calls which method, and which cdkDrag
   * output feeds onDrag / the `dragging` flag — was never executed. These tests
   * go through the rendered DOM instead, so re-pointing a (click) at the wrong
   * handler, or dropping one of the cdkDrag bindings, fails here.
   */
  describe("toolbar and drag wiring", () => {
    const button = (id: string): HTMLButtonElement =>
      fixture.nativeElement.querySelector(`#${id}`) as HTMLButtonElement;

    /** The mini-map surface, whose visibility the toggle button drives. */
    const container = (): HTMLElement => fixture.nativeElement.querySelector("#mini-map-container") as HTMLElement;

    /** ng-zorro renders <span nz-icon nzType="x"> as class "anticon-x". */
    const toggleIconType = (): string | undefined =>
      Array.from(button("minimap-button").querySelector("span[nz-icon]")!.classList)
        .find(name => name.startsWith("anticon-"))
        ?.slice("anticon-".length);

    it("collapses and re-opens the mini-map from the toolbar toggle", () => {
      fixture.detectChanges();
      expect(container().hidden).toBe(false);
      expect(toggleIconType()).toBe("minus");

      button("minimap-button").click();
      fixture.detectChanges();

      expect(container().hidden).toBe(true);
      // The glyph flips to the "show me" affordance while the map is folded away.
      expect(toggleIconType()).toBe("global");

      button("minimap-button").click();
      fixture.detectChanges();

      expect(container().hidden).toBe(false);
      expect(toggleIconType()).toBe("minus");
    });

    it("broadcasts a center event from the center button only", () => {
      fixture.detectChanges();
      const centerEvents: void[] = [];
      workflowActionService
        .getTexeraGraph()
        .getCenterEventStream()
        .subscribe(event => centerEvents.push(event));

      // The three other toolbar buttons sit next to it and must not centre.
      button("minimap-button").click();
      button("minimap-zoom-in-button").click();
      button("minimap-zoom-out-button").click();
      expect(centerEvents).toHaveLength(0);

      button("minimap-center-button").click();

      expect(centerEvents).toHaveLength(1);
    });

    it("zooms out and in from their own toolbar buttons", () => {
      fixture.detectChanges();
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();
      jointGraphWrapper.setZoomProperty(1);

      button("minimap-zoom-out-button").click();
      expect(jointGraphWrapper.getZoomRatio()).toBeCloseTo(1 - JointGraphWrapper.ZOOM_CLICK_DIFF, 10);

      // Two zoom-ins from here land one step *above* the starting ratio, so the
      // two buttons cannot be swapped without breaking this.
      button("minimap-zoom-in-button").click();
      button("minimap-zoom-in-button").click();
      expect(jointGraphWrapper.getZoomRatio()).toBeCloseTo(1 + JointGraphWrapper.ZOOM_CLICK_DIFF, 10);
    });

    it("pans the main paper from the navigator's cdkDragMoved output", () => {
      fixture.detectChanges();
      const paper = new StubPaper();
      paper.offset = { tx: 100, ty: 50 };
      component.paper = paper as unknown as joint.dia.Paper;
      component.scale = 0.25;

      fixture.debugElement
        .query(By.css("#mini-map-navigator"))
        .triggerEventHandler("cdkDragMoved", { event: { movementX: 10, movementY: -20 } });

      expect(paper.translateArgs).toEqual([[100 - 40, 50 + 80]]);
    });

    it("freezes the navigator between cdkDragStarted and cdkDragEnded", () => {
      mountWorkflowEditorStub(800, 600, 30, 40);
      fixture.detectChanges();
      component.scale = 0.25;
      const paper = new StubPaper(2, 4);
      paper.localPoint = { x: -160, y: -140 };
      attachMainPaper(paper);

      const navigator = document.getElementById("mini-map-navigator") as HTMLElement;
      const navigatorDebugElement = fixture.debugElement.query(By.css("#mini-map-navigator"));
      expect(navigator.style.left).toBe("200px");

      // While the pointer owns the navigator, echoes of the paper's own translate
      // must not fight it.
      navigatorDebugElement.triggerEventHandler("cdkDragStarted", {});
      paper.localPoint = { x: -560, y: -140 };
      paper.handlers["translate"]();
      expect(navigator.style.left).toBe("200px");

      // Once the drag ends the navigator tracks the paper again.
      navigatorDebugElement.triggerEventHandler("cdkDragEnded", {});
      paper.handlers["translate"]();
      expect(navigator.style.left).toBe("100px");
    });
  });
});

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

import {
  ChangeDetectorRef,
  Component,
  ElementRef,
  HostListener,
  Input,
  OnChanges,
  OnDestroy,
  OnInit,
  SimpleChanges,
  Type,
  ViewChild,
  ViewRef,
} from "@angular/core";
import { merge } from "rxjs";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { OperatorPropertyEditFrameComponent } from "./operator-property-edit-frame/operator-property-edit-frame.component";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { distinctUntilChanged, filter } from "rxjs/operators";
import { PortPropertyEditFrameComponent } from "./port-property-edit-frame/port-property-edit-frame.component";
import { NzResizeEvent, NzResizableDirective, NzResizeHandlesComponent } from "ng-zorro-antd/resizable";
import { calculateTotalTranslate3d } from "../../../common/util/panel-dock";
import { PanelService } from "../../service/panel/panel.service";
import { FormBindingService } from "../../service/form-binding/form-binding.service";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { NzMenuDirective, NzMenuItemComponent, NzMenuDividerDirective } from "ng-zorro-antd/menu";
import { NgClass, NgIf, NgComponentOutlet } from "@angular/common";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { CdkDrag, CdkDragHandle } from "@angular/cdk/drag-drop";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";

/**
 * PropertyEditorComponent is the panel that allows user to edit operator properties.
 * Depending on the highlighted operator or link, it displays OperatorPropertyEditFrameComponent
 * or BreakpointPropertyEditFrameComponent accordingly
 *
 */
@UntilDestroy()
@Component({
  selector: "texera-property-editor",
  templateUrl: "property-editor.component.html",
  styleUrls: ["property-editor.component.scss"],
  imports: [
    NzMenuDirective,
    NgClass,
    NgIf,
    NzMenuItemComponent,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzTooltipDirective,
    CdkDrag,
    NzResizableDirective,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    NzMenuDividerDirective,
    CdkDragHandle,
    NgComponentOutlet,
    NzResizeHandlesComponent,
  ],
})
export class PropertyEditorComponent implements OnInit, OnDestroy, OnChanges {
  @ViewChild("contentWrapper") contentWrapperRef!: ElementRef;
  protected readonly window = window;
  id = -1;
  width = 260;
  height = Math.max(300, window.innerHeight * 0.6);
  currentComponent: Type<any> | null = null;
  /**
   * Set while an author is choosing which properties the Form View offers.
   * Forwarded to the operator frame, which puts a tick box beside each property.
   */
  @Input() exposeChoosing = false;
  /** Set from the toolbar toggle on the operator canvas; the input covers the form view. */
  private choosingFromToolbar = false;

  /** The choose-what-to-expose affordance appears wherever the feature flag is on: any
   *  workflow can expose inputs to its Form View, independent of the default-view bit. Named
   *  for the flag (not a per-workflow capability) so the gating reads clearly at the call site. */
  public get formViewFeatureEnabled(): boolean {
    return this.config.env.formViewEnabled;
  }

  public toggleChoosing(): void {
    this.formBindingService.setChoosing(!this.formBindingService.isChoosing());
  }

  public get choosing(): boolean {
    return this.exposeChoosing || this.choosingFromToolbar;
  }
  componentInputs = {};
  dragPosition = { x: 0, y: 0 };
  returnPosition = { x: 0, y: 0 };
  constructor(
    public workflowActionService: WorkflowActionService,
    private changeDetectorRef: ChangeDetectorRef,
    private panelService: PanelService,
    private formBindingService: FormBindingService,
    private config: GuiConfigService
  ) {
    const width = localStorage.getItem("right-panel-width");
    if (width) this.width = Number(width);
    this.height = Number(localStorage.getItem("right-panel-height")) || this.height;
  }

  /**
   * The Form View turns tick boxes on by setting this input, and it flips whenever the author
   * enters or leaves edit mode. The frame builds its formly fields once, so without remounting
   * here the boxes only appeared if the mode was already on when the panel opened -- entering
   * edit mode with a step already selected showed none.
   */
  ngOnChanges(changes: SimpleChanges): void {
    if (changes["exposeChoosing"] && !changes["exposeChoosing"].firstChange) {
      this.remountOperatorFrame();
    }
  }

  ngOnInit(): void {
    const style = localStorage.getItem("right-panel-style");
    if (style) document.getElementById("right-container")!.style.cssText = style;
    const translates = document.getElementById("right-container")!.style.transform;
    const [xOffset, yOffset, _] = calculateTotalTranslate3d(translates);
    this.returnPosition = { x: -xOffset, y: -yOffset };
    this.registerHighlightEventsHandler();
    // The toolbar's "choose fields" toggle lives in the service so both the canvas toolbar
    // and this panel see the same state. Re-emit the frame's inputs when it changes, so tick
    // boxes appear and disappear without needing a re-selection.
    this.formBindingService.choosing$.pipe(distinctUntilChanged(), untilDestroyed(this)).subscribe(choosing => {
      const wasChoosing = this.choosingFromToolbar;
      this.choosingFromToolbar = choosing;
      // Only an actual change needs the frame rebuilt. The stream is a BehaviorSubject, so it
      // replays its current value on subscribe; remounting for that would tear the panel down
      // during the page's first change-detection pass.
      if (wasChoosing === choosing || this.currentComponent !== OperatorPropertyEditFrameComponent) {
        return;
      }
      // Rebuild the frame so the tick boxes appear/disappear (see remountOperatorFrame).
      this.remountOperatorFrame();
    });
    this.panelService.closePanelStream.pipe(untilDestroyed(this)).subscribe(() => this.closePanel());
    this.panelService.resetPanelStream.pipe(untilDestroyed(this)).subscribe(() => {
      this.resetPanelPosition();
      this.openPanel();
    });
  }

  private updateHeightBasedOnContent(): void {
    setTimeout(() => {
      const contentEl = this.contentWrapperRef?.nativeElement;
      if (contentEl) {
        const contentHeight = contentEl.scrollHeight;
        const maxHeight = this.window.innerHeight * 0.6;
        this.height = Math.min(contentHeight + 40, maxHeight);
        this.changeDetectorRef.detectChanges();
      }
    });
  }

  /**
   * Rebuild the operator frame so a changed tick-box mode takes effect. The frame builds its
   * formly fields once when created, so a new input alone would not add or remove the tick
   * boxes -- it has to be torn down and recreated. The recreation is deferred to a timer rather
   * than run straight after the teardown so it lands in its own change-detection pass (a
   * synchronous rebuild here fought the pass already running). The frame is restored before
   * detectChanges, so the panel is never left on the `null` state the template hides on; and a
   * timer that outlives the view (destroy within the same tick) is skipped, since detectChanges
   * on a destroyed view throws.
   */
  private remountOperatorFrame(): void {
    if (this.currentComponent !== OperatorPropertyEditFrameComponent) {
      return;
    }
    const inputs = { ...this.componentInputs, exposeChoosing: this.choosing };
    this.currentComponent = null;
    setTimeout(() => {
      if ((this.changeDetectorRef as ViewRef).destroyed) {
        return;
      }
      this.componentInputs = inputs;
      this.currentComponent = OperatorPropertyEditFrameComponent;
      this.changeDetectorRef.detectChanges();
    });
  }

  @HostListener("window:beforeunload")
  ngOnDestroy(): void {
    localStorage.setItem("right-panel-width", String(this.width));
    localStorage.setItem("right-panel-height", String(this.height));

    const rightContainer = document.getElementById("right-container");
    if (rightContainer) {
      localStorage.setItem("right-panel-style", rightContainer.style.cssText);
    }
  }

  /**
   * This method changes the property editor according to how operators are highlighted on the workflow editor.
   *
   * Displays the form of the highlighted operator if only one operator is highlighted;
   * Displays the form of the link breakpoint if only one link is highlighted;
   * hides the form if no operator/link is highlighted or multiple operators and/or groups and/or links are highlighted.
   */
  registerHighlightEventsHandler() {
    merge(
      this.workflowActionService.getJointGraphWrapper().getJointOperatorHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorUnhighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointGroupHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointGroupUnhighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getLinkHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getLinkUnhighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointCommentBoxHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointCommentBoxUnhighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointPortHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointPortUnhighlightStream()
    )
      .pipe(
        filter(() => this.workflowActionService.getTexeraGraph().getSyncTexeraGraph()),
        untilDestroyed(this)
      )
      .subscribe(_ => {
        const highlightedOperators = this.workflowActionService
          .getJointGraphWrapper()
          .getCurrentHighlightedOperatorIDs();
        const highlightLinks = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedLinkIDs();
        this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedCommentBoxIDs();
        const highlightedPorts = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedPortIDs();

        if (highlightedOperators.length === 1 && highlightLinks.length === 0 && highlightedPorts.length === 0) {
          this.currentComponent = OperatorPropertyEditFrameComponent;
          this.componentInputs = { currentOperatorId: highlightedOperators[0], exposeChoosing: this.choosing };
        } else if (highlightedPorts.length === 1 && highlightLinks.length === 0) {
          this.currentComponent = PortPropertyEditFrameComponent;
          this.componentInputs = { currentPortID: highlightedPorts[0] };
        } else {
          this.currentComponent = null;
          this.componentInputs = {};
          this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("currentlyEditing", undefined);
        }
        this.changeDetectorRef.detectChanges();
        this.updateHeightBasedOnContent();
      });
  }
  onResize({ width, height }: NzResizeEvent) {
    cancelAnimationFrame(this.id);
    this.id = requestAnimationFrame(() => {
      this.width = width!;
      this.height = height!;
    });
  }

  openPanel() {
    this.width = 280;
    this.height = 300;
    this.updateHeightBasedOnContent();
  }

  closePanel() {
    this.width = 0;
    this.height = 65;
  }

  resetPanelPosition() {
    this.dragPosition = { x: this.returnPosition.x, y: this.returnPosition.y };
  }
}

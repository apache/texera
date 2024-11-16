import { ChangeDetectorRef, Component, OnInit, OnDestroy, HostListener, Type } from "@angular/core";
import { merge } from "rxjs";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { OperatorPropertyEditFrameComponent } from "./operator-property-edit-frame/operator-property-edit-frame.component";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { filter } from "rxjs/operators";
import { PortPropertyEditFrameComponent } from "./port-property-edit-frame/port-property-edit-frame.component";
import { NzResizeEvent } from "ng-zorro-antd/resizable";
import { calculateTotalTranslate3d } from "../../../common/util/panel-dock";
import { PanelService } from "../../service/panel/panel.service";
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
})
export class PropertyEditorComponent implements OnInit, OnDestroy {
  protected readonly window = window;
  id = -1;
  width = 260;
  height = Math.max(300, window.innerHeight * 0.6);
  currentComponent: Type<any> | null = null;
  componentInputs = {};
  dragPosition = { x: 0, y: 0 };
  returnPosition = { x: 0, y: 0 };
  constructor(
    public workflowActionService: WorkflowActionService,
    private changeDetectorRef: ChangeDetectorRef,
    private panelService: PanelService
  ) {
    const width = localStorage.getItem("right-panel-width");
    if (width) this.width = Number(width);
    this.height = Number(localStorage.getItem("right-panel-height")) || this.height;
  }

  ngOnInit(): void {
    const style = localStorage.getItem("right-panel-style");
    if (style) document.getElementById("right-container")!.style.cssText = style;
    const translates = document.getElementById("right-container")!.style.transform;
    const [xOffset, yOffset, _] = calculateTotalTranslate3d(translates);
    this.returnPosition = { x: -xOffset, y: -yOffset };
    this.registerHighlightEventsHandler();
    this.panelService.closePanelStream.pipe(untilDestroyed(this)).subscribe(() => this.closePanel());
    this.panelService.resetPanelStream.pipe(untilDestroyed(this)).subscribe(() => {
      this.resetPanelPosition();
      this.openPanel();
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
          this.componentInputs = { currentOperatorId: highlightedOperators[0] };
        } else if (highlightedPorts.length === 1 && highlightLinks.length === 0) {
          this.currentComponent = PortPropertyEditFrameComponent;
          this.componentInputs = { currentPortID: highlightedPorts[0] };
        } else {
          this.currentComponent = null;
          this.componentInputs = {};
          this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("currentlyEditing", undefined);
        }
        this.changeDetectorRef.detectChanges();
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
  }

  closePanel() {
    this.width = 0;
    this.height = 65;
  }

  resetPanelPosition() {
    this.dragPosition = { x: this.returnPosition.x, y: this.returnPosition.y };
  }
}

import { ChangeDetectorRef, Component, OnInit, OnDestroy, HostListener, Type } from "@angular/core";
import { merge } from "rxjs";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { OperatorPropertyEditFrameComponent } from "./operator-property-edit-frame/operator-property-edit-frame.component";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { filter } from "rxjs/operators";
import { PortPropertyEditFrameComponent } from "./port-property-edit-frame/port-property-edit-frame.component";
import { NzResizeEvent } from "ng-zorro-antd/resizable";
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
  id = -1;
  width = 300;
  height = 400;
  hidden = false;

  currentComponent: Type<any> | null = null;
  componentInputs = {};

  propertyDisplay = true;
  screenWidth = window.innerWidth;
  propertyWidth = 300;
  propertyHeight = 200;
  prevHeight = 0;
  prevWidth = 0;

  constructor(
    public workflowActionService: WorkflowActionService,
    private changeDetectorRef: ChangeDetectorRef
  ) {
    const width = localStorage.getItem("property-panel-width");
    if (width) this.propertyWidth = Number(width);

    const height = localStorage.getItem("property-panel-height");
    if (height) this.propertyHeight = Number(height);

    const pWidth = localStorage.getItem("property-panel-prevWidth");
    if (pWidth) this.prevWidth = Number(pWidth);

    const pHeight = localStorage.getItem("property-panel-prevHeight");
    if (pHeight) this.prevHeight = Number(pHeight);

    const display = localStorage.getItem("property-panel-display");

    this.propertyDisplay = display === "true";
  }

  ngOnInit(): void {
    this.registerHighlightEventsHandler();
    const style = localStorage.getItem("property-panel-style");
    if (style) document.getElementById("property-editor-container")!.style.cssText = style;
  }

  @HostListener("window:beforeunload")
  ngOnDestroy(): void {
    localStorage.setItem("property-panel-width", String(this.propertyWidth));
    localStorage.setItem("property-panel-height", String(this.propertyHeight));
    localStorage.setItem("property-panel-prevWidth", String(this.prevWidth));
    localStorage.setItem("property-panel-prevHeight", String(this.prevHeight));

    if (this.propertyDisplay) {
      localStorage.setItem("property-panel-display", "true");
    } else {
      localStorage.setItem("property-panel-display", "false");
    }

    localStorage.setItem("property-panel-style", document.getElementById("property-editor-container")!.style.cssText);
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
        const highlightedGroups = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs();
        const highlightLinks = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedLinkIDs();
        this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedCommentBoxIDs();
        const highlightedPorts = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedPortIDs();

        if (
          highlightedOperators.length === 1 &&
          highlightedGroups.length === 0 &&
          highlightLinks.length === 0 &&
          highlightedPorts.length === 0
        ) {
          this.currentComponent = OperatorPropertyEditFrameComponent;
          this.componentInputs = { currentOperatorId: highlightedOperators[0] };
        } else if (highlightedPorts.length === 1 && highlightedGroups.length === 0 && highlightLinks.length === 0) {
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

  openPropertyPanel() {
    if (!this.propertyDisplay) {
      this.propertyWidth = 300;
    }
    this.propertyDisplay = true;

    if (this.prevHeight != 0) {
      this.propertyHeight = this.prevHeight;
    }

    if (this.prevWidth != 0) {
      this.propertyWidth = this.prevWidth;
    }
  }

  onResize({ width, height }: NzResizeEvent) {
    cancelAnimationFrame(this.id);
    this.id = requestAnimationFrame(() => {
      this.width = width!;
      this.height = height!;
    });
  }

  onClose_property(): void {
    this.propertyDisplay = false;
    this.prevWidth = this.propertyWidth;
    this.propertyWidth = 47;
    this.prevHeight = this.propertyHeight;
    this.propertyHeight = 100;
  }
}

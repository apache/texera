import { AfterViewInit, ChangeDetectorRef, Component, ElementRef, OnDestroy } from "@angular/core";
import * as joint from "jointjs";
// if jQuery needs to be used:
// 1) use `import * as jQuery` as follows, instead of using `$`,
// 2) import any jquery plugins after importing jQuery
// 3) always add the imports even if TypeScript doesn't show an error https://github.com/Microsoft/TypeScript/issues/22016
import * as jQuery from "jquery";
import { fromEvent, merge, Subject } from "rxjs";
import { NzModalCommentBoxComponent } from "./comment-box-modal/nz-modal-comment-box.component";
import { NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import { assertType } from "src/app/common/util/assert";
import { environment } from "../../../../environments/environment";
import { DragDropService } from "../../service/drag-drop/drag-drop.service";
import { DynamicSchemaService } from "../../service/dynamic-schema/dynamic-schema.service";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { fromJointPaperEvent, JointUIService, linkPathStrokeColor } from "../../service/joint-ui/joint-ui.service";
import { ResultPanelToggleService } from "../../service/result-panel-toggle/result-panel-toggle.service";
import { ValidationWorkflowService } from "../../service/validation/validation-workflow.service";
import { JointGraphWrapper } from "../../service/workflow-graph/model/joint-graph-wrapper";
import { OperatorInfo } from "../../service/workflow-graph/model/operator-group";
import { MAIN_CANVAS_LIMIT } from "./workflow-editor-constants";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowStatusService } from "../../service/workflow-status/workflow-status.service";
import { ExecutionState, OperatorState } from "../../types/execute-workflow.interface";
import { OperatorLink, Point } from "../../types/workflow-common.interface";
import { auditTime, filter, map, takeUntil } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { UndoRedoService } from "../../service/undo-redo/undo-redo.service";
import { WorkflowCollabService } from "../../service/workflow-collab/workflow-collab.service";
import { WorkflowVersionService } from "../../../dashboard/service/workflow-version/workflow-version.service";
import { OperatorMenuService } from "../../service/operator-menu/operator-menu.service";
import { NzContextMenuService, NzDropdownMenuComponent } from "ng-zorro-antd/dropdown";

// jointjs interactive options for enabling and disabling interactivity
// https://resources.jointjs.com/docs/jointjs/v3.2/joint.html#dia.Paper.prototype.options.interactive
const defaultInteractiveOption = { vertexAdd: false, labelMove: false };
const disableInteractiveOption = {
  linkMove: false,
  labelMove: false,
  arrowheadMove: false,
  vertexMove: false,
  vertexAdd: false,
  vertexRemove: false,
  elementMove: false, // TODO: This is only a temporary change, will introduce another level of disable option.
};

export const WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID = "texera-workflow-editor-jointjs-wrapper-id";
export const WORKFLOW_EDITOR_JOINTJS_ID = "texera-workflow-editor-jointjs-body-id";

/**
 * WorkflowEditorComponent is the component for the main workflow editor part of the UI.
 *
 * This component is bound with the JointJS paper. JointJS handles the operations of the main workflow.
 * The JointJS UI events are wrapped into observables and exposed to other components / services.
 *
 * See JointJS documentation for the list of events that can be captured on the JointJS paper view.
 * https://resources.jointjs.com/docs/jointjs/v2.0/joint.html#dia.Paper.events
 *
 * @author Zuozhi Wang
 * @author Henry Chen
 *
 */
@UntilDestroy()
@Component({
  selector: "texera-workflow-editor",
  templateUrl: "./workflow-editor.component.html",
  styleUrls: ["./workflow-editor.component.scss"],
})
export class WorkflowEditorComponent implements AfterViewInit, OnDestroy {
  // the DOM element ID of the main editor. It can be used by jQuery and jointJS to find the DOM element
  // in the HTML template, the div element ID is set using this variable
  public readonly WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID = WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID;
  public readonly WORKFLOW_EDITOR_JOINTJS_ID = WORKFLOW_EDITOR_JOINTJS_ID;

  public readonly COPY_OFFSET = 20;

  private paper: joint.dia.Paper | undefined;
  private interactive: boolean = true;
  private gridOn: boolean = true;

  // private ifMouseDown: boolean = false;
  private mouseDown: Point | undefined;

  private _onProcessKeyboardActionObservable: Subject<void> = new Subject();

  constructor(
    private workflowActionService: WorkflowActionService,
    private dynamicSchemaService: DynamicSchemaService,
    private dragDropService: DragDropService,
    private elementRef: ElementRef,
    private resultPanelToggleService: ResultPanelToggleService,
    private validationWorkflowService: ValidationWorkflowService,
    private jointUIService: JointUIService,
    private workflowStatusService: WorkflowStatusService,
    private executeWorkflowService: ExecuteWorkflowService,
    private nzModalService: NzModalService,
    private changeDetectorRef: ChangeDetectorRef,
    private undoRedoService: UndoRedoService,
    private workflowVersionService: WorkflowVersionService,
    private workflowCollabService: WorkflowCollabService,
    private operatorMenu: OperatorMenuService,
    private nzContextMenu: NzContextMenuService
  ) {}

  public getJointPaper(): joint.dia.Paper {
    if (this.paper === undefined) {
      throw new Error("JointJS paper is undefined");
    }

    return this.paper;
  }

  ngOnDestroy(): void {
    this._unregisterKeyboard();
  }

  ngAfterViewInit() {
    this._registerKeyboard();
    this.initializeJointPaper();
    this.handleDisableJointPaperInteractiveness();
    this.handleOperatorValidation();
    this.handlePaperRestoreDefaultOffset();
    this.handlePaperZoom();
    this.handleWindowResize();
    this.handleViewDeleteOperator();
    this.handleCellHighlight();
    this.handleDisableOperator();
    this.registerOperatorDisplayNameChangeHandler();
    this.handleViewDeleteLink();
    this.handleViewCollapseGroup();
    this.handleViewExpandGroup();
    this.handlePaperPan();
    this.handleGroupResize();
    this.handleViewMouseoverOperator();
    this.handleViewMouseoutOperator();

    if (environment.executionStatusEnabled) {
      this.handleOperatorStatisticsUpdate();
    }

    this.handlePaperMouseZoom();
    this.handleOperatorSuggestionHighlightEvent();
    this.dragDropService.registerWorkflowEditorDrop(this.WORKFLOW_EDITOR_JOINTJS_ID);

    // this.rightClickContextMenu();

    this.handleElementDelete();
    this.handleElementSelectAll();
    this.handleElementCopy();
    this.handleElementCut();
    this.handleElementPaste();

    this.handleLinkCursorHover();
    this.handleGridsToggle();
    if (environment.linkBreakpointEnabled) {
      this.handleLinkBreakpoint();
    }
  }

  private _unregisterKeyboard() {
    document.removeEventListener("keydown", this._handleKeyboardAction.bind(this));
  }

  private _registerKeyboard() {
    document.addEventListener("keydown", this._handleKeyboardAction.bind(this));
  }

  private _handleKeyboardAction(event: any) {
    this._onProcessKeyboardActionObservable = new Subject();
    this.workflowVersionService
      .getDisplayParticularVersionStream()
      .pipe(takeUntil(this._onProcessKeyboardActionObservable))
      .subscribe(displayParticularWorkflowVersion => {
        if (!displayParticularWorkflowVersion && this.workflowCollabService.isLockGranted()) {
          // cmd/ctrl+z undo ; ctrl+y or cmd/ctrl + shift+z for redo
          if ((event.metaKey || event.ctrlKey) && !event.shiftKey && event.key.toLowerCase() === "z") {
            // UNDO
            if (this.undoRedoService.canUndo()) {
              this.undoRedoService.undoAction();
            }
          } else if (
            ((event.metaKey || event.ctrlKey) && !event.shiftKey && event.key.toLowerCase() === "y") ||
            ((event.metaKey || event.ctrlKey) && event.shiftKey && event.key.toLowerCase() === "z")
          ) {
            // redo
            if (this.undoRedoService.canRedo()) {
              this.undoRedoService.redoAction();
            }
          }
          // below for future hotkeys
        }
        this._onProcessKeyboardActionObservable.complete();
      });
  }

  private initializeJointPaper(): void {
    // get the custom paper options
    const jointPaperOptions = this.getJointPaperOptions();
    // attach the DOM element to the paper
    jointPaperOptions.el = jQuery(`#${this.WORKFLOW_EDITOR_JOINTJS_ID}`);
    // attach the JointJS graph (model) to the paper (view)
    this.paper = this.workflowActionService.getJointGraphWrapper().attachMainJointPaper(jointPaperOptions);

    this.setJointPaperOriginOffset();
    this.setJointPaperDimensions();
  }

  private handleDisableJointPaperInteractiveness(): void {
    this.workflowActionService
      .getWorkflowModificationEnabledStream()
      .pipe(untilDestroyed(this))
      .subscribe(enabled => {
        if (enabled) {
          this.interactive = true;
          this.getJointPaper().setInteractivity(defaultInteractiveOption);
        } else {
          this.interactive = false;
          this.getJointPaper().setInteractivity(disableInteractiveOption);
        }
        this.changeDetectorRef.detectChanges();
      });
  }

  /**
   * This method subscribe to workflowStatusService's status stream
   * for Each processStatus that has been emitted
   *    1. enable operatorStatusTooltipDisplay because tooltip will not be empty
   *    2. for each operator in current texeraGraph:
   *        - find its Statistics in processStatus, thrown an error if not found
   *        - generate its corresponding tooltip's id
   *        - pass the tooltip id and Statistics to jointUIService
   *          the specific tooltip content will be updated
   *          - if operator is in a group, save statistics in group's operatorInfo
   *    3. Whenever a group is expanded
   *        - for each operatorInfo, display statistics if there are some saved.
   */
  private handleOperatorStatisticsUpdate(): void {
    this.workflowStatusService
      .getStatusUpdateStream()
      .pipe(untilDestroyed(this))
      .subscribe(status => {
        Object.keys(status).forEach(operatorID => {
          if (!this.workflowActionService.getTexeraGraph().hasOperator(operatorID)) {
            return;
          }
          if (this.executeWorkflowService.getExecutionState().state === ExecutionState.Recovering) {
            status[operatorID] = {
              ...status[operatorID],
              operatorState: OperatorState.Recovering,
            };
          }
          // if operator is part of a group, find it
          const parentGroup = this.workflowActionService.getOperatorGroup().getGroupByOperator(operatorID);

          // if operator is not in a group or in a group that isn't collapsed, it is okay to draw statistics on it
          if (!parentGroup || !parentGroup.collapsed) {
            this.jointUIService.changeOperatorStatistics(
              this.getJointPaper(),
              operatorID,
              status[operatorID],
              this.isSource(operatorID),
              this.isSink(operatorID)
            );
          }

          // if operator is in a group, write statistics to the group's operatorInfo
          // so that it can be restored if the group is collapsed and expanded.
          if (parentGroup) {
            const operatorInfo = parentGroup.operators.get(operatorID);
            assertType<OperatorInfo>(operatorInfo);
            operatorInfo.statistics = status[operatorID];
          }
        });
      });

    // listen for group expanding, and redraw operator statistics if they exist
    this.workflowActionService
      .getOperatorGroup()
      .getGroupExpandStream()
      .pipe(untilDestroyed(this))
      .subscribe(group => {
        group.operators.forEach((operatorInfo, operatorID) => {
          if (operatorInfo.statistics) {
            this.jointUIService.changeOperatorStatistics(
              this.getJointPaper(),
              operatorID,
              operatorInfo.statistics,
              this.isSource(operatorID),
              this.isSink(operatorID)
            );
          }
        });
      });

    this.executeWorkflowService
      .getExecutionStateStream()
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        if (event.previous.state === ExecutionState.Recovering) {
          let operatorState: OperatorState;
          if (event.current.state === ExecutionState.Paused) {
            operatorState = OperatorState.Paused;
          } else if (event.current.state === ExecutionState.Completed) {
            operatorState = OperatorState.Completed;
          } else if (event.current.state === ExecutionState.Running) {
            operatorState = OperatorState.Running;
          } else {
            throw new Error("unknown state transition from recovering state: " + event.current.state);
          }
          this.workflowActionService
            .getTexeraGraph()
            .getAllOperators()
            .forEach(op => {
              this.jointUIService.changeOperatorState(this.getJointPaper(), op.operatorID, operatorState);
            });
        }
      });
  }

  /**
   * Handles restore offset default event by translating jointJS paper
   *  back to original position
   */
  private handlePaperRestoreDefaultOffset(): void {
    this.workflowActionService
      .getJointGraphWrapper()
      .getRestorePaperOffsetStream()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.workflowActionService.getJointGraphWrapper().setZoomProperty(1);
        this.getJointPaper().translate(0, 0);
      });
  }

  /**
   * Handles zoom events to make the jointJS paper larger or smaller.
   */
  private handlePaperZoom(): void {
    this.workflowActionService
      .getJointGraphWrapper()
      .getWorkflowEditorZoomStream()
      .pipe(untilDestroyed(this))
      .subscribe(newRatio => {
        // set jointjs scale
        this.getJointPaper().scale(newRatio, newRatio);
      });
  }

  /**
   * Handles zoom events when user slides the mouse wheel.
   *
   * The first filter will removes all the mousewheel events that are undefined
   * The second filter will remove all the mousewheel events that are
   *  from different components
   *
   * From the mousewheel event:
   *  1. when delta Y is negative, the wheel is scrolling down, so
   *      the jointJS paper will zoom in.
   *  2. when delta Y is positive, the wheel is scrolling up, so the
   *      jointJS paper will zoom out.
   */
  private handlePaperMouseZoom(): void {
    fromEvent<WheelEvent>(document, "mousewheel")
      .pipe(
        filter(event => event !== undefined),
        filter(event => this.elementRef.nativeElement.contains(event.target))
      )
      .forEach(event => {
        if (event.metaKey || event.ctrlKey) {
          if (event.deltaY < 0) {
            // if zoom ratio already at minimum, do not zoom out.
            if (this.workflowActionService.getJointGraphWrapper().isZoomRatioMin()) {
              return;
            }
            this.workflowActionService
              .getJointGraphWrapper()
              .setZoomProperty(
                this.workflowActionService.getJointGraphWrapper().getZoomRatio() -
                  JointGraphWrapper.ZOOM_MOUSEWHEEL_DIFF
              );
          } else {
            // if zoom ratio already at maximum, do not zoom in.
            if (this.workflowActionService.getJointGraphWrapper().isZoomRatioMax()) {
              return;
            }
            this.workflowActionService
              .getJointGraphWrapper()
              .setZoomProperty(
                this.workflowActionService.getJointGraphWrapper().getZoomRatio() +
                  JointGraphWrapper.ZOOM_MOUSEWHEEL_DIFF
              );
          }
        }
      });
  }

  /**
   * This method gets all operators' position and
   * gets the limits of translating.
   */
  private getTranslateLimit(): {
    xMin: number;
    xMax: number;
    yMin: number;
    yMax: number;
  } {
    return MAIN_CANVAS_LIMIT;
  }

  /**
   * This method handles user mouse drag events to pan JointJS paper.
   *
   * This method will listen to 4 events to implement the pan feature
   *   1. pointerdown event in the JointJS paper to start panning
   *   2. mousemove event on the document to change the offset of the paper
   *   3. pointerup event in the JointJS paper to stop panning
   *   4. mousewheel event on the document to start panning
   */
  private handlePaperPan(): void {
    // pointer down event to start the panning, this will record the original paper offset
    fromJointPaperEvent(this.getJointPaper(), "blank:pointerdown")
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        const x = event[0].screenX;
        const y = event[0].screenY;
        if (x !== undefined && y !== undefined) {
          this.mouseDown = { x, y };
        }
        event[0].preventDefault();
      });

    // This observable captures the drop event to stop the panning
    merge(fromEvent(document, "mouseup"), fromJointPaperEvent(this.getJointPaper(), "blank:pointerup"))
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.mouseDown = undefined;
      });

    /* mousemove event to move paper, this will calculate the new coordinate based on the
     *  starting coordinate, the mousemove offset, and the current zoom ratio.
     *  To move the paper based on the new coordinate, this will translate the paper by calling
     *  the JointJS method .translate() to move paper's offset.
     */
    const mousePanEvent = fromEvent<MouseEvent>(document, "mousemove").pipe(
      filter(() => this.mouseDown !== undefined),
      map(event => {
        event.preventDefault();
        if (this.mouseDown === undefined) {
          throw new Error("Error: Mouse down is undefined after the filter");
        }
        const newCoordinate = { x: event.screenX, y: event.screenY };
        const panDelta = {
          deltaX: newCoordinate.x - this.mouseDown.x,
          deltaY: newCoordinate.y - this.mouseDown.y,
        };
        this.mouseDown = newCoordinate;
        return panDelta;
      })
    );

    const mouseWheelEvent = fromEvent<WheelEvent>(document, "mousewheel").pipe(
      filter(event => this.elementRef.nativeElement.contains(event.target)),
      filter(event => !(event.metaKey || event.ctrlKey)),
      map(event => {
        return { deltaX: -event.deltaX, deltaY: -event.deltaY };
      })
    );

    merge(
      mousePanEvent,
      mouseWheelEvent,
      this.workflowActionService.getJointGraphWrapper().navigatorMoveDelta.pipe(
        map(event => {
          const scale = this.getJointPaper().scale();
          return {
            deltaX: event.deltaX * scale.sx,
            deltaY: event.deltaY * scale.sy,
          };
        })
      )
    )
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        const oldOrigin = this.getJointPaper().translate();
        const newOrigin = {
          x: oldOrigin.tx + event.deltaX,
          y: oldOrigin.ty + event.deltaY,
        };

        const scale = this.getJointPaper().scale();

        const translateLimit = this.getTranslateLimit();
        const elementSize = this.getWrapperElementSize();

        // Check canvas limit
        if (-newOrigin.x <= translateLimit.xMin) {
          newOrigin.x = -translateLimit.xMin;
        }
        if (-newOrigin.y <= translateLimit.yMin) {
          newOrigin.y = -translateLimit.yMin;
        }
        if (-newOrigin.x >= translateLimit.xMax - elementSize.width / scale.sx) {
          newOrigin.x = -(translateLimit.xMax - elementSize.width / scale.sx);
        }
        if (-newOrigin.y >= translateLimit.yMax - elementSize.height / scale.sy) {
          newOrigin.y = -(translateLimit.yMax - elementSize.height / scale.sy);
        }

        if (newOrigin.x !== oldOrigin.tx || newOrigin.y !== oldOrigin.ty) {
          this.getJointPaper().translate(newOrigin.x, newOrigin.y);
        }
      });
  }

  /**
   * This is the handler for window resize event
   * When the window is resized, trigger an event to set papaer offset and dimension
   *  and limit the event to at most one every 30ms.
   *
   * When user open the result panel and resize, the paper will resize to the size relative
   *  to the result panel, therefore we also need to listen to the event from opening
   *  and closing of the result panel.
   */
  private handleWindowResize(): void {
    // when the window is resized (limit to at most one event every 30ms).
    merge(
      fromEvent(window, "resize").pipe(auditTime(30)),
      this.resultPanelToggleService.getToggleChangeStream().pipe(auditTime(30))
    )
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        // resize the JointJS paper dimensions
        this.setJointPaperDimensions();
      });
  }

  private handleCellHighlight(): void {
    this.handleHighlightMouseDBClickInput();
    this.handleHighlightMouseInput();
    this.handleElementHightlightEvent();
  }

  private handleDisableOperator(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getDisabledOperatorsChangedStream()
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        event.newDisabled.concat(event.newEnabled).forEach(opID => {
          const op = this.workflowActionService.getTexeraGraph().getOperator(opID);
          this.jointUIService.changeOperatorDisableStatus(this.getJointPaper(), op);
        });
      });
  }

  private registerOperatorDisplayNameChangeHandler(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getOperatorDisplayNameChangedStream()
      .pipe(untilDestroyed(this))
      .subscribe(({ operatorID, newDisplayName }) => {
        const op = this.workflowActionService.getTexeraGraph().getOperator(operatorID);
        this.jointUIService.changeOperatorJointDisplayName(op, this.getJointPaper(), newDisplayName);
      });
  }

  public contextMenu($event: MouseEvent, menu: NzDropdownMenuComponent): void {
    this.nzContextMenu.create($event, menu);
  }

  private handleHighlightMouseDBClickInput(): void {
    fromJointPaperEvent(this.getJointPaper(), "cell:pointerdblclick")
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        const clickedCommentBox = event[0].model;
        if (
          clickedCommentBox.isElement() &&
          this.workflowActionService.getTexeraGraph().hasCommentBox(clickedCommentBox.id.toString())
        ) {
          this.workflowActionService.getJointGraphWrapper().setMultiSelectMode(<boolean>event[1].shiftKey);
          const elementID = event[0].model.id.toString();
          if (this.workflowActionService.getTexeraGraph().hasCommentBox(elementID)) {
            this.workflowActionService.getJointGraphWrapper().highlightCommentBoxes(elementID);
          }
        }
      });
  }
  /**
   * Handles user mouse down events to trigger logically highlight and unhighlight an operator or group.
   * If user clicks the operator/group while pressing the shift key, multiselect mode is turned on.
   * When pressing the shift key, user can unhighlight a highlighted operator/group by clicking on it.
   * User can also unhighlight all operators and groups by clicking on the blank area of the graph.
   */
  private handleHighlightMouseInput(): void {
    // on user mouse clicks an operator/group cell, highlight that operator/group
    // operator status tooltips should never be highlighted
    merge(
      fromJointPaperEvent(this.getJointPaper(), "cell:pointerdown"),
      fromJointPaperEvent(this.getJointPaper(), "cell:contextmenu")
    )
      // event[0] is the JointJS CellView; event[1] is the original JQuery Event
      .pipe(
        filter(event => event[0].model.isElement()),
        filter(
          event =>
            this.workflowActionService.getTexeraGraph().hasOperator(event[0].model.id.toString()) ||
            this.workflowActionService.getOperatorGroup().hasGroup(event[0].model.id.toString())
        )
      )
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        // multiselect mode on if holding shift
        this.workflowActionService.getJointGraphWrapper().setMultiSelectMode(<boolean>event[1].shiftKey);

        const elementID = event[0].model.id.toString();
        const highlightedOperatorIDs = this.workflowActionService
          .getJointGraphWrapper()
          .getCurrentHighlightedOperatorIDs();
        if (event[1].shiftKey) {
          // if in multiselect toggle highlights on click
          if (highlightedOperatorIDs.includes(elementID)) {
            this.workflowActionService.unhighlightOperators(elementID);
          } else if (this.workflowActionService.getTexeraGraph().hasOperator(elementID)) {
            this.workflowActionService.highlightOperators(<boolean>event[1].shiftKey, elementID);
          }
          // if in the multiselect mode, also highlight the links in between two highlighted operators
          const allLinks: OperatorLink[] = this.workflowActionService.getTexeraGraph().getAllLinks();
          const linksToBeHighlighted: string[] = allLinks
            .filter(link => {
              const currentHighlightedOperatorIDs = this.workflowActionService
                .getJointGraphWrapper()
                .getCurrentHighlightedOperatorIDs();
              for (let sourceOperatorID of currentHighlightedOperatorIDs) {
                // first make sure the link is not already highlighted
                if (!(link.linkID in this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedLinkIDs)) {
                  if (sourceOperatorID === link.source.operatorID) {
                    // iterate through all the other highlighted operators
                    for (let targetOperatorID of currentHighlightedOperatorIDs.filter(
                      each => each != sourceOperatorID
                    )) {
                      if (targetOperatorID === link.target.operatorID) {
                        return true;
                      }
                    }
                  }
                }
              }
            })
            .map(link => link.linkID);
          this.workflowActionService.highlightLinks(<boolean>event[1].shiftKey, ...linksToBeHighlighted);
        } else {
          // else only highlight a single operator or group
          if (this.workflowActionService.getTexeraGraph().hasOperator(elementID)) {
            this.workflowActionService.highlightOperators(<boolean>event[1].shiftKey, elementID);
          } else if (this.workflowActionService.getOperatorGroup().hasGroup(elementID)) {
            this.workflowActionService.getJointGraphWrapper().highlightGroups(elementID);
          }
        }
      });

    // on user mouse clicks on blank area, unhighlight all operators and groups
    merge(
      fromJointPaperEvent(this.getJointPaper(), "blank:pointerdown"),
      fromJointPaperEvent(this.getJointPaper(), "blank:contextmenu")
    )
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        const highlightedOperatorIDs = this.workflowActionService
          .getJointGraphWrapper()
          .getCurrentHighlightedOperatorIDs();
        const highlightedLinkIDs = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedLinkIDs();
        this.workflowActionService.unhighlightOperators(...highlightedOperatorIDs);
        this.workflowActionService.unhighlightLinks(...highlightedLinkIDs);
      });
  }

  private handleElementHightlightEvent(): void {
    // handle logical operator and group highlight / unhighlight events to let JointJS
    //  use our own custom highlighter
    const highlightOptions = {
      name: "stroke",
      options: {
        attrs: {
          "stroke-width": 2,
          stroke: "#4A95FF",
        },
      },
    };

    // highlight on OperatorHighlightStream or GroupHighlightStream
    merge(
      this.workflowActionService.getJointGraphWrapper().getJointOperatorHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointGroupHighlightStream()
    )
      .pipe(untilDestroyed(this))
      .subscribe(elementIDs =>
        elementIDs.forEach(elementID =>
          this.getJointPaper().findViewByModel(elementID).highlight("rect.body", { highlighter: highlightOptions })
        )
      );

    // unhighlight on OperatorUnhighlightStream or GroupUnhighlightStream
    merge(
      this.workflowActionService.getJointGraphWrapper().getJointOperatorUnhighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointGroupUnhighlightStream()
    )
      .pipe(untilDestroyed(this))
      .subscribe(elementIDs =>
        elementIDs.forEach(elementID => {
          const elem = this.getJointPaper().findViewByModel(elementID);
          if (elem !== undefined) {
            elem.unhighlight("rect.body", { highlighter: highlightOptions });
          }
        })
      );

    this.workflowActionService
      .getJointGraphWrapper()
      .getJointCommentBoxHighlightStream()
      .pipe(untilDestroyed(this))
      .subscribe(commentBoxIDs => {
        this.openCommentBox(commentBoxIDs[0]);
      });
  }

  private openCommentBox(commentBoxID: string): void {
    const commentBox = this.workflowActionService.getTexeraGraph().getCommentBox(commentBoxID);
    const modalRef: NzModalRef = this.nzModalService.create({
      // modal title
      nzTitle: "Comments",
      nzContent: NzModalCommentBoxComponent,
      // set component @Input attributes
      nzComponentParams: {
        // set the index value and page size to the modal for navigation
        commentBox: commentBox,
      },
      // prevent browser focusing close button (ugly square highlight)
      nzAutofocus: null,
      // modal footer buttons
      nzFooter: [
        {
          label: "OK",
          onClick: () => {
            modalRef.destroy();
          },
          type: "primary",
        },
      ],
    });
  }

  private handleOperatorSuggestionHighlightEvent(): void {
    const highlightOptions = {
      name: "stroke",
      options: {
        attrs: {
          "stroke-width": 5,
          stroke: "#551A8B70",
        },
      },
    };

    this.dragDropService
      .getOperatorSuggestionHighlightStream()
      .pipe(untilDestroyed(this))
      .subscribe(value =>
        this.getJointPaper().findViewByModel(value).highlight("rect.body", { highlighter: highlightOptions })
      );

    this.dragDropService
      .getOperatorSuggestionUnhighlightStream()
      .pipe(untilDestroyed(this))
      .subscribe(value =>
        this.getJointPaper().findViewByModel(value).unhighlight("rect.body", { highlighter: highlightOptions })
      );
  }

  /**
   * Modifies the JointJS paper origin coordinates
   *  by shifting it to the left top (minus the x and y offset of the wrapper element)
   * So that elements in JointJS paper have the same coordinates as the actual document.
   *  and we don't have to convert between JointJS coordinates and actual coordinates.
   *
   * panOffset is added to this translation to consider the situation that the paper
   *  has been panned by the user previously.
   *
   * Note: attribute `origin` and function `setOrigin` are deprecated and won't work
   *  function `translate` does the same thing
   */
  private setJointPaperOriginOffset(): void {
    this.getJointPaper().translate(0, 0);
  }

  /**
   * Sets the size of the JointJS paper to be the exact size of its wrapper element.
   */
  private setJointPaperDimensions(): void {
    const elementSize = this.getWrapperElementSize();
    this.getJointPaper().setDimensions(elementSize.width, elementSize.height);
  }

  /**
   * Handles the event where the Delete button is clicked for an Operator,
   *  and call workflowAction to delete the corresponding operator.
   *
   * JointJS doesn't have delete button built-in with an operator element,
   *  the delete button is Texera's own customized element.
   * Therefore JointJS doesn't come with default handler for delete an operator,
   *  we need to handle the callback event `element:delete`.
   * The name of this callback event is registered in `JointUIService.getCustomOperatorStyleAttrs`
   */
  private handleViewDeleteOperator(): void {
    // bind the delete button event to call the delete operator function in joint model action
    fromJointPaperEvent(this.getJointPaper(), "element:delete")
      .pipe(
        filter(value => this.interactive),
        map(value => value[0])
      )
      .pipe(untilDestroyed(this))
      .subscribe(elementView => {
        if (this.workflowActionService.getTexeraGraph().hasOperator(elementView.model.id.toString())) {
          this.workflowActionService.deleteOperator(elementView.model.id.toString());
        }
        if (this.workflowActionService.getTexeraGraph().hasCommentBox(elementView.model.id.toString())) {
          this.workflowActionService.deleteCommentBox(elementView.model.id.toString());
        }
      });
  }

  private handleViewMouseoverOperator(): void {
    fromJointPaperEvent(this.getJointPaper(), "element:mouseenter")
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        this.jointUIService.unfoldOperatorDetails(this.getJointPaper(), event[0].model.id.toString());
      });
  }

  private handleViewMouseoutOperator(): void {
    fromJointPaperEvent(this.getJointPaper(), "element:mouseleave")
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        this.jointUIService.foldOperatorDetails(this.getJointPaper(), event[0].model.id.toString());
      });
  }

  /**
   * Handles the event where the Delete button is clicked for a Link,
   *  and call workflowAction to delete the corresponding link.
   *
   * We handle link deletion on our own by defining a custom markup.
   * Therefore JointJS doesn't come with default handler for delete an operator,
   *  we need to handle the callback event `tool:remove`.
   */
  private handleViewDeleteLink(): void {
    fromJointPaperEvent(this.getJointPaper(), "tool:remove")
      .pipe(
        filter(value => this.interactive),
        map(value => value[0])
      )
      .pipe(untilDestroyed(this))
      .subscribe(elementView => {
        this.workflowActionService.deleteLinkWithID(elementView.model.id.toString());
      });
  }

  /**
   * Handles the event where the Collapse button is clicked for a Group,
   *  and call groupOperator to collapse the corresponding group.
   *
   * JointJS doesn't have collapse button built-in with a group element,
   *  the collapse button is Texera's own customized element.
   * Therefore JointJS doesn't come with default handler for collapse a group,
   *  we need to handle the callback event `element:collapse`.
   * The name of this callback event is registered in `JointUIService.getCustomGroupStyleAttrs`
   */
  private handleViewCollapseGroup(): void {
    fromJointPaperEvent(this.getJointPaper(), "element:collapse")
      .pipe(map(value => value[0]))
      .pipe(untilDestroyed(this))
      .subscribe(elementView => {
        const groupID = elementView.model.id.toString();
        this.workflowActionService.collapseGroups(groupID);
      });
  }

  /**
   * Handles the event where the Expand button is clicked for a Group,
   *  and call groupOperator to expand the corresponding group.
   *
   * JointJS doesn't have expand button built-in with a group element,
   *  the expand button is Texera's own customized element.
   * Therefore JointJS doesn't come with default handler for expand a group,
   *  we need to handle the callback event `element:expand`.
   * The name of this callback event is registered in `JointUIService.getCustomGroupStyleAttrs`
   */
  private handleViewExpandGroup(): void {
    fromJointPaperEvent(this.getJointPaper(), "element:expand")
      .pipe(map(value => value[0]))
      .pipe(untilDestroyed(this))
      .subscribe(elementView => {
        const groupID = elementView.model.id.toString();
        this.workflowActionService.expandGroups(groupID);
      });
  }

  /**
   * if the operator is valid , the border of the box will be default
   */
  private handleOperatorValidation(): void {
    this.validationWorkflowService
      .getOperatorValidationStream()
      .pipe(untilDestroyed(this))
      .subscribe(value => {
        if (!this.workflowActionService.getOperatorGroup().getGroupByOperator(value.operatorID)?.collapsed) {
          this.jointUIService.changeOperatorColor(this.getJointPaper(), value.operatorID, value.validation.isValid);
        }
      });
  }

  /**
   * Handles events that cause a group's size to change (collapse, expand, or
   * resize), and hides or repositions the group's collapse/expand button.
   *
   * Since the collapse button's position is relative to a group's width,
   * resizing the group will cause the button to be out of place.
   */
  private handleGroupResize(): void {
    this.workflowActionService
      .getOperatorGroup()
      .getGroupCollapseStream()
      .pipe(untilDestroyed(this))
      .subscribe(group => {
        this.jointUIService.hideGroupCollapseButton(this.getJointPaper(), group.groupID);
      });

    this.workflowActionService
      .getOperatorGroup()
      .getGroupExpandStream()
      .pipe(untilDestroyed(this))
      .subscribe(group => {
        this.jointUIService.hideGroupExpandButton(this.getJointPaper(), group.groupID);
      });

    this.workflowActionService
      .getOperatorGroup()
      .getGroupResizeStream()
      .pipe(untilDestroyed(this))
      .subscribe(value => {
        this.jointUIService.repositionGroupCollapseButton(this.getJointPaper(), value.groupID, value.width);
      });
  }

  /**
   * Gets the width and height of the parent wrapper element
   */
  private getWrapperElementSize(): { width: number; height: number } {
    const width = jQuery("#" + this.WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID).width();
    const height = jQuery("#" + this.WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID).height();

    if (width === undefined || height === undefined) {
      throw new Error("fail to get Workflow Editor wrapper element size");
    }

    return { width, height };
  }

  /**
   * Gets our customize options for the JointJS Paper object, which is the JointJS view object responsible for
   *  rendering the workflow cells and handle UI events.
   * JointJS documentation about paper: https://resources.jointjs.com/docs/jointjs/v2.0/joint.html#dia.Paper
   */
  private getJointPaperOptions(): joint.dia.Paper.Options {
    const jointPaperOptions: joint.dia.Paper.Options = {
      // enable jointjs feature that automatically snaps a link to the closest port with a radius of 30px
      snapLinks: { radius: 40 },
      // disable jointjs default action that can make a link not connect to an operator
      linkPinning: false,
      // provide a validation to determine if two ports could be connected (only output connect to input is allowed)
      validateConnection: (...args) => this.validateJointOperatorConnection(...args),
      // provide a validation to determine if the port where link starts from is an out port
      validateMagnet: (...args) => WorkflowEditorComponent.validateOperatorMagnet(...args),
      // marks all the available magnets or elements when a link is dragged
      markAvailable: true,
      // disable jointjs default action of adding vertexes to the link
      interactive: defaultInteractiveOption,
      // set a default link element used by jointjs when user creates a link on UI
      defaultLink: JointUIService.getDefaultLinkCell(),
      // disable jointjs default action that stops propagate click events on jointjs paper
      preventDefaultBlankAction: false,
      // prevents normal right click menu showing up on jointjs paper
      preventContextMenu: true,
      // draw dots in the background of the paper
      drawGrid: {
        name: "fixedDot",
        args: { color: "black", scaleFactor: 8, thickness: 1.2 },
      },
      // set grid size
      gridSize: 2,
      // use approximate z-index sorting, this is a workaround of a bug in async rendering mode
      // see https://github.com/clientIO/joint/issues/1320
      sorting: joint.dia.Paper.sorting.APPROX,
    };

    return jointPaperOptions;
  }

  /**
   * This function is provided to JointJS to disable some invalid connections on the UI.
   * If the connection is invalid, users are not able to connect the links on the UI.
   *
   * https://resources.jointjs.com/docs/jointjs/v2.0/joint.html#dia.Paper.prototype.options.validateConnection
   */
  private validateJointOperatorConnection(
    sourceView: joint.dia.CellView,
    sourceMagnet: SVGElement | undefined,
    targetView: joint.dia.CellView,
    targetMagnet: SVGElement | undefined,
    end: joint.dia.LinkEnd,
    linkView: joint.dia.LinkView
  ): boolean {
    // user cannot draw connection starting from the input port (left side)
    if (sourceMagnet && sourceMagnet.getAttribute("port-group") === "in") {
      return false;
    }

    // user cannot connect to the output port (right side)
    if (targetMagnet && targetMagnet.getAttribute("port-group") === "out") {
      return false;
    }

    const sourceCellID = sourceView.model.id.toString();
    const sourcePortID = sourceMagnet?.getAttribute("port");
    const targetCellID = targetView.model.id.toString();
    const targetPortID = targetMagnet?.getAttribute("port");

    return this.validateOperatorConnection(sourceCellID, sourcePortID, targetCellID, targetPortID);
  }

  private validateOperatorConnection(
    sourceCellID: string,
    sourcePortID: string | null | undefined,
    targetCellID: string,
    targetPortID: string | null | undefined
  ): boolean {
    // cannot connect to itself
    if (sourceCellID === targetCellID) {
      return false;
    }

    // must connect to ports
    if (!sourcePortID || !targetPortID) {
      return false;
    }

    // must connect to operators
    if (
      !this.workflowActionService.getTexeraGraph().hasOperator(sourceCellID) ||
      !this.workflowActionService.getTexeraGraph().hasOperator(targetCellID)
    ) {
      return false;
    }

    // find all the links that are connected to the target operator and port
    const connectedLinksToTargetPort = this.workflowActionService
      .getTexeraGraph()
      .getAllLinks()
      .filter(link => link.target.operatorID === targetCellID && link.target.portID === targetPortID);

    // check if this link already exists, duplicate links are not allowed
    const isDuplicateLink =
      connectedLinksToTargetPort.filter(
        link => link.source.operatorID === sourceCellID && link.source.portID === sourcePortID
      ).length > 0;
    if (isDuplicateLink) {
      return false;
    }

    let allowMultiInput = false;
    if (this.workflowActionService.getTexeraGraph().hasOperator(targetCellID)) {
      const portIndex = this.workflowActionService
        .getTexeraGraph()
        .getOperator(targetCellID)
        .inputPorts.findIndex(p => p.portID === targetPortID);
      if (portIndex >= 0) {
        const portInfo =
          this.dynamicSchemaService.getDynamicSchema(targetCellID).additionalMetadata.inputPorts[portIndex];
        allowMultiInput = portInfo.allowMultiInputs ?? false;
      }
    }

    if (connectedLinksToTargetPort.length > 0 && !allowMultiInput) {
      return false;
    }

    return true;
  }

  /**
   * Deletes currently highlighted operators and groups when user presses the delete key.
   * When the focus is not on root document body, operator should not be deleted
   */
  private handleElementDelete(): void {
    fromEvent<KeyboardEvent>(document, "keydown")
      .pipe(
        filter(event => document.activeElement === document.body),
        filter(event => this.interactive),
        filter(event => event.key === "Backspace" || event.key === "Delete")
      )
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        const highlightedOperatorIDs = this.workflowActionService
          .getJointGraphWrapper()
          .getCurrentHighlightedOperatorIDs();
        const highlightedGroupIDs = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs();
        this.workflowActionService.deleteOperatorsAndLinks(highlightedOperatorIDs, [], highlightedGroupIDs);
      });
  }

  /**
   * Highlight all operators and groups on the graph when user presses command/ctrl + A.
   */
  private handleElementSelectAll(): void {
    fromEvent<KeyboardEvent>(document, "keydown")
      .pipe(
        filter(event => document.activeElement === document.body),
        filter(event => (event.metaKey || event.ctrlKey) && event.key === "a")
      )
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        event.preventDefault();
        const allOperators = this.workflowActionService
          .getTexeraGraph()
          .getAllOperators()
          .map(operator => operator.operatorID)
          .filter(
            operatorID => !this.workflowActionService.getOperatorGroup().getGroupByOperator(operatorID)?.collapsed
          );
        const allLinks = this.workflowActionService
          .getTexeraGraph()
          .getAllLinks()
          .map(link => link.linkID);
        const allGroups = this.workflowActionService
          .getOperatorGroup()
          .getAllGroups()
          .map(group => group.groupID);
        this.workflowActionService
          .getJointGraphWrapper()
          .setMultiSelectMode(allOperators.length + allGroups.length > 1);
        this.workflowActionService.highlightOperators(allOperators.length + allGroups.length > 1, ...allOperators);
        this.workflowActionService.highlightLinks(allLinks.length > 1, ...allLinks);
        this.workflowActionService.getJointGraphWrapper().highlightGroups(...allGroups);
      });
  }

  /**
   * Caches the currently highlighted operators' info when user
   * triggers the copy event (i.e. presses command/ctrl + c on
   * keyboard or selects copy option from the browser menu).
   */
  private handleElementCopy(): void {
    fromEvent<ClipboardEvent>(document, "copy")
      .pipe(filter(event => document.activeElement === document.body))
      .subscribe(() => {
        if (this.operatorMenu.effectivelyHighlightedOperators.value.length > 0) {
          this.operatorMenu.saveHighlightedElements();
        }
      });
  }

  /**
   * Caches the currently highlighted operators' info and deletes it
   * when user triggers the cut event (i.e. presses command/ctrl + x
   * on keyboard or selects cut option from the browser menu).
   */
  private handleElementCut(): void {
    fromEvent<ClipboardEvent>(document, "cut")
      .pipe(
        filter(event => document.activeElement === document.body),
        filter(event => this.interactive)
      )
      .subscribe(() => {
        if (this.operatorMenu.effectivelyHighlightedOperators.value.length > 0) {
          const highlightedOperatorIDs = this.workflowActionService
            .getJointGraphWrapper()
            .getCurrentHighlightedOperatorIDs();
          const highlightedGroupIDs = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs();

          this.operatorMenu.saveHighlightedElements();
          this.workflowActionService.deleteOperatorsAndLinks(highlightedOperatorIDs, [], highlightedGroupIDs);
        }
      });
  }

  /**
   * Pastes the cached operators onto the workflow graph and highlights them
   * when user triggers the paste event (i.e. presses command/ctrl + v on
   * keyboard or selects paste option from the browser menu).
   */
  private handleElementPaste(): void {
    fromEvent<ClipboardEvent>(document, "paste")
      .pipe(
        filter(event => document.activeElement === document.body),
        filter(event => this.interactive)
      )
      .subscribe(() => this.operatorMenu.performPasteOperation());
  }

  /**
   * handle the events of the cursor enter/leave a jointJS link cell
   *
   * Originally, such "hover -> appear" feature came as a default setting with JointJS library
   * However, in order to achieve conditional disappearance for the breakpoint button,
   * every interaction between the cursor and the link tools, including the delete button,
   * need to be handled manually
   */
  private handleLinkCursorHover(): void {
    // When the cursor hovers over a link, the delete button and the breakpoint button appear
    fromJointPaperEvent(this.getJointPaper(), "link:mouseenter")
      .pipe(map(value => value[0]))
      .pipe(untilDestroyed(this))
      .subscribe(elementView => {
        if (environment.linkBreakpointEnabled) {
          this.getJointPaper()
            .getModelById(elementView.model.id)
            .attr({
              ".tool-remove": { display: "block" },
            });
          this.getJointPaper().getModelById(elementView.model.id).findView(this.getJointPaper()).showTools();
        } else {
          // only display the delete button
          this.getJointPaper()
            .getModelById(elementView.model.id)
            .attr({
              ".tool-remove": { display: "block" },
            });
        }
      });

    /**
     * When the cursor leaves a link, the delete button disappears.
     * If there is no breakpoint present on that link, the breakpoint button also disappears,
     * otherwise, the breakpoint button is not changed.
     */
    fromJointPaperEvent(this.getJointPaper(), "link:mouseleave")
      .pipe(map(value => value[0]))
      .pipe(untilDestroyed(this))
      .subscribe(elementView => {
        // ensure that the link element exists
        if (this.getJointPaper().getModelById(elementView.model.id)) {
          const LinksWithBreakpoint = this.workflowActionService.getJointGraphWrapper().getLinkIDsWithBreakpoint();
          if (!LinksWithBreakpoint.includes(elementView.model.id.toString())) {
            this.getJointPaper().getModelById(elementView.model.id).findView(this.getJointPaper()).hideTools();
          }
          this.getJointPaper()
            .getModelById(elementView.model.id)
            .attr({
              ".tool-remove": { display: "none" },
            });
        }
      });
  }

  /**
   * handles events/observables related to the breakpoint
   */
  private handleLinkBreakpoint(): void {
    this.handleLinkBreakpointToolAttachment();
    this.handleLinkBreakpointButtonClick();
    this.handleLinkBreakpointHighlightEvents();
    this.handleLinkBreakpointToggleEvents();
  }

  // when a link is added, append a breakpoint link-tool to its LinkView
  private handleLinkBreakpointToolAttachment(): void {
    this.workflowActionService
      .getJointGraphWrapper()
      .getJointLinkCellAddStream()
      .pipe(this.workflowActionService.getJointGraphWrapper().jointGraphContext.bufferWhileAsync, untilDestroyed(this))
      .subscribe(link => {
        const linkView = link.findView(this.getJointPaper());
        const breakpointButtonTool = this.jointUIService.getBreakpointButton();
        const breakpointButton = new breakpointButtonTool();
        const toolsView = new joint.dia.ToolsView({
          name: "basic-tools",
          tools: [breakpointButton],
        });
        linkView.addTools(toolsView);
        // tools remain hidden until the cursor hovers over it or a break point is added
        linkView.hideTools();
      });
  }

  /**
   * handles the events of the breakpoint button is clicked for a link
   * and converts that event to a workflow action
   */
  private handleLinkBreakpointButtonClick(): void {
    fromJointPaperEvent(this.getJointPaper(), "tool:breakpoint")
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        // set the multi-select mode
        this.workflowActionService.getJointGraphWrapper().setMultiSelectMode(<boolean>event[1].shiftKey);

        const clickedLinkID = event[0].model.id.toString();
        const currentlyHighlightedLinkIDs = this.workflowActionService
          .getJointGraphWrapper()
          .getCurrentHighlightedLinkIDs();

        if (event[1].shiftKey) {
          if (currentlyHighlightedLinkIDs.includes(clickedLinkID)) {
            // if the link being clicked is already highlighted, unhighlight it
            this.workflowActionService.unhighlightLinks(clickedLinkID);
          } else if (this.workflowActionService.getTexeraGraph().hasLinkWithID(clickedLinkID)) {
            // highlight the link if the link has not already been highlighted
            this.workflowActionService.highlightLinks(<boolean>event[1].shiftKey, clickedLinkID);
          }
        } else {
          // if user doesn't click on the shift key, highlight only a single link
          if (this.workflowActionService.getTexeraGraph().hasLinkWithID(clickedLinkID)) {
            this.workflowActionService.highlightLinks(<boolean>event[1].shiftKey, clickedLinkID);
          }
        }
      });
  }

  /**
   * Highlight/unhighlight the link according to the observable value received.
   */
  private handleLinkBreakpointHighlightEvents(): void {
    this.workflowActionService
      .getJointGraphWrapper()
      .getLinkHighlightStream()
      .pipe(untilDestroyed(this))
      .subscribe(linkIDs => {
        linkIDs.forEach(linkID => {
          this.getJointPaper()
            .getModelById(linkID)
            .attr({
              ".connection": { stroke: "orange" },
              ".marker-source": { fill: "orange" },
              ".marker-target": { fill: "orange" },
            });
        });
      });

    this.workflowActionService
      .getJointGraphWrapper()
      .getLinkUnhighlightStream()
      .pipe(untilDestroyed(this))
      .subscribe(linkIDs => {
        linkIDs.forEach(linkID => {
          const linkView = this.getJointPaper().findViewByModel(linkID);
          // ensure that the link still exist
          if (this.getJointPaper().getModelById(linkID)) {
            this.getJointPaper()
              .getModelById(linkID)
              .attr({
                ".connection": { stroke: linkPathStrokeColor },
                ".marker-source": { fill: "none" },
                ".marker-target": { fill: "none" },
              });
          }
        });
      });
  }

  /**
   * show/hide the breakpoint button according to the observable value received
   */
  private handleLinkBreakpointToggleEvents(): void {
    this.workflowActionService
      .getJointGraphWrapper()
      .getLinkBreakpointShowStream()
      .pipe(this.workflowActionService.getJointGraphWrapper().jointGraphContext.bufferWhileAsync, untilDestroyed(this))
      .subscribe(linkID => {
        this.getJointPaper().getModelById(linkID.linkID).findView(this.getJointPaper()).showTools();
      });

    this.workflowActionService
      .getJointGraphWrapper()
      .getLinkBreakpointHideStream()
      .pipe(this.workflowActionService.getJointGraphWrapper().jointGraphContext.bufferWhileAsync, untilDestroyed(this))
      .subscribe(linkID => {
        this.getJointPaper().getModelById(linkID.linkID).findView(this.getJointPaper()).hideTools();
      });
  }

  private isSource(operatorID: string): boolean {
    return this.workflowActionService.getTexeraGraph().getOperator(operatorID).inputPorts.length == 0;
  }

  private isSink(operatorID: string): boolean {
    return this.workflowActionService.getTexeraGraph().getOperator(operatorID).outputPorts.length == 0;
  }

  /**
   * This function is provided to JointJS to disallow links starting from an in port.
   *
   * https://resources.jointjs.com/docs/jointjs/v2.0/joint.html#dia.Paper.prototype.options.validateMagnet
   */
  private static validateOperatorMagnet(
    cellView: joint.dia.CellView,
    magnet: SVGElement,
    event: joint.dia.Event
  ): boolean {
    return magnet && magnet.getAttribute("port-group") === "out";
  }

  /**
   * This function handles the event stream from jointGraph to toggle the grids in jointPaper on or off.
   * @private
   */
  private handleGridsToggle(): void {
    this.workflowActionService
      .getJointGraphWrapper()
      .getJointPaperGridsToggleStream()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        if (this.gridOn) {
          this.getJointPaper().setGridSize(0);
          this.gridOn = false;
        } else {
          this.getJointPaper().setGridSize(2);
          this.gridOn = true;
        }
      });
  }
}

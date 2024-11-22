import { AfterViewInit, ChangeDetectorRef, Component, OnDestroy } from "@angular/core";
import { fromEvent, merge, Subject } from "rxjs";
import { NzModalCommentBoxComponent } from "./comment-box-modal/nz-modal-comment-box.component";
import { NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import { environment } from "../../../../environments/environment";
import { DragDropService } from "../../service/drag-drop/drag-drop.service";
import { DynamicSchemaService } from "../../service/dynamic-schema/dynamic-schema.service";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { fromJointPaperEvent, JointUIService, linkPathStrokeColor } from "../../service/joint-ui/joint-ui.service";
import { ValidationWorkflowService } from "../../service/validation/validation-workflow.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowStatusService } from "../../service/workflow-status/workflow-status.service";
import { ExecutionState, OperatorState } from "../../types/execute-workflow.interface";
import { LogicalPort, OperatorLink } from "../../types/workflow-common.interface";
import { auditTime, filter, map, takeUntil } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { UndoRedoService } from "../../service/undo-redo/undo-redo.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { OperatorMenuService } from "../../service/operator-menu/operator-menu.service";
import { NzContextMenuService } from "ng-zorro-antd/dropdown";
import { ActivatedRoute, Router } from "@angular/router";
import * as _ from "lodash";
import * as joint from "jointjs";

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
  addLinkFromMagnet: false,
};

export const MAIN_CANVAS = {
  xMin: -960,
  xMax: 2688, // xMin * 2.8
  yMin: -540,
  yMax: 1512, // yMin * 2.8
};

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
  templateUrl: "workflow-editor.component.html",
  styleUrls: ["workflow-editor.component.scss"],
})
export class WorkflowEditorComponent implements AfterViewInit, OnDestroy {
  editor!: HTMLElement;
  editorWrapper!: HTMLElement;
  paper!: joint.dia.Paper;
  private interactive: boolean = true;
  private gridOn: boolean = false;
  private _onProcessKeyboardActionObservable: Subject<void> = new Subject();
  private wrapper;

  constructor(
    private workflowActionService: WorkflowActionService,
    private dynamicSchemaService: DynamicSchemaService,
    private dragDropService: DragDropService,
    private validationWorkflowService: ValidationWorkflowService,
    private jointUIService: JointUIService,
    private workflowStatusService: WorkflowStatusService,
    private executeWorkflowService: ExecuteWorkflowService,
    private nzModalService: NzModalService,
    private changeDetectorRef: ChangeDetectorRef,
    private undoRedoService: UndoRedoService,
    private workflowVersionService: WorkflowVersionService,
    private operatorMenu: OperatorMenuService,
    private route: ActivatedRoute,
    private router: Router,
    public nzContextMenu: NzContextMenuService
  ) {
    this.wrapper = this.workflowActionService.getJointGraphWrapper();
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

  ngAfterViewInit() {
    this.editor = document.getElementById("workflow-editor")!;
    this.editorWrapper = document.getElementById("workflow-editor-wrapper")!;
    document.addEventListener("keydown", this._handleKeyboardAction.bind(this));
    this.initializeJointPaper();
    this.handleDisableJointPaperInteractiveness();
    this.handleOperatorValidation();
    this.handlePaperRestoreDefaultOffset();
    this.handlePaperZoom();
    this.handleWindowResize();
    this.handleViewDeleteOperator();
    this.handleCellHighlight();
    this.handleDisableOperator();
    this.handleViewOperatorResult();
    this.handleReuseCacheOperator();
    this.registerOperatorDisplayNameChangeHandler();
    this.handleViewDeleteLink();
    this.handleViewAddPort();
    this.handleViewRemovePort();
    this.handlePortClick();
    this.handlePaperPan();
    this.handleViewMouseoverOperator();
    this.handleViewMouseoutOperator();
    this.handlePortHighlightEvent();
    this.registerPortDisplayNameChangeHandler();
    this.handleOperatorStatisticsUpdate();
    this.handleOperatorSuggestionHighlightEvent();
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
    this.handlePointerEvents();
    this.handleURLFragment();
    this.invokeResize();
  }

  ngOnDestroy(): void {
    document.removeEventListener("keydown", this._handleKeyboardAction.bind(this));
  }

  private _handleKeyboardAction(event: any) {
    this._onProcessKeyboardActionObservable = new Subject();
    this.workflowVersionService
      .getDisplayParticularVersionStream()
      .pipe(takeUntil(this._onProcessKeyboardActionObservable))
      .subscribe(displayParticularWorkflowVersion => {
        if (!displayParticularWorkflowVersion) {
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
    // attach the JointJS graph (model) to the paper (view)
    this.paper = this.wrapper.attachMainJointPaper({
      el: this.editor,
      background: { color: "#F6F6F6" },
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
      gridSize: 1,
      // use approximate z-index sorting, this is a workaround of a bug in async rendering mode
      // see https://github.com/clientIO/joint/issues/1320
      sorting: joint.dia.Paper.sorting.APPROX,
      width: this.editor.offsetWidth,
      height: this.editor.offsetHeight,
    });
  }

  private handleDisableJointPaperInteractiveness(): void {
    this.workflowActionService
      .getWorkflowModificationEnabledStream()
      .pipe(untilDestroyed(this))
      .subscribe(enabled => {
        if (enabled) {
          this.interactive = true;
          this.paper.setInteractivity(defaultInteractiveOption);
        } else {
          this.interactive = false;
          this.paper.setInteractivity(disableInteractiveOption);
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

          this.jointUIService.changeOperatorStatistics(
            this.paper,
            operatorID,
            status[operatorID],
            this.isSource(operatorID),
            this.isSink(operatorID)
          );
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
              this.jointUIService.changeOperatorState(this.paper, op.operatorID, operatorState);
            });
        }
      });
  }

  /**
   * Handles restore offset default event by translating jointJS paper
   *  back to original position
   */
  private handlePaperRestoreDefaultOffset(): void {
    this.wrapper
      .getRestorePaperOffsetStream()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.wrapper.setZoomProperty(1);
        this.paper.translate(0, 0);
      });
  }

  /**
   * Handles zoom events to make the jointJS paper larger or smaller.
   */
  private handlePaperZoom(): void {
    this.wrapper
      .getWorkflowEditorZoomStream()
      .pipe(untilDestroyed(this))
      .subscribe(newRatio => this.paper.scale(newRatio, newRatio));
  }

  private handlePaperPan(): void {
    fromJointPaperEvent(this.paper, "blank:pointerdown")
      .pipe(untilDestroyed(this))
      .subscribe(() =>
        fromEvent<MouseEvent>(document, "mousemove")
          .pipe(takeUntil(fromEvent(document, "mouseup")))
          .subscribe(event =>
            this.paper.translate(
              this.paper.translate().tx + event.movementX / this.paper.scale().sx,
              this.paper.translate().ty + event.movementY / this.paper.scale().sy
            )
          )
      );
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
    merge(fromEvent(window, "resize").pipe(auditTime(30)))
      .pipe(untilDestroyed(this))
      .subscribe(() => this.paper.setDimensions(this.editorWrapper.offsetWidth, this.editorWrapper.offsetHeight));
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
          this.jointUIService.changeOperatorDisableStatus(this.paper, op);
        });
      });
  }

  private handleViewOperatorResult(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getViewResultOperatorsChangedStream()
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        event.newViewResultOps.concat(event.newUnviewResultOps).forEach(opID => {
          const op = this.workflowActionService.getTexeraGraph().getOperator(opID);
          this.jointUIService.changeOperatorViewResultStatus(this.paper, op, op.viewResult);
        });
      });
  }

  private handleReuseCacheOperator(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getReuseCacheOperatorsChangedStream()
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        event.newReuseCacheOps.concat(event.newUnreuseCacheOps).forEach(opID => {
          const op = this.workflowActionService.getTexeraGraph().getOperator(opID);
          this.jointUIService.changeOperatorReuseCacheStatus(this.paper, op);
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
        this.jointUIService.changeOperatorJointDisplayName(op, this.paper, newDisplayName);
      });
  }

  private registerPortDisplayNameChangeHandler(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getPortDisplayNameChangedSubject()
      .pipe(untilDestroyed(this))
      .subscribe(({ operatorID, portID, newDisplayName }) => {
        const operatorJointElement = <joint.dia.Element>this.workflowActionService.getJointGraph().getCell(operatorID);
        operatorJointElement.portProp(portID, "attrs/.port-label", {
          text: newDisplayName,
        });
      });
  }

  private handleHighlightMouseDBClickInput(): void {
    // on user mouse double-clicks a comment box, open that comment box
    fromJointPaperEvent(this.paper, "cell:pointerdblclick")
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        const clickedCommentBox = event[0].model;
        if (
          clickedCommentBox.isElement() &&
          this.workflowActionService.getTexeraGraph().hasCommentBox(clickedCommentBox.id.toString())
        ) {
          this.wrapper.setMultiSelectMode(<boolean>event[1].shiftKey);
          const elementID = event[0].model.id.toString();
          if (this.workflowActionService.getTexeraGraph().hasCommentBox(elementID)) {
            this.openCommentBox(elementID);
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
    merge(fromJointPaperEvent(this.paper, "cell:pointerdown"), fromJointPaperEvent(this.paper, "cell:contextmenu"))
      // event[0] is the JointJS CellView; event[1] is the original JQuery Event
      .pipe(
        filter(event => event[0].model.isElement()),
        filter(
          event =>
            this.workflowActionService.getTexeraGraph().hasOperator(event[0].model.id.toString()) ||
            this.workflowActionService.getTexeraGraph().hasCommentBox(event[0].model.id.toString())
        )
      )
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        // multiselect mode on if holding shift
        this.wrapper.setMultiSelectMode(<boolean>event[1].shiftKey);

        const elementID = event[0].model.id.toString();
        const highlightedOperatorIDs = this.wrapper.getCurrentHighlightedOperatorIDs();
        const highlightedCommentBoxIDs = this.wrapper.getCurrentHighlightedCommentBoxIDs();
        if (event[1].shiftKey) {
          // if in multiselect toggle highlights on click
          if (highlightedOperatorIDs.includes(elementID)) {
            this.workflowActionService.unhighlightOperators(elementID);
          } else if (this.workflowActionService.getTexeraGraph().hasOperator(elementID)) {
            this.workflowActionService.highlightOperators(<boolean>event[1].shiftKey, elementID);
          }
          if (highlightedCommentBoxIDs.includes(elementID)) {
            this.wrapper.unhighlightCommentBoxes(elementID);
          } else if (this.workflowActionService.getTexeraGraph().hasCommentBox(elementID)) {
            this.workflowActionService.highlightCommentBoxes(<boolean>event[1].shiftKey, elementID);
          }
          // if in the multiselect mode, also highlight the links in between two highlighted operators
          const allLinks: OperatorLink[] = this.workflowActionService.getTexeraGraph().getAllLinks();
          const linksToBeHighlighted: string[] = allLinks
            .filter(link => {
              const currentHighlightedOperatorIDs = this.wrapper.getCurrentHighlightedOperatorIDs();
              for (let sourceOperatorID of currentHighlightedOperatorIDs) {
                // first make sure the link is not already highlighted
                if (!(link.linkID in this.wrapper.getCurrentHighlightedLinkIDs)) {
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
          } else if (this.workflowActionService.getTexeraGraph().hasCommentBox(elementID)) {
            this.wrapper.highlightCommentBoxes(elementID);
          }
        }
      });

    // on user mouse clicks on blank area, unhighlight all operators and groups
    merge(fromJointPaperEvent(this.paper, "blank:pointerdown"), fromJointPaperEvent(this.paper, "blank:contextmenu"))
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.wrapper.unhighlightElements(this.wrapper.getCurrentHighlights());
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

    // highlight on OperatorHighlightStream or GroupHighlightStream or CommentBoxHighlightStream
    merge(
      this.wrapper.getJointOperatorHighlightStream(),
      this.wrapper.getJointGroupHighlightStream(),
      this.wrapper.getJointCommentBoxHighlightStream()
    )
      .pipe(untilDestroyed(this))
      .subscribe(elementIDs =>
        elementIDs.forEach(elementID => {
          this.paper.findViewByModel(elementID).highlight("rect.body", { highlighter: highlightOptions });
        })
      );

    // unhighlight on OperatorUnhighlightStream or GroupUnhighlightStream or CommentBoxUnhighlightStream
    merge(
      this.wrapper.getJointOperatorUnhighlightStream(),
      this.wrapper.getJointGroupUnhighlightStream(),
      this.wrapper.getJointCommentBoxUnhighlightStream()
    )
      .pipe(untilDestroyed(this))
      .subscribe(elementIDs =>
        elementIDs.forEach(elementID => {
          const elem = this.paper.findViewByModel(elementID);
          if (elem !== undefined) {
            elem.unhighlight("rect.body", { highlighter: highlightOptions });
          }
        })
      );
  }

  private handlePortHighlightEvent(): void {
    this.wrapper
      .getJointPortHighlightStream()
      .pipe(untilDestroyed(this))
      .subscribe(operatorPortIDs => {
        operatorPortIDs.forEach(operatorPortID => {
          const operatorJointElement = <joint.dia.Element>(
            this.workflowActionService.getJointGraph().getCell(operatorPortID.operatorID)
          );
          operatorJointElement.portProp(operatorPortID.portID, "attrs/.port-body", {
            r: 8,
            stroke: "#4A95FF",
            "stroke-width": 3,
          });
        });
      });

    this.wrapper
      .getJointPortUnhighlightStream()
      .pipe(untilDestroyed(this))
      .subscribe(operatorPortIDs => {
        operatorPortIDs.forEach(operatorPortID => {
          const operatorJointElement = <joint.dia.Element>(
            this.workflowActionService.getJointGraph().getCell(operatorPortID.operatorID)
          );
          operatorJointElement.portProp(operatorPortID.portID, "attrs/.port-body", {
            r: 5,
            stroke: "none",
          });
        });
      });
  }

  private openCommentBox(commentBoxID: string): void {
    const commentBox = this.workflowActionService.getTexeraGraph().getSharedCommentBoxType(commentBoxID);
    const modalRef: NzModalRef = this.nzModalService.create({
      // modal title
      nzTitle: "Comments",
      nzContent: NzModalCommentBoxComponent,
      // set component @Input attributes
      nzData: { commentBox: commentBox }, // set the index value and page size to the modal for navigation
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
    modalRef.afterClose.pipe(untilDestroyed(this)).subscribe(() => {
      this.wrapper.unhighlightCommentBoxes(commentBoxID);
      this.setURLFragment(null);
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
      .subscribe(value => this.paper.findViewByModel(value).highlight("rect.body", { highlighter: highlightOptions }));

    this.dragDropService
      .getOperatorSuggestionUnhighlightStream()
      .pipe(untilDestroyed(this))
      .subscribe(value =>
        this.paper.findViewByModel(value).unhighlight("rect.body", { highlighter: highlightOptions })
      );
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
    fromJointPaperEvent(this.paper, "element:delete")
      .pipe(
        filter(() => this.interactive),
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

  private handleViewAddPort(): void {
    fromJointPaperEvent(this.paper, "element:add-input-port")
      .pipe(
        filter(() => this.interactive),
        map(value => value[0])
      )
      .pipe(untilDestroyed(this))
      .subscribe(elementView => {
        if (this.workflowActionService.getTexeraGraph().hasOperator(elementView.model.id.toString())) {
          this.workflowActionService.addPort(elementView.model.id.toString(), true, false);
        }
      });
    fromJointPaperEvent(this.paper, "element:add-output-port")
      .pipe(
        filter(() => this.interactive),
        map(value => value[0])
      )
      .pipe(untilDestroyed(this))
      .subscribe(elementView => {
        if (this.workflowActionService.getTexeraGraph().hasOperator(elementView.model.id.toString())) {
          this.workflowActionService.addPort(elementView.model.id.toString(), false);
        }
      });
  }

  private handleViewRemovePort(): void {
    fromJointPaperEvent(this.paper, "element:remove-input-port")
      .pipe(
        filter(() => this.interactive),
        map(value => value[0])
      )
      .pipe(untilDestroyed(this))
      .subscribe(elementView => {
        if (this.workflowActionService.getTexeraGraph().hasOperator(elementView.model.id.toString())) {
          this.workflowActionService.removePort(elementView.model.id.toString(), true);
        }
      });
    fromJointPaperEvent(this.paper, "element:remove-output-port")
      .pipe(
        filter(() => this.interactive),
        map(value => value[0])
      )
      .pipe(untilDestroyed(this))
      .subscribe(elementView => {
        if (this.workflowActionService.getTexeraGraph().hasOperator(elementView.model.id.toString())) {
          this.workflowActionService.removePort(elementView.model.id.toString(), false);
        }
      });
  }

  private handlePortClick(): void {
    fromJointPaperEvent(this.paper, "element:magnet:pointerclick")
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        // set the multi-select mode
        this.wrapper.setMultiSelectMode(<boolean>event[1].shiftKey);

        const clickedPortID: LogicalPort = {
          operatorID: event[0].model.id as string,
          portID: event[2].getAttribute("port") as string,
        };

        if (event[1].shiftKey) {
          if (_.find(this.wrapper.getCurrentHighlightedPortIDs(), clickedPortID) !== undefined) {
            // if the link being clicked is already highlighted, unhighlight it
            this.workflowActionService.unhighlightPorts(clickedPortID);
          } else if (this.workflowActionService.getTexeraGraph().hasOperator(clickedPortID.operatorID)) {
            // highlight the link if the link has not already been highlighted
            this.workflowActionService.highlightPorts(<boolean>event[1].shiftKey, clickedPortID);
          }
        } else {
          // if user doesn't click on the shift key, highlight only a single port
          if (this.workflowActionService.getTexeraGraph().hasOperator(clickedPortID.operatorID)) {
            this.workflowActionService.highlightPorts(<boolean>event[1].shiftKey, clickedPortID);
          }
        }
      });
  }

  private handleViewMouseoverOperator(): void {
    fromJointPaperEvent(this.paper, "element:mouseenter")
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        this.jointUIService.unfoldOperatorDetails(this.paper, event[0].model.id.toString());
      });
  }

  private handleViewMouseoutOperator(): void {
    fromJointPaperEvent(this.paper, "element:mouseleave")
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        this.jointUIService.foldOperatorDetails(this.paper, event[0].model.id.toString());
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
    fromJointPaperEvent(this.paper, "tool:remove")
      .pipe(
        filter(() => this.interactive),
        map(value => value[0])
      )
      .pipe(untilDestroyed(this))
      .subscribe(elementView => {
        this.workflowActionService.deleteLinkWithID(elementView.model.id.toString());
      });
  }

  /**
   * if the operator is valid , the border of the box will be default
   */
  private handleOperatorValidation(): void {
    this.validationWorkflowService
      .getOperatorValidationStream()
      .pipe(untilDestroyed(this))
      .subscribe(value =>
        this.jointUIService.changeOperatorColor(this.paper, value.operatorID, value.validation.isValid)
      );
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
        allowMultiInput = portInfo?.allowMultiLinks ?? false;
      }
    }
    return !(connectedLinksToTargetPort.length > 0 && !allowMultiInput);
  }

  /**
   * Deletes currently highlighted operators and groups when user presses the delete key.
   * When the focus is not on root document body, operator should not be deleted
   */
  private handleElementDelete(): void {
    fromEvent<KeyboardEvent>(document, "keydown")
      .pipe(
        filter(() => document.activeElement === document.body),
        filter(() => this.interactive),
        filter(event => event.key === "Backspace" || event.key === "Delete")
      )
      .pipe(untilDestroyed(this))
      .subscribe(() => this.deleteElements());
  }

  private deleteElements(): void {
    this.workflowActionService.deleteOperatorsAndLinks(this.wrapper.getCurrentHighlightedOperatorIDs());
    this.wrapper
      .getCurrentHighlightedCommentBoxIDs()
      .forEach(highlightedCommentBoxesID => this.workflowActionService.deleteCommentBox(highlightedCommentBoxesID));
  }

  /**
   * Highlight all operators and groups on the graph when user presses command/ctrl + A.
   */
  private handleElementSelectAll(): void {
    fromEvent<KeyboardEvent>(document, "keydown")
      .pipe(
        filter(() => document.activeElement === document.body),
        filter(event => (event.metaKey || event.ctrlKey) && event.key === "a")
      )
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        event.preventDefault();
        const allOperators = this.workflowActionService
          .getTexeraGraph()
          .getAllOperators()
          .map(operator => operator.operatorID);
        const allLinks = this.workflowActionService
          .getTexeraGraph()
          .getAllLinks()
          .map(link => link.linkID);
        const allCommentBoxes = this.workflowActionService
          .getTexeraGraph()
          .getAllCommentBoxes()
          .map(CommentBox => CommentBox.commentBoxID);
        this.wrapper.setMultiSelectMode(allOperators.length + allCommentBoxes.length > 1);
        this.workflowActionService.highlightLinks(allLinks.length > 1, ...allLinks);
        this.workflowActionService.highlightOperators(allOperators.length > 1, ...allOperators);
        this.workflowActionService.highlightCommentBoxes(
          allOperators.length + allCommentBoxes.length > 1,
          ...allCommentBoxes
        );
      });
  }

  /**
   * Caches the currently highlighted operators' info when user
   * triggers the copy event (i.e. presses command/ctrl + c on
   * keyboard or selects copy option from the browser menu).
   */
  private handleElementCopy(): void {
    fromEvent<ClipboardEvent>(document, "copy")
      .pipe(
        filter(_ => document.activeElement === document.body),
        untilDestroyed(this)
      )
      .subscribe(() => {
        if (
          this.operatorMenu.highlightedOperators.value.length > 0 ||
          this.operatorMenu.highlightedCommentBoxes.value.length > 0
        ) {
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
        filter(() => document.activeElement === document.body),
        filter(() => this.interactive),
        untilDestroyed(this)
      )
      .subscribe(() => {
        if (
          this.operatorMenu.highlightedOperators.value.length > 0 ||
          this.operatorMenu.highlightedCommentBoxes.value.length > 0
        ) {
          this.operatorMenu.saveHighlightedElements();
          this.deleteElements();
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
        filter(() => document.activeElement === document.body),
        filter(() => this.interactive),
        untilDestroyed(this)
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
    fromJointPaperEvent(this.paper, "link:mouseenter")
      .pipe(map(value => value[0]))
      .pipe(untilDestroyed(this))
      .subscribe(elementView => {
        if (environment.linkBreakpointEnabled) {
          this.paper.getModelById(elementView.model.id).attr({
            ".tool-remove": { display: "block" },
          });
          this.paper.getModelById(elementView.model.id).findView(this.paper).showTools();
        } else {
          // only display the delete button
          this.paper.getModelById(elementView.model.id).attr({
            ".tool-remove": { display: "block" },
          });
        }
      });

    /**
     * When the cursor leaves a link, the delete button disappears.
     * If there is no breakpoint present on that link, the breakpoint button also disappears,
     * otherwise, the breakpoint button is not changed.
     */
    fromJointPaperEvent(this.paper, "link:mouseleave")
      .pipe(map(value => value[0]))
      .pipe(untilDestroyed(this))
      .subscribe(elementView => {
        // ensure that the link element exists
        if (this.paper.getModelById(elementView.model.id)) {
          const LinksWithBreakpoint = this.wrapper.getLinkIDsWithBreakpoint();
          if (!LinksWithBreakpoint.includes(elementView.model.id.toString())) {
            this.paper.getModelById(elementView.model.id).findView(this.paper).hideTools();
          }
          this.paper.getModelById(elementView.model.id).attr({
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
    this.wrapper
      .getJointLinkCellAddStream()
      .pipe(this.wrapper.jointGraphContext.bufferWhileAsync, untilDestroyed(this))
      .subscribe(link => {
        const linkView = link.findView(this.paper);
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
    fromJointPaperEvent(this.paper, "tool:breakpoint")
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        // set the multi-select mode
        this.wrapper.setMultiSelectMode(<boolean>event[1].shiftKey);
        const clickedLinkID = event[0].model.id.toString();
        if (event[1].shiftKey) {
          if (this.wrapper.getCurrentHighlightedLinkIDs().includes(clickedLinkID)) {
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
    this.wrapper
      .getLinkHighlightStream()
      .pipe(untilDestroyed(this))
      .subscribe(linkIDs => {
        linkIDs.forEach(linkID => {
          this.paper.getModelById(linkID).attr({
            ".connection": { stroke: "orange" },
            ".marker-source": { fill: "orange" },
            ".marker-target": { fill: "orange" },
          });
        });
      });

    this.wrapper
      .getLinkUnhighlightStream()
      .pipe(untilDestroyed(this))
      .subscribe(linkIDs => {
        linkIDs.forEach(linkID => {
          this.paper.findViewByModel(linkID);
          if (this.paper.getModelById(linkID)) {
            // ensure that the link still exist
            this.paper.getModelById(linkID).attr({
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
    this.wrapper
      .getLinkBreakpointShowStream()
      .pipe(this.wrapper.jointGraphContext.bufferWhileAsync, untilDestroyed(this))
      .subscribe(linkID => {
        this.paper.getModelById(linkID.linkID).findView(this.paper).showTools();
      });

    this.wrapper
      .getLinkBreakpointHideStream()
      .pipe(this.wrapper.jointGraphContext.bufferWhileAsync, untilDestroyed(this))
      .subscribe(linkID => {
        this.paper.getModelById(linkID.linkID).findView(this.paper).hideTools();
      });
  }

  private isSource(operatorID: string): boolean {
    return this.workflowActionService.getTexeraGraph().getOperator(operatorID).inputPorts.length == 0;
  }

  private isSink(operatorID: string): boolean {
    return this.workflowActionService.getTexeraGraph().getOperator(operatorID).outputPorts.length == 0;
  }

  /**
   * This function handles the event stream from jointGraph to toggle the grids in jointPaper on or off.
   * @private
   */
  private handleGridsToggle(): void {
    this.wrapper
      .getJointPaperGridsToggleStream()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        if (this.gridOn) {
          this.paper.setGridSize(1);
          this.gridOn = false;
        } else {
          this.paper.setGridSize(2);
          this.gridOn = true;
        }
      });
  }

  /**
   * Handles mouse events to enable shared cursor.
   */
  private handlePointerEvents(): void {
    fromEvent<MouseEvent>(this.editor, "mousemove")
      .pipe(untilDestroyed(this))
      .subscribe(e => {
        const jointPoint = this.paper.clientToLocalPoint({ x: e.clientX, y: e.clientY });
        this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("userCursor", jointPoint);
      });
    fromEvent<MouseEvent>(this.editor, "mouseleave")
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("isActive", false);
      });
    fromEvent<MouseEvent>(this.editor, "mouseenter")
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("isActive", true);
      });
  }

  private setURLFragment(fragment: string | null): void {
    this.router.navigate([], {
      relativeTo: this.route,
      fragment: fragment !== null ? fragment : undefined,
      preserveFragment: false,
    });
  }

  private handleURLFragment(): void {
    // when operator/link/comment box is highlighted/unhighlighted, update URL fragment
    merge(
      this.wrapper.getJointOperatorHighlightStream(),
      this.wrapper.getJointOperatorUnhighlightStream(),
      this.wrapper.getLinkHighlightStream(),
      this.wrapper.getLinkUnhighlightStream(),
      this.wrapper.getJointCommentBoxHighlightStream(),
      this.wrapper.getJointCommentBoxUnhighlightStream()
    )
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        // add element ID to URL fragment when only one element is highlighted
        // clear URL fragment when no element or multiple elements are highlighted
        //          from state      -> to state
        // case 1a: no highlighted  -> highlight one element
        // case 1b: more than one elements highlighted -> unhighlight some elements so that only one element is highlighted
        // for case 1: set URL fragment to the highlighted element
        // case 2a: one element highlighted -> unhighlight the element
        // case 2b: one element highlighted -> highlight another element
        // for case 2: clear URL fragment
        // other cases, do nothing
        const highlightedIds = this.wrapper.getCurrentHighlightedIDs();
        if (highlightedIds.length === 1) {
          this.setURLFragment(highlightedIds[0]);
        } else {
          this.setURLFragment(null);
        }
      });

    // special case: open comment box when URL fragment is set
    this.workflowActionService
      .getTexeraGraph()
      .getCommentBoxAddStream()
      .pipe(untilDestroyed(this))
      .subscribe(box => {
        if (this.route.snapshot.fragment === box.commentBoxID) {
          this.openCommentBox(box.commentBoxID);
        }
      });
  }
  invokeResize() {
    const resizeEvent = new Event("resize");
    setTimeout(() => {
      window.dispatchEvent(resizeEvent);
    }, 175);
  }
}

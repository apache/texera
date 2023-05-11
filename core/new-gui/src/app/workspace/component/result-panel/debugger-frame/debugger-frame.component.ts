import { Component, Input, OnChanges, OnInit, SimpleChanges } from "@angular/core";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { BreakpointTriggerInfo } from "../../../types/workflow-common.interface";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { WorkflowWebsocketService } from "../../../service/workflow-websocket/workflow-websocket.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { TypedValue } from "../../../types/workflow-websocket.interface";
import { FlatTreeControl, TreeControl } from "@angular/cdk/tree";
import { CollectionViewer, DataSource, SelectionChange } from "@angular/cdk/collections";
import { BehaviorSubject, merge, Observable } from "rxjs";
import { map, tap } from "rxjs/operators";

interface FlatTreeNode {
  expandable: boolean;
  expression: string;
  name: string;
  type: string;
  value: string;
  level: number;
  loading?: boolean;
}

@UntilDestroy()
@Component({
  selector: "texera-debugger-frame",
  templateUrl: "./debugger-frame.component.html",
  styleUrls: ["./debugger-frame.component.scss"],
})
export class DebuggerFrameComponent implements OnInit, OnChanges {
  @Input() operatorId?: string;
  // display breakpoint
  breakpointTriggerInfo?: BreakpointTriggerInfo;
  breakpointAction: boolean = false;

  expressionTreeControl = new FlatTreeControl<FlatTreeNode>(
    node => node.level,
    node => node.expandable
  );
  pythonExpressionSource?: PythonExpressionSource;
  hasNoContent = (_: number, node: FlatTreeNode) => node.name === "";

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private notificationService: NotificationService
  ) {}

  ngOnChanges(changes: SimpleChanges): void {
    this.operatorId = changes.operatorId?.currentValue;
    this.renderConsole();
  }

  renderConsole() {
    // try to fetch if we have breakpoint info
    this.breakpointTriggerInfo = this.executeWorkflowService.getBreakpointTriggerInfo();
    if (this.breakpointTriggerInfo) {
      this.breakpointAction = true;
    }
  }

  onClickSkipTuples(): void {
    try {
      this.executeWorkflowService.skipTuples();
    } catch (e) {
      this.notificationService.error(e);
    }
    this.breakpointAction = false;
  }

  onClickRetry() {
    try {
      this.executeWorkflowService.retryExecution();
    } catch (e) {
      this.notificationService.error(e);
    }
    this.breakpointAction = false;
  }

  onClickEvaluate() {
    if (this.operatorId) {
      this.workflowWebsocketService.send("PythonExpressionEvaluateRequest", {
        expression: "self",
        operatorId: this.operatorId,
      });
    }
  }

  ngOnInit(): void {
    this.pythonExpressionSource = new PythonExpressionSource(
      this.expressionTreeControl,
      this.workflowWebsocketService,
      this.operatorId
    );
  }

  saveNode(node: FlatTreeNode, value: string) {
    if (this.operatorId) {
      this.pythonExpressionSource?.removeNode(node);
      this.workflowWebsocketService.send("PythonExpressionEvaluateRequest", {
        expression: value,
        operatorId: this.operatorId,
      });
    }
  }

  addNewNode(): void {
    const flattenedData = this.pythonExpressionSource?.flattenedDataSubject.getValue();
    // append new expressions as new tree roots
    flattenedData?.push(<FlatTreeNode>{
      expandable: false,
      expression: "please input",
      name: "",
      type: "",
      value: "",
      level: 0,
      loading: false,
    });
    if (flattenedData) {
      this.pythonExpressionSource?.flattenedDataSubject.next(flattenedData);
    }
  }
}

@UntilDestroy()
class PythonExpressionSource implements DataSource<FlatTreeNode> {
  public readonly flattenedDataSubject: BehaviorSubject<FlatTreeNode[]>;
  private childrenLoadedSet = new Set<FlatTreeNode>();

  constructor(
    private treeControl: TreeControl<FlatTreeNode>,
    private workflowWebsocketService: WorkflowWebsocketService,
    private operatorId?: string
  ) {
    this.flattenedDataSubject = new BehaviorSubject<FlatTreeNode[]>([]);
    treeControl.dataNodes = [];

    this.registerEvaluatedValuesHandler();
  }

  removeNode(node: FlatTreeNode): void {
    const flattenedData = this.flattenedDataSubject.getValue();
    this.flattenedDataSubject.next(flattenedData.filter((value, _, __) => value !== node));
    console.log(flattenedData.filter((value, _, __) => value !== node));
  }

  toFlatTreeNode(value: TypedValue, parentNode?: FlatTreeNode): FlatTreeNode {
    return <FlatTreeNode>{
      expression: (parentNode?.expression ? parentNode?.expression + "." : "") + value.expression,
      name: value.valueRef,
      type: value.valueType,
      value: value.valueStr,
      expandable: value.expandable,
      level: (parentNode?.level ?? -1) + 1,
    };
  }

  connect(collectionViewer: CollectionViewer): Observable<FlatTreeNode[]> {
    const changes = [
      collectionViewer.viewChange,
      this.treeControl.expansionModel.changed.pipe(tap(change => this.handleExpansionChange(change))),
      this.flattenedDataSubject.asObservable(),
    ];
    return merge(...changes).pipe(map(() => this.expandFlattenedNodes(this.flattenedDataSubject.getValue())));
  }

  expandFlattenedNodes(nodes: FlatTreeNode[]): FlatTreeNode[] {
    console.log(nodes);
    const treeControl = this.treeControl;
    const results: FlatTreeNode[] = [];
    const currentExpand: boolean[] = [];
    currentExpand[0] = true;

    nodes.forEach(node => {
      let expand = true;
      for (let i = 0; i <= treeControl.getLevel(node); i++) {
        expand = expand && currentExpand[i];
      }
      if (expand) {
        results.push(node);
      }
      if (treeControl.isExpandable(node)) {
        currentExpand[treeControl.getLevel(node) + 1] = treeControl.isExpanded(node);
      }
    });
    return results;
  }

  handleExpansionChange(change: SelectionChange<FlatTreeNode>): void {
    if (change.added) {
      change.added.forEach(node => this.loadChildren(node));
    }
  }

  loadChildren(node: FlatTreeNode): void {
    if (this.childrenLoadedSet.has(node)) {
      return;
    }
    node.loading = true;
    this.getChildren(node);
  }

  disconnect(): void {
    this.flattenedDataSubject.complete();
  }

  getChildren(node: FlatTreeNode): void {
    if (this.operatorId) {
      this.workflowWebsocketService.send("PythonExpressionEvaluateRequest", {
        expression: node.expression,
        operatorId: this.operatorId,
      });
    }
  }

  registerEvaluatedValuesHandler() {
    this.workflowWebsocketService
      .subscribeToEvent("PythonExpressionEvaluateResponse")
      .pipe(untilDestroyed(this))
      .subscribe(response => {
        const flattenedData = this.flattenedDataSubject.getValue();
        const parentNode = flattenedData.find(node => node.expression === response.expression);

        if (parentNode) {
          // found parent node, add to it as children
          response.values.forEach(evaluatedValue => {
            const treeNodes = evaluatedValue.attributes.map(typedValue => this.toFlatTreeNode(typedValue, parentNode));
            const index = flattenedData.indexOf(parentNode);
            if (index !== -1) {
              flattenedData.splice(index + 1, 0, ...treeNodes);
              this.childrenLoadedSet.add(parentNode);
            }
            parentNode.loading = false;
          });
        } else {
          // append new expressions as new tree roots
          const newRootNodes = response.values.map(evaluatedValue => this.toFlatTreeNode(evaluatedValue.value));
          flattenedData.push(...newRootNodes);
        }
        this.flattenedDataSubject.next(flattenedData);
      });
  }
}

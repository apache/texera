import {
  Component,
  Input,
  OnChanges,
  OnInit,
  SimpleChanges
} from "@angular/core";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { BreakpointTriggerInfo } from "../../../types/workflow-common.interface";
import { WorkflowWebsocketService } from "../../../service/workflow-websocket/workflow-websocket.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { EvaluatedValue } from "../../../types/workflow-websocket.interface";
import { FlatTreeControl, TreeControl } from "@angular/cdk/tree";
import {
  CollectionViewer,
  DataSource,
  SelectionChange
} from "@angular/cdk/collections";
import { BehaviorSubject, merge, Observable } from "rxjs";
import { map, tap } from "rxjs/operators";

interface FlatNode {
  expandable: boolean;
  expression: string;
  label: string;
  level: number;
  loading?: boolean;
}

@UntilDestroy()
@Component({
  selector: "texera-debugger-frame",
  templateUrl: "./debugger-frame.component.html",
  styleUrls: ["./debugger-frame.component.scss"]
})
export class DebuggerFrameComponent implements OnInit, OnChanges {
  @Input() operatorId?: string;
  // display breakpoint
  breakpointTriggerInfo?: BreakpointTriggerInfo;
  breakpointAction: boolean = false;
  evaluateTrees: object[] = [];
  treeControl = new FlatTreeControl<FlatNode>(
    (node) => node.level,
    (node) => node.expandable
  );
  TREE_DATA: FlatNode[] = [
    // {
    //   expandable: true,
    //   expression: "self",
    //   label: "UDF",
    //   level: 0
    // }
  ];
  dataSource?: DynamicDatasource;
  hasChild = (_: number, node: FlatNode) => node.expandable;

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowWebsocketService: WorkflowWebsocketService
  ) {}

  ngOnChanges(changes: SimpleChanges): void {
    this.operatorId = changes.operatorId?.currentValue;
    this.renderConsole();
  }

  renderConsole() {
    // try to fetch if we have breakpoint info
    this.breakpointTriggerInfo =
      this.executeWorkflowService.getBreakpointTriggerInfo();
    if (this.breakpointTriggerInfo) {
      this.breakpointAction = true;
    }
  }

  onClickSkipTuples(): void {
    this.executeWorkflowService.skipTuples();
    this.breakpointAction = false;
  }

  onClickRetry() {
    this.executeWorkflowService.retryExecution();
    this.breakpointAction = false;
  }

  onClickEvaluate() {
    if (this.operatorId) {
      this.workflowWebsocketService.send("PythonExpressionEvaluateRequest", {
        expression: "self",
        operatorId: this.operatorId
      });
    }
  }

  ngOnInit(): void {
    this.dataSource = new DynamicDatasource(
      this.treeControl,
      this.TREE_DATA,
      this.workflowWebsocketService,
      this.operatorId
    );
  }
}

@UntilDestroy()
class DynamicDatasource implements DataSource<FlatNode> {
  private flattenedData: BehaviorSubject<FlatNode[]>;
  private childrenLoadedSet = new Set<FlatNode>();

  constructor(
    private treeControl: TreeControl<FlatNode>,
    initData: FlatNode[],
    private workflowWebsocketService: WorkflowWebsocketService,
    private operatorId?: string
  ) {
    this.flattenedData = new BehaviorSubject<FlatNode[]>(initData);
    treeControl.dataNodes = initData;

    function toTreeNode(
      value: EvaluatedValue,
      parentNode?: FlatNode
    ): FlatNode[] {
      return value.attributes.map(
        (typedValue) =>
          <FlatNode>{
            expression:
              (parentNode?.expression ? parentNode?.expression + "." : "") +
              typedValue.expression,
            label:
              typedValue.expression +
              "(" +
              typedValue.valueType +
              "): " +
              typedValue.valueStr,
            expandable: typedValue.expandable,
            level: (parentNode?.level ?? -1) + 1
          }
      );
    }

    this.workflowWebsocketService
      .subscribeToEvent("PythonExpressionEvaluateResponse")
      .pipe(untilDestroyed(this))
      .subscribe((root) => {
        const flattenedData = this.flattenedData.getValue();
        const node = flattenedData.find(
          (data) => data.expression === root.expression
        );

        if (node) {
          root.values.forEach((evaluatedValue) => {
            const treeNodes = toTreeNode(evaluatedValue, node);
            const index = flattenedData.indexOf(node);
            if (index !== -1) {
              flattenedData.splice(index + 1, 0, ...treeNodes);
              this.childrenLoadedSet.add(node);
            }
            this.flattenedData.next(flattenedData);

            node.loading = false;
          });
        } else {
          root.values.forEach((evaluatedValue) => {
            flattenedData.push(<FlatNode>{
              expression: evaluatedValue.value.expression,
              label:
                evaluatedValue.value.expression +
                "(" +
                evaluatedValue.value.valueType +
                "): " +
                evaluatedValue.value.valueStr,
              level: 0,
              expandable: evaluatedValue.value.expandable
            });
          });
          this.flattenedData.next(flattenedData);
        }
      });
  }

  connect(collectionViewer: CollectionViewer): Observable<FlatNode[]> {
    const changes = [
      collectionViewer.viewChange,
      this.treeControl.expansionModel.changed.pipe(
        tap((change) => this.handleExpansionChange(change))
      ),
      this.flattenedData
    ];
    return merge(...changes).pipe(
      map(() => this.expandFlattenedNodes(this.flattenedData.getValue()))
    );
  }

  expandFlattenedNodes(nodes: FlatNode[]): FlatNode[] {
    const treeControl = this.treeControl;
    const results: FlatNode[] = [];
    const currentExpand: boolean[] = [];
    currentExpand[0] = true;

    nodes.forEach((node) => {
      let expand = true;
      for (let i = 0; i <= treeControl.getLevel(node); i++) {
        expand = expand && currentExpand[i];
      }
      if (expand) {
        results.push(node);
      }
      if (treeControl.isExpandable(node)) {
        currentExpand[treeControl.getLevel(node) + 1] =
          treeControl.isExpanded(node);
      }
    });
    return results;
  }

  handleExpansionChange(change: SelectionChange<FlatNode>): void {
    if (change.added) {
      change.added.forEach((node) => this.loadChildren(node));
    }
  }

  loadChildren(node: FlatNode): void {
    if (this.childrenLoadedSet.has(node)) {
      return;
    }
    node.loading = true;
    this.getChildren(node);
  }

  disconnect(): void {
    this.flattenedData.complete();
  }

  getChildren(node: FlatNode): void {
    if (this.operatorId) {
      this.workflowWebsocketService.send("PythonExpressionEvaluateRequest", {
        expression: node.expression,
        operatorId: this.operatorId
      });
    }
  }
}


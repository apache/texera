import { Injectable } from "@angular/core";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { SCHEMA_PROPAGATION_DEBOUNCE_TIME_MS } from "../dynamic-schema/schema-propagation/schema-propagation.service";
import { debounceTime, filter } from "rxjs/operators";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { merge } from "rxjs";
import { JointUIService } from "../joint-ui/joint-ui.service";
import { ExecutionState } from "../../types/execute-workflow.interface";

@Injectable({
  providedIn: "root",
})
export class OperatorReuseCacheStatusService {
  constructor(
    private jointUIService: JointUIService,
    private workflowActionService: WorkflowActionService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private executeWorkflowService: ExecuteWorkflowService
  ) {
    this.registerRequestCacheStatusUpdate();
    this.registerHandleCacheStatusUpdate();
  }

  /**
   * Requests cache status (invalid/valid) when workflow is changed from the engine
   * for example, when operator is updated, the cache status might be invalidated
   */
  private registerRequestCacheStatusUpdate() {
    merge(
      this.workflowActionService.getTexeraGraph().getLinkAddStream(),
      this.workflowActionService.getTexeraGraph().getLinkDeleteStream(),
      this.workflowActionService
        .getTexeraGraph()
        .getOperatorPropertyChangeStream()
        .pipe(debounceTime(SCHEMA_PROPAGATION_DEBOUNCE_TIME_MS)),
      this.workflowActionService.getTexeraGraph().getDisabledOperatorsChangedStream(),
      this.workflowActionService.getTexeraGraph().getReuseCacheOperatorsChangedStream(),
      this.executeWorkflowService
        .getExecutionStateStream()
        .pipe(
          filter(
            evt => evt.previous.state !== ExecutionState.Completed && evt.current.state == ExecutionState.Completed
          )
        )
    ).subscribe(() => {
      const workflow = ExecuteWorkflowService.getLogicalPlanRequest(this.workflowActionService.getTexeraGraph());
      this.workflowWebsocketService.send("CacheStatusUpdateRequest", workflow);
    });
  }

  /**
   * Registers handler for cache status update from the backend.
   */
  private registerHandleCacheStatusUpdate() {
    this.workflowActionService
      .getTexeraGraph()
      .getReuseCacheOperatorsChangedStream()
      .subscribe(event => {
        const mainJointPaper = this.workflowActionService.getJointGraphWrapper().getMainJointPaper();
        if (!mainJointPaper) {
          return;
        }

        event.newReuseCacheOps.concat(event.newUnreuseCacheOps).forEach(opID => {
          const op = this.workflowActionService.getTexeraGraph().getOperator(opID);

          this.jointUIService.changeOperatorReuseCacheStatus(mainJointPaper, op);
        });
      });
    this.workflowWebsocketService.subscribeToEvent("CacheStatusUpdateEvent").subscribe(event => {
      const mainJointPaper = this.workflowActionService.getJointGraphWrapper().getMainJointPaper();
      if (!mainJointPaper) {
        return;
      }
      Object.entries(event.cacheStatusMap).forEach(([opID, cacheStatus]) => {
        const op = this.workflowActionService.getTexeraGraph().getOperator(opID);
        this.jointUIService.changeOperatorReuseCacheStatus(mainJointPaper, op, cacheStatus);
      });
    });
  }
}

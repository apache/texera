import { Injectable } from "@angular/core";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { SCHEMA_PROPAGATION_DEBOUNCE_TIME_MS } from "../dynamic-schema/schema-propagation/schema-propagation.service";
import { debounceTime } from "rxjs/operators";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { merge } from "rxjs";
import { JointUIService } from "../joint-ui/joint-ui.service";
import { environment } from "src/environments/environment";

@Injectable({
  providedIn: "root",
})
export class OperatorCacheStatusService {
  constructor(
    private jointUIService: JointUIService,
    private workflowActionService: WorkflowActionService,
    private workflowWebsocketService: WorkflowWebsocketService
  ) {
    if (!environment.operatorCacheEnabled) {
      return;
    }
    this.registerRequestCacheStatusUpdate();
    this.registerHandleCacheStatusUpdate();
  }

  /**
   * Requests cache status (invalid/valid/toBeCached) when workflow is changed from the engine
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
      this.workflowActionService.getTexeraGraph().getCachedOperatorsChangedStream()
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
      .getCachedOperatorsChangedStream()
      .subscribe(event => {
        const mainJointPaper = this.workflowActionService.getJointGraphWrapper().getMainJointPaper();
        if (!mainJointPaper) {
          return;
        }

        event.newCached.concat(event.newUnCached).forEach(opID => {
          const op = this.workflowActionService.getTexeraGraph().getOperator(opID);

          this.jointUIService.changeOperatorCacheStatus(mainJointPaper, op);
        });
      });
    this.workflowWebsocketService.subscribeToEvent("CacheStatusUpdateEvent").subscribe(event => {
      const mainJointPaper = this.workflowActionService.getJointGraphWrapper().getMainJointPaper();
      if (!mainJointPaper) {
        return;
      }
      Object.entries(event.cacheStatusMap).forEach(([opID, cacheStatus]) => {
        const op = this.workflowActionService.getTexeraGraph().getOperator(opID);
        this.jointUIService.changeOperatorCacheStatus(mainJointPaper, op, cacheStatus);
      });
    });
  }
}

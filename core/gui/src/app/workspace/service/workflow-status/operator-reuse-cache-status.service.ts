import { Injectable } from "@angular/core";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { JointUIService } from "../joint-ui/joint-ui.service";
@Injectable({
  providedIn: "root",
})
export class OperatorReuseCacheStatusService {
  constructor(
    private jointUIService: JointUIService,
    private workflowActionService: WorkflowActionService,
    private workflowWebsocketService: WorkflowWebsocketService
  ) {
    this.registerHandleCacheStatusUpdate();
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

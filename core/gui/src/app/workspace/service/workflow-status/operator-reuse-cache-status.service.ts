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

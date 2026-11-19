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

import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { LogicalPort } from "../../types/workflow-common.interface";
import { isMacroTerminal } from "../../../common/util/workflow-macro";
import { v4 as uuid } from "uuid";

export interface MacroTerminal {
  direction: "inputs" | "outputs";
  portID?: string;
}

/** Expose a normal operator port using the same graph actions as dragging a connection. */
export class MacroTerminals {
  constructor(private readonly actions: WorkflowActionService) {}

  public direction(target: LogicalPort): MacroTerminal["direction"] | undefined {
    if (!this.actions.getMacroInterface()) return undefined;
    const op = this.actions.getTexeraGraph().getOperator(target.operatorID);
    if (isMacroTerminal(op)) return undefined;
    return op.inputPorts.some(port => port.portID === target.portID) ? "inputs" : "outputs";
  }

  public needsReplacement(target: LogicalPort): boolean {
    return (
      this.direction(target) === "inputs" &&
      this.actions
        .getTexeraGraph()
        .getInputLinksByOperatorId(target.operatorID)
        .some(link => link.target.portID === target.portID)
    );
  }

  public expose(target: LogicalPort, replace = false): void {
    const direction = this.direction(target);
    if (!direction || !this.actions.checkWorkflowModificationEnabled()) return;
    const input = direction === "inputs";
    const graph = this.actions.getTexeraGraph();
    const node = graph.getAllOperators().find(op => op.operatorType === (input ? "MacroInput" : "MacroOutput"));
    if (!node) return;
    const links = graph.getAllLinks();
    const existing = links.find(link => {
      const end = input ? link.target : link.source;
      const boundary = input ? link.source : link.target;
      return (
        end.operatorID === target.operatorID && end.portID === target.portID && boundary.operatorID === node.operatorID
      );
    });
    if (existing) {
      this.actions.highlightPorts(false, input ? existing.source : existing.target);
      return;
    }
    if (this.needsReplacement(target) && !replace) return;
    this.actions.bundleMacroActions(() => {
      if (input && replace)
        links
          .filter(link => link.target.operatorID === target.operatorID && link.target.portID === target.portID)
          .forEach(link => this.actions.deleteLinkWithID(link.linkID));
      this.actions.addPort(node.operatorID, !input);
      const portID = (
        input ? graph.getOperator(node.operatorID).outputPorts : graph.getOperator(node.operatorID).inputPorts
      ).at(-1)!.portID;
      const boundary = { operatorID: node.operatorID, portID };
      this.actions.addLink({ linkID: uuid(), source: input ? boundary : target, target: input ? target : boundary });
      this.actions.highlightPorts(false, boundary);
    });
  }
}

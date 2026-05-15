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

import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { PortIdentity } from "../../types/execute-workflow.interface";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";

export const MACRO_BASE_URL = "macro";
export const MACRO_CREATE_URL = MACRO_BASE_URL + "/create";
export const MACRO_LIST_URL = MACRO_BASE_URL + "/list";

// Mirrors the case classes on `MacroResource` (amber). Keeping the shapes
// hand-typed (rather than generating) so the dev loop stays simple.
export interface MacroPortSpec {
  index: number;
  displayName?: string;
}

export interface PortSpec {
  inputs: MacroPortSpec[];
  outputs: MacroPortSpec[];
}

export interface MacroCreateRequest {
  name: string;
  description?: string;
  content: string;
  isPublic?: boolean;
  portSpec: PortSpec;
  paramSpec?: unknown;
  category?: string;
  icon?: string;
}

export interface MacroDetail {
  wid: number;
  name: string;
  description: string;
  content: string;
  creationTime: string;
  lastModifiedTime: string;
  isPublic: boolean;
  portSpec: PortSpec;
  paramSpec: unknown;
  category?: string;
  icon?: string;
  isOwner: boolean;
  readonly: boolean;
}

export interface MacroSummary {
  wid: number;
  name: string;
  description: string;
  lastModifiedTime: string;
  portSpec: PortSpec;
  category?: string;
  icon?: string;
}

// Shape that MacroExpander (backend) reads off `workflow.content`. Matches the
// MacroBody / MacroLink case classes in `common/workflow-operator`.
interface MacroBodyLink {
  fromOpId: string;
  fromPortId: PortIdentity;
  toOpId: string;
  toPortId: PortIdentity;
}

interface MacroBody {
  operators: unknown[];
  links: MacroBodyLink[];
  inputs: MacroPortSpec[];
  outputs: MacroPortSpec[];
}

@Injectable({
  providedIn: "root",
})
export class MacroService {
  constructor(private http: HttpClient) {}

  public createMacro(req: MacroCreateRequest): Observable<MacroDetail> {
    return this.http.post<MacroDetail>(`${AppSettings.getApiEndpoint()}/${MACRO_CREATE_URL}`, req);
  }

  public listMacros(): Observable<MacroSummary[]> {
    return this.http.get<MacroSummary[]>(`${AppSettings.getApiEndpoint()}/${MACRO_LIST_URL}`);
  }

  public getMacro(wid: number): Observable<MacroDetail> {
    return this.http.get<MacroDetail>(`${AppSettings.getApiEndpoint()}/${MACRO_BASE_URL}/${wid}`);
  }

  /**
   * Build a `MacroCreateRequest` from the operators the user has multi-selected
   * on the parent canvas. Caller is responsible for sending it via `createMacro`.
   *
   * Boundary handling: for every link crossing the selection edge we add a
   * `MacroInput` / `MacroOutput` marker inside the body (one per unique inner
   * port) and rewire it so MacroExpander can splice the body back into a
   * parent at compile time. Internal links (both endpoints inside the
   * selection) are passed through with port-ordinal IDs to match the
   * backend's PortIdentity shape.
   */
  public buildMacroCreateRequestFromSelection(
    workflowActionService: WorkflowActionService,
    selectedOperatorIDs: readonly string[],
    name: string
  ): MacroCreateRequest {
    const graph = workflowActionService.getTexeraGraph();
    const selectedSet = new Set(selectedOperatorIDs);

    const innerOps = selectedOperatorIDs.map(opId => {
      const op = graph.getOperator(opId);
      // LogicalOp on the backend is reconstructed by Jackson from the same
      // shape the compiler uses — flat properties merged with the structural
      // bits (operatorID/Type/Version/ports).
      return {
        ...op.operatorProperties,
        operatorID: op.operatorID,
        operatorType: op.operatorType,
        operatorVersion: op.operatorVersion,
        inputPorts: op.inputPorts,
        outputPorts: op.outputPorts,
      };
    });

    const inputPortOrdinal = (operatorID: string, portID: string): number =>
      graph.getOperator(operatorID).inputPorts.findIndex(p => p.portID === portID);
    const outputPortOrdinal = (operatorID: string, portID: string): number =>
      graph.getOperator(operatorID).outputPorts.findIndex(p => p.portID === portID);

    const internal: { srcOp: string; srcPort: string; dstOp: string; dstPort: string }[] = [];
    const incoming: { srcOp: string; srcPort: string; dstOp: string; dstPort: string }[] = [];
    const outgoing: { srcOp: string; srcPort: string; dstOp: string; dstPort: string }[] = [];

    graph.getAllLinks().forEach(link => {
      const entry = {
        srcOp: link.source.operatorID,
        srcPort: link.source.portID,
        dstOp: link.target.operatorID,
        dstPort: link.target.portID,
      };
      const srcIn = selectedSet.has(entry.srcOp);
      const dstIn = selectedSet.has(entry.dstOp);
      if (srcIn && dstIn) internal.push(entry);
      else if (!srcIn && dstIn) incoming.push(entry);
      else if (srcIn && !dstIn) outgoing.push(entry);
    });

    // Allocate one MacroInput marker per unique (innerOp, innerPort) that is
    // fed by at least one external link. A single marker can have multiple
    // external feeders but it only drives one inner port.
    const incomingKeys = Array.from(new Set(incoming.map(l => `${l.dstOp}|${l.dstPort}`))).sort();
    const inputMarkers = incomingKeys.map((key, idx) => {
      const [innerOpId, innerPortID] = key.split("|");
      return {
        markerOpId: `MacroInput-operator-${this.uuid()}`,
        portIndex: idx,
        innerOpId,
        innerPortIdx: inputPortOrdinal(innerOpId, innerPortID),
      };
    });

    const outgoingKeys = Array.from(new Set(outgoing.map(l => `${l.srcOp}|${l.srcPort}`))).sort();
    const outputMarkers = outgoingKeys.map((key, idx) => {
      const [innerOpId, innerPortID] = key.split("|");
      return {
        markerOpId: `MacroOutput-operator-${this.uuid()}`,
        portIndex: idx,
        innerOpId,
        innerPortIdx: outputPortOrdinal(innerOpId, innerPortID),
      };
    });

    const markerOps: unknown[] = [
      ...inputMarkers.map(m => ({
        operatorID: m.markerOpId,
        operatorType: "MacroInput",
        operatorVersion: "",
        portIndex: m.portIndex,
        displayName: "",
        inputPorts: [],
        outputPorts: [{ id: { id: 0, internal: false }, displayName: "" }],
      })),
      ...outputMarkers.map(m => ({
        operatorID: m.markerOpId,
        operatorType: "MacroOutput",
        operatorVersion: "",
        portIndex: m.portIndex,
        displayName: "",
        inputPorts: [{ id: { id: 0, internal: false }, displayName: "" }],
        outputPorts: [],
      })),
    ];

    const internalLinks: MacroBodyLink[] = internal.map(l => ({
      fromOpId: l.srcOp,
      fromPortId: { id: outputPortOrdinal(l.srcOp, l.srcPort), internal: false },
      toOpId: l.dstOp,
      toPortId: { id: inputPortOrdinal(l.dstOp, l.dstPort), internal: false },
    }));

    const inputMarkerLinks: MacroBodyLink[] = inputMarkers.map(m => ({
      fromOpId: m.markerOpId,
      fromPortId: { id: 0, internal: false },
      toOpId: m.innerOpId,
      toPortId: { id: m.innerPortIdx, internal: false },
    }));

    const outputMarkerLinks: MacroBodyLink[] = outputMarkers.map(m => ({
      fromOpId: m.innerOpId,
      fromPortId: { id: m.innerPortIdx, internal: false },
      toOpId: m.markerOpId,
      toPortId: { id: 0, internal: false },
    }));

    const portSpec: PortSpec = {
      inputs: inputMarkers.map(m => ({ index: m.portIndex })),
      outputs: outputMarkers.map(m => ({ index: m.portIndex })),
    };

    const body: MacroBody = {
      operators: [...innerOps, ...markerOps],
      links: [...internalLinks, ...inputMarkerLinks, ...outputMarkerLinks],
      inputs: portSpec.inputs,
      outputs: portSpec.outputs,
    };

    return {
      name,
      content: JSON.stringify(body),
      portSpec,
    };
  }

  private uuid(): string {
    // Lightweight RFC4122-ish ID; the actions service uses crypto.randomUUID
    // elsewhere but we don't have it imported here and don't need strict
    // collision resistance for marker ops within a single macro body.
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, c => {
      const r = (Math.random() * 16) | 0;
      const v = c === "x" ? r : (r & 0x3) | 0x8;
      return v.toString(16);
    });
  }
}

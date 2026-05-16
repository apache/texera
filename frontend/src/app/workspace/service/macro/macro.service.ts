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
import { ExecutionMode, Workflow, WorkflowContent } from "../../../common/type/workflow";
import {
  OperatorLink,
  OperatorPredicate,
  PortDescription,
  Point,
} from "../../types/workflow-common.interface";
import { PortIdentity } from "../../types/execute-workflow.interface";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { v4 as uuid } from "uuid";

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
   * on the parent canvas, plus the boundary info the caller needs to swap the
   * selection out for a single MacroOp node on the canvas.
   *
   * Boundary handling: for every link crossing the selection edge we add a
   * `MacroInput` / `MacroOutput` marker inside the body (one per unique inner
   * port) and rewire it so MacroExpander can splice the body back into a
   * parent at compile time. Internal links (both endpoints inside the
   * selection) are passed through with port-ordinal IDs to match the
   * backend's PortIdentity shape.
   *
   * The returned `incomingEdges` / `outgoingEdges` describe each external link
   * that needs to be re-pointed at the new MacroOp instance (one entry per
   * link, where multiple external feeders can share the same `macroPortIndex`).
   */
  public buildMacroFromSelection(
    workflowActionService: WorkflowActionService,
    selectedOperatorIDs: readonly string[],
    name: string
  ): {
    request: MacroCreateRequest;
    incomingEdges: { externalOpId: string; externalPortID: string; macroPortIndex: number }[];
    outgoingEdges: { externalOpId: string; externalPortID: string; macroPortIndex: number }[];
    inputPortCount: number;
    outputPortCount: number;
  } {
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

    // Preserve the sub-DAG's full external interface, not just the ports that
    // happen to be wired up at macro-creation time. Replacing a sub-DAG with a
    // macro op is a dataflow-equivalence transformation: every input port on
    // the selection that isn't fed by another selected op is a boundary input
    // (regardless of whether an external feeder is currently connected), and
    // symmetrically for output ports. That way a selection of
    // Filter → Projection where Projection's output is currently unwired still
    // surfaces that output as an external macro port the user can connect later.
    const internallyFedInputPorts = new Set(internal.map(l => `${l.dstOp}|${l.dstPort}`));
    const internallyConsumedOutputPorts = new Set(internal.map(l => `${l.srcOp}|${l.srcPort}`));

    type BoundaryPort = { innerOpId: string; innerPortID: string; innerPortIdx: number };
    const boundaryInputPorts: BoundaryPort[] = [];
    const boundaryOutputPorts: BoundaryPort[] = [];
    selectedOperatorIDs.forEach(opId => {
      const op = graph.getOperator(opId);
      op.inputPorts.forEach((port, idx) => {
        if (!internallyFedInputPorts.has(`${opId}|${port.portID}`)) {
          boundaryInputPorts.push({ innerOpId: opId, innerPortID: port.portID, innerPortIdx: idx });
        }
      });
      op.outputPorts.forEach((port, idx) => {
        if (!internallyConsumedOutputPorts.has(`${opId}|${port.portID}`)) {
          boundaryOutputPorts.push({ innerOpId: opId, innerPortID: port.portID, innerPortIdx: idx });
        }
      });
    });

    // Allocate one MacroInput/MacroOutput marker per boundary port. Marker
    // ordering follows the selection's visual order (selectedOperatorIDs ×
    // op.inputPorts), giving the user a stable mapping between macro ports
    // and the underlying sub-DAG ports.
    const inputMarkers = boundaryInputPorts.map((p, idx) => ({
      markerOpId: `MacroInput-operator-${uuid()}`,
      portIndex: idx,
      innerOpId: p.innerOpId,
      innerPortID: p.innerPortID,
      innerPortIdx: p.innerPortIdx,
    }));
    const outputMarkers = boundaryOutputPorts.map((p, idx) => ({
      markerOpId: `MacroOutput-operator-${uuid()}`,
      portIndex: idx,
      innerOpId: p.innerOpId,
      innerPortID: p.innerPortID,
      innerPortIdx: p.innerPortIdx,
    }));

    // Marker ports follow the backend's `PortDescription` shape (portID string,
    // disallowMultiInputs/isDynamicPort flags) so MacroBody parses cleanly when
    // DbMacroRegistry deserializes `workflow.content`. The actual port wiring
    // is derived from `portIndex` server-side via `operatorInfo`; these entries
    // exist purely to keep Jackson happy.
    const markerOps: unknown[] = [
      ...inputMarkers.map(m => ({
        operatorID: m.markerOpId,
        operatorType: "MacroInput",
        operatorVersion: "",
        portIndex: m.portIndex,
        displayName: "",
        inputPorts: [],
        outputPorts: [
          { portID: "output-0", displayName: "", disallowMultiInputs: false, isDynamicPort: false },
        ],
      })),
      ...outputMarkers.map(m => ({
        operatorID: m.markerOpId,
        operatorType: "MacroOutput",
        operatorVersion: "",
        portIndex: m.portIndex,
        displayName: "",
        inputPorts: [
          {
            portID: "input-0",
            displayName: "",
            disallowMultiInputs: false,
            isDynamicPort: false,
            dependencies: [],
          },
        ],
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

    // Per-link rewire instructions. Several external links may share the same
    // macroPortIndex when they all target the same inner port.
    const inputIdxByInnerPort = new Map(
      inputMarkers.map(m => [`${m.innerOpId}|${m.innerPortID}`, m.portIndex])
    );
    const outputIdxByInnerPort = new Map(
      outputMarkers.map(m => [`${m.innerOpId}|${m.innerPortID}`, m.portIndex])
    );

    const incomingEdges = incoming.map(l => ({
      externalOpId: l.srcOp,
      externalPortID: l.srcPort,
      macroPortIndex: inputIdxByInnerPort.get(`${l.dstOp}|${l.dstPort}`) as number,
    }));
    const outgoingEdges = outgoing.map(l => ({
      externalOpId: l.dstOp,
      externalPortID: l.dstPort,
      macroPortIndex: outputIdxByInnerPort.get(`${l.srcOp}|${l.srcPort}`) as number,
    }));

    return {
      request: {
        name,
        content: JSON.stringify(body),
        portSpec,
      },
      incomingEdges,
      outgoingEdges,
      inputPortCount: inputMarkers.length,
      outputPortCount: outputMarkers.length,
    };
  }

  /**
   * Adapt a backend `MacroDetail` (whose `content` is a serialized `MacroBody`)
   * into a `Workflow`-shaped object the existing `reloadWorkflow` flow can
   * consume. Used by the drill-down editor route.
   *
   * v1 caveats:
   *  - operator positions are auto-laid-out (MacroInput on the left, regular
   *    inner ops in the middle, MacroOutput on the right) because the body
   *    doesn't carry positions yet.
   *  - inner ops that came from the canvas already have `PortDescription`
   *    ports; marker ops were authored with backend `PortIdentity` shape and
   *    are normalized here.
   */
  public macroDetailToWorkflow(detail: MacroDetail): Workflow {
    const body = JSON.parse(detail.content) as MacroBody;

    const operators = body.operators.map(raw => this.normalizeBodyOperator(raw));
    const operatorPositions = this.autoLayoutMacroBody(operators);
    const links = body.links
      .map(ml => this.macroLinkToOperatorLink(ml, operators))
      .filter((l): l is OperatorLink => l !== null);

    const content: WorkflowContent = {
      operators,
      operatorPositions,
      links,
      commentBoxes: [],
      settings: { dataTransferBatchSize: 400, executionMode: ExecutionMode.PIPELINED },
    };

    return {
      wid: detail.wid,
      name: detail.name,
      description: detail.description,
      creationTime: new Date(detail.creationTime).getTime(),
      lastModifiedTime: new Date(detail.lastModifiedTime).getTime(),
      isPublished: detail.isPublic ? 1 : 0,
      readonly: detail.readonly,
      content,
    };
  }

  private normalizeBodyOperator(raw: unknown): OperatorPredicate {
    const r = raw as Record<string, unknown>;
    const {
      operatorID,
      operatorType,
      operatorVersion,
      inputPorts,
      outputPorts,
      ...rest
    } = r as {
      operatorID: string;
      operatorType: string;
      operatorVersion?: string;
      inputPorts?: unknown[];
      outputPorts?: unknown[];
    } & Record<string, unknown>;

    return {
      operatorID,
      operatorType,
      operatorVersion: operatorVersion ?? "",
      operatorProperties: rest,
      inputPorts: this.normalizePortList(inputPorts ?? [], "input"),
      outputPorts: this.normalizePortList(outputPorts ?? [], "output"),
      showAdvanced: false,
      isDisabled: false,
      customDisplayName: typeof rest["displayName"] === "string" ? (rest["displayName"] as string) : undefined,
      dynamicInputPorts: false,
      dynamicOutputPorts: false,
    };
  }

  private normalizePortList(ports: unknown[], dir: "input" | "output"): PortDescription[] {
    return ports.map((raw, idx) => {
      const p = raw as Record<string, unknown>;
      // Already PortDescription-shaped (came from the canvas serialization).
      if (typeof p?.["portID"] === "string") {
        return p as unknown as PortDescription;
      }
      // Backend PortIdentity shape ({id: {id, internal}, displayName, ...}) —
      // synthesize a portID using the ordinal.
      const displayName = typeof p?.["displayName"] === "string" ? (p["displayName"] as string) : "";
      const base: PortDescription = {
        portID: `${dir}-${idx}`,
        displayName,
        disallowMultiInputs: false,
        isDynamicPort: false,
      };
      return dir === "input" ? { ...base, dependencies: [] } : base;
    });
  }

  private macroLinkToOperatorLink(
    ml: MacroBodyLink,
    operators: OperatorPredicate[]
  ): OperatorLink | null {
    const fromOp = operators.find(o => o.operatorID === ml.fromOpId);
    const toOp = operators.find(o => o.operatorID === ml.toOpId);
    if (!fromOp || !toOp) return null;
    const fromPortID = fromOp.outputPorts[ml.fromPortId.id]?.portID;
    const toPortID = toOp.inputPorts[ml.toPortId.id]?.portID;
    if (!fromPortID || !toPortID) return null;
    return {
      linkID: `macro-link-${uuid()}`,
      source: { operatorID: ml.fromOpId, portID: fromPortID },
      target: { operatorID: ml.toOpId, portID: toPortID },
    };
  }

  /**
   * Place MacroInput markers on the left, MacroOutput markers on the right,
   * and everything else in a middle column. Sufficient for visual
   * inspection; a proper layout pass is a follow-up.
   */
  private autoLayoutMacroBody(operators: OperatorPredicate[]): { [id: string]: Point } {
    const xLeft = 100;
    const xMiddle = 450;
    const xRight = 800;
    const ySpacing = 120;
    const ySeen = { left: 0, middle: 0, right: 0 };
    const positions: { [id: string]: Point } = {};
    operators.forEach(op => {
      let column: keyof typeof ySeen;
      let x: number;
      if (op.operatorType === "MacroInput") {
        column = "left";
        x = xLeft;
      } else if (op.operatorType === "MacroOutput") {
        column = "right";
        x = xRight;
      } else {
        column = "middle";
        x = xMiddle;
      }
      const y = 100 + ySeen[column] * ySpacing;
      ySeen[column] += 1;
      positions[op.operatorID] = { x, y };
    });
    return positions;
  }
}

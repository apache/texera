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
import { WorkflowUtilService } from "../../../../workspace/service/workflow-graph/util/workflow-util.service";
import { OperatorPredicate, PortDescription } from "../../../../workspace/types/workflow-common.interface";
import { CustomOperator, CustomOperatorProperty } from "../../../type/custom-operator.interface";

const PYTHON_UDF_TYPE = "PythonUDFV2";

/**
 * Builds an OperatorPredicate that wraps a Python UDF with the saved
 * code, ports, and property defaults of a CustomOperator. The graph still
 * stores an ordinary PythonUDFV2 — this factory only pre-fills its fields.
 */
@Injectable({
  providedIn: "root",
})
export class CustomOperatorFactoryService {
  constructor(private workflowUtilService: WorkflowUtilService) {}

  public buildPredicate(customOp: CustomOperator): OperatorPredicate | null {
    let basePredicate: OperatorPredicate;
    try {
      basePredicate = this.workflowUtilService.getNewOperatorPredicate(PYTHON_UDF_TYPE, customOp.name);
    } catch {
      return null;
    }

    const inputPorts = this.buildPorts(
      customOp.inputPorts.map(p => p.name),
      "input"
    );
    const outputPorts = this.buildPorts(
      customOp.outputPorts.map(p => p.name),
      "output"
    );

    const code = this.composeCode(customOp);

    return {
      ...basePredicate,
      customDisplayName: customOp.name || basePredicate.customDisplayName,
      inputPorts,
      outputPorts,
      operatorProperties: {
        ...basePredicate.operatorProperties,
        code,
        workers: 1,
        retainInputColumns: true,
      },
    };
  }

  private buildPorts(names: string[], prefix: "input" | "output"): PortDescription[] {
    if (names.length === 0) return [];
    return names.map((name, i) => ({
      portID: `${prefix}-${i}`,
      displayName: name || `${prefix}-${i}`,
      disallowMultiInputs: false,
      isDynamicPort: false,
      dependencies: [],
    }));
  }

  /**
   * Prepend a PROPS dict with the saved property defaults so the user's
   * code can read them via `PROPS["property_name"]`. We can't extend the
   * Python UDF schema without touching the engine, so this is the simplest
   * way to surface custom properties.
   */
  private composeCode(customOp: CustomOperator): string {
    const code = customOp.code ?? "";
    if (customOp.properties.length === 0) return code;

    const propsBlock = this.renderPropsBlock(customOp.properties);
    return `${propsBlock}\n${code}`;
  }

  private renderPropsBlock(properties: CustomOperatorProperty[]): string {
    const lines: string[] = [];
    lines.push("# Properties configured in 'My Operators' — edit values as needed.");
    lines.push("PROPS = {");
    for (const prop of properties) {
      const value = this.toPythonLiteral(prop.default, prop.type);
      const desc = prop.description ? `  # ${prop.description}` : "";
      lines.push(`    ${this.quoteKey(prop.name)}: ${value},${desc}`);
    }
    lines.push("}");
    return lines.join("\n");
  }

  private quoteKey(name: string): string {
    return JSON.stringify(name);
  }

  private toPythonLiteral(value: string | number | boolean, type: string): string {
    if (type === "boolean") return value ? "True" : "False";
    if (type === "number") return String(Number(value) || 0);
    return JSON.stringify(String(value ?? ""));
  }
}

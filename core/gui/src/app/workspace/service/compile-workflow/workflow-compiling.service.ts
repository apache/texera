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

import { HttpClient, HttpHeaders } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { EMPTY, merge, Observable, ReplaySubject } from "rxjs";
import { CustomJSONSchema7 } from "src/app/workspace/types/custom-json-schema.interface";
import { AppSettings } from "../../../common/app-setting";
import { areOperatorSchemasEqual, OperatorSchema } from "../../types/operator-schema.interface";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { catchError, debounceTime, mergeMap } from "rxjs/operators";
import { DynamicSchemaService } from "../dynamic-schema/dynamic-schema.service";
import {
  AttributeType,
  CompilationState,
  CompilationStateInfo,
  OperatorPortSchemaMap,
  PortSchema,
  WorkflowCompilationResponse,
} from "../../types/workflow-compiling.interface";
import { WorkflowFatalError } from "../../types/workflow-websocket.interface";
import { LogicalPlan } from "../../types/execute-workflow.interface";
import { ValidationWorkflowService } from "../validation/validation-workflow.service";
import { WorkflowGraphReadonly } from "../workflow-graph/model/workflow-graph";
import { serializePortIdentity } from "../../../common/util/port-identity-serde";
import { areAllPortSchemasEqual, addCompilationError } from "../../../common/util/workflow-compilation-utils";
import { parseLogicalOperatorPortID } from "../../../common/util/logical-operator-port-serde";

// endpoint for workflow compile
export const WORKFLOW_COMPILATION_ENDPOINT = "compile";

export const WORKFLOW_COMPILATION_DEBOUNCE_TIME_MS = 500;

/**
 * Workflow Compiling Service provides mainly 3 functionalities:
 * 1. autocomplete attribute property of operators (previously done by the SchemaPropagationService)
 * 2. receive static errors (previously done by sending EditingTimeCompilationRequest and saving in the ExecutionStateInfo)
 * 3. manage PhysicalPlan (TODO: send the physical plan to the standalone WorkflowExecutingService once we have it)
 *
 * When user creates and connects operators in workflow, the WorkflowCompilingService's api will be triggered, which,
 * propagate the schemas, compiles the user's workflow to get the physical plan and static errors(if any).
 *
 * Specifically for schema autocomplete, by contract, property name `attribute` and `attributes` indicate the field is a column of the operator's input,
 *  and schema propagation can provide autocomplete for the column names.
 */
@Injectable({
  providedIn: "root",
})
export class WorkflowCompilingService {
  private currentCompilationStateInfo: CompilationStateInfo = {
    state: CompilationState.Uninitialized,
  };
  private compilationStateInfoChangedStream = new ReplaySubject<CompilationState>(1);

  constructor(
    private httpClient: HttpClient,
    private workflowActionService: WorkflowActionService,
    private dynamicSchemaService: DynamicSchemaService,
    private validationWorkflowService: ValidationWorkflowService
  ) {
    // Subscribe to compilation state changes to apply schema propagation
    this.compilationStateInfoChangedStream.subscribe(() => {
      this.applySchemaPropagationResult();
    });

    // invoke the compilation service when there are any changes on workflow topology and properties. This includes:
    // - operator add, delete, property changed, disabled
    // - link add, delete
    merge(
      this.workflowActionService.getTexeraGraph().getLinkAddStream(),
      this.workflowActionService.getTexeraGraph().getLinkDeleteStream(),
      this.workflowActionService.getTexeraGraph().getOperatorAddStream(),
      this.workflowActionService.getTexeraGraph().getOperatorDeleteStream(),
      this.workflowActionService.getTexeraGraph().getOperatorPropertyChangeStream(),
      this.workflowActionService.getTexeraGraph().getDisabledOperatorsChangedStream()
    )
      .pipe(debounceTime(WORKFLOW_COMPILATION_DEBOUNCE_TIME_MS))
      .pipe(
        mergeMap(() => {
          const logicalPlan = ExecuteWorkflowService.getLogicalPlanRequest(
            this.validationWorkflowService.getValidTexeraGraph(),
            undefined
          );
          return this.compile(logicalPlan);
        })
      )
      .subscribe(response => {
        if (response.physicalPlan) {
          this.currentCompilationStateInfo = {
            state: CompilationState.Succeeded,
            physicalPlan: response.physicalPlan,
            operatorOutputPortSchemaMap: response.operatorOutputSchemas,
          };
        } else {
          this.currentCompilationStateInfo = {
            state: CompilationState.Failed,
            operatorOutputPortSchemaMap: response.operatorOutputSchemas,
            operatorErrors: response.operatorErrors,
          };
        }
        this.compilationStateInfoChangedStream.next(this.currentCompilationStateInfo.state);
      });
  }

  public getWorkflowCompilationState(): CompilationState {
    return this.currentCompilationStateInfo.state;
  }

  public getWorkflowCompilationErrors(): Readonly<Record<string, WorkflowFatalError>> {
    if (
      this.currentCompilationStateInfo.state === CompilationState.Succeeded ||
      this.currentCompilationStateInfo.state === CompilationState.Uninitialized
    ) {
      return {};
    }
    return this.currentCompilationStateInfo.operatorErrors;
  }

  public getOperatorInputSchemaMap(operatorID: string): OperatorPortSchemaMap | undefined {
    if (
      this.currentCompilationStateInfo.state == CompilationState.Uninitialized ||
      !this.currentCompilationStateInfo.operatorOutputPortSchemaMap
    ) {
      return undefined;
    }

    return this.extractOperatorInputPortSchemaMap(
      operatorID,
      this.currentCompilationStateInfo.operatorOutputPortSchemaMap,
      this.workflowActionService.getTexeraGraph()
    );
  }

  public getPortInputSchema(operatorID: string, portIndex: number): PortSchema | undefined {
    return this.getOperatorInputSchemaMap(operatorID)?.[serializePortIdentity({ id: portIndex, internal: false })];
  }

  public getOperatorInputAttributeType(
    operatorID: string,
    portIndex: number,
    attributeName: string
  ): AttributeType | undefined {
    return this.getPortInputSchema(operatorID, portIndex)?.find(e => e.attributeName === attributeName)?.attributeType;
  }

  /**
   * Apply the schema propagation result to an operator.
   * The schema propagation result contains the input attributes of operators.
   *
   * If an operator is not in the result, then:
   * 1. the operator's input attributes cannot be inferred. In this case, the operator dynamic schema is unchanged.
   * 2. the operator is a source operator. In this case, we need to fill in the attributes using the selected table.
   */
  private applySchemaPropagationResult(): void {
    // for each operator, try to apply schema propagation result
    Array.from(this.dynamicSchemaService.getDynamicSchemaMap().keys()).forEach(operatorID => {
      const currentDynamicSchema = this.dynamicSchemaService.getDynamicSchema(operatorID);

      // Get the input schema for this operator using the centralized method
      const inputSchema = this.getOperatorInputSchemaMap(operatorID);

      let newDynamicSchema: OperatorSchema;
      if (inputSchema) {
        newDynamicSchema = WorkflowCompilingService.setOperatorInputAttrs(currentDynamicSchema, inputSchema);
      } else {
        // otherwise, the input attributes of the operator is unknown
        // if the operator is not a source operator, restore its original schema of input attributes
        if (currentDynamicSchema.additionalMetadata.inputPorts.length > 0) {
          newDynamicSchema = WorkflowCompilingService.restoreOperatorInputAttrs(currentDynamicSchema);
        } else {
          newDynamicSchema = currentDynamicSchema;
        }
      }

      if (!areOperatorSchemasEqual(currentDynamicSchema, newDynamicSchema)) {
        this.dynamicSchemaService.setDynamicSchema(operatorID, newDynamicSchema);
      }
    });
  }

  /**
   * Extracts input schema per port for an operator by looking at the output schemas of operators that are connecting to it.
   *
   * @param operatorID The target operator's ID
   * @param outputSchemas Map of operator IDs to their output schemas per output port
   * @param workflowGraph to get input links from
   * @returns The extracted input schema per port or undefined
   */
  private extractOperatorInputPortSchemaMap(
    operatorID: string,
    outputSchemas: Record<string, OperatorPortSchemaMap>,
    workflowGraph: WorkflowGraphReadonly
  ): OperatorPortSchemaMap | undefined {
    const inputLinks = workflowGraph.getInputLinksByOperatorId(operatorID);
    if (!inputLinks.length) return undefined;

    // Get the operator's dynamic schema to know what input ports it has
    const dynamicSchema = this.dynamicSchemaService.getDynamicSchema(operatorID);
    if (!dynamicSchema) return undefined;

    const inputPortSchemaMap = new Map<string, PortSchema | undefined>();

    dynamicSchema.additionalMetadata.inputPorts.forEach((inputPort, portIndex) => {
      const portId = serializePortIdentity({ id: portIndex, internal: false });
      inputPortSchemaMap.set(portId, undefined);

      // Find all links that connect to this input port
      const linksToThisPort = inputLinks.filter(link => {
        const inputPort = parseLogicalOperatorPortID(link.target.portID);
        if (!inputPort) return false;
        return inputPort.portNumber === portIndex;
      });

      if (linksToThisPort.length > 0) {
        // Check if multiple links have different schemas
        const schemas: (PortSchema | undefined)[] = linksToThisPort.map(link => {
          const sourcePortSchemaMap = outputSchemas[link.source.operatorID];
          if (!sourcePortSchemaMap) {
            return undefined;
          }

          const outputPort = parseLogicalOperatorPortID(link.source.portID);
          if (!outputPort) {
            return undefined;
          }

          return sourcePortSchemaMap[serializePortIdentity({ id: outputPort.portNumber, internal: false })];
        });

        // Check if all schemas are the same using utility function
        if (schemas.length > 1 && !areAllPortSchemasEqual(schemas)) {
          // Set compilation state to failed and add error using utility function
          this.currentCompilationStateInfo = addCompilationError(
            this.currentCompilationStateInfo,
            operatorID,
            `Multiple links with different schemas connected to the same input port ${portIndex}`,
            `Port ${portIndex} received ${schemas.length} different schemas (some may be undefined)`
          );
          return undefined;
        }

        // All port schemas of this input port has been checked to be the same, use the first schema to set
        if (schemas.length > 0) {
          inputPortSchemaMap.set(portId, schemas[0]);
        }
      }
    });

    if (!inputPortSchemaMap.size) return undefined;
    return Object.fromEntries(inputPortSchemaMap);
  }

  /**
   * Used for automated propagation of input schema in workflow.
   *
   * When users are in the process of building a workflow, Texera can propagate schema forwards so
   * that users can easily set the properties of the next operator. For eg: If there are two operators Source:Scan and KeywordSearch and
   * a link is created between them, the attributed of the table selected in Source can be propagated to the KeywordSearch operator.
   */
  private compile(logicalPlan: LogicalPlan): Observable<WorkflowCompilationResponse> {
    // create a Logical Plan based on the workflow graph
    // remove unnecessary information for schema propagation.
    const body = {
      operators: logicalPlan.operators,
      links: logicalPlan.links,
      opsToReuseResult: [],
      opsToViewResult: [],
    };
    // make a http post request to the API endpoint with the logical plan object
    return this.httpClient
      .post<WorkflowCompilationResponse>(
        `${AppSettings.getApiEndpoint()}/${WORKFLOW_COMPILATION_ENDPOINT}`,
        JSON.stringify(body),
        {
          headers: new HttpHeaders({
            "Content-Type": "application/json",
          }),
        }
      )
      .pipe(
        catchError((err: unknown) => {
          console.warn("compile workflow API returns error", err);
          return EMPTY;
        })
      );
  }

  public static setOperatorInputAttrs(
    operatorSchema: OperatorSchema,
    inputPortSchemaMap: OperatorPortSchemaMap | undefined
  ): OperatorSchema {
    // If the inputSchema is empty, just return the original operator metadata.
    if (!inputPortSchemaMap || Object.keys(inputPortSchemaMap).length === 0) {
      return operatorSchema;
    }

    let newJsonSchema = operatorSchema.jsonSchema;

    const getAttrNames = (attrName: string, v: CustomJSONSchema7): string[] | undefined => {
      const i = v.autofillAttributeOnPort;
      if (i === undefined || i === null || !Number.isInteger(i)) {
        return undefined;
      }

      // Use serializePortIdentity to get the correct key for the input port
      const portId = serializePortIdentity({ id: i, internal: false });
      const inputAttrAtPort = inputPortSchemaMap[portId];
      if (!inputAttrAtPort) {
        return undefined;
      }
      const attrNames: string[] = inputAttrAtPort.map(attr => attr.attributeName);
      if (v.additionalEnumValue) {
        attrNames.push(v.additionalEnumValue);
      }

      // ajv does not support null values, so it converts all the nulls to empty strings.
      // https://github.com/ajv-validator/ajv/issues/1471
      // the null -> "" change is done by Ajv.validate() with useDefault set to true.
      // It is converted during the property editor form initialization and workflow validation, instead of during schema propagation.
      if (!operatorSchema.jsonSchema.required?.includes(attrName)) {
        if (v.default) {
          if (typeof v.default !== "string") {
            throw new Error("default value must be a string");
          }
          // We are adding the default value or "" into
          // the enum list to pass the frontend check for optional properties.
          attrNames.push(v.default);
        } else {
          attrNames.push("");
        }
      }
      return attrNames;
    };

    newJsonSchema = DynamicSchemaService.mutateProperty(
      newJsonSchema,
      (k, v) => v.autofill === "attributeName",
      (attrName, old) => ({
        ...old,
        type: "string",
        enum: getAttrNames(attrName, old),
        uniqueItems: true,
      })
    );

    newJsonSchema = DynamicSchemaService.mutateProperty(
      newJsonSchema,
      (k, v) => v.autofill === "attributeNameList",
      (attrName, old) => ({
        ...old,
        type: "array",
        uniqueItems: true,
        items: {
          ...(old.items as CustomJSONSchema7),
          type: "string",
          enum: getAttrNames(attrName, old),
        },
      })
    );

    return {
      ...operatorSchema,
      jsonSchema: newJsonSchema,
    };
  }

  public static restoreOperatorInputAttrs(operatorSchema: OperatorSchema): OperatorSchema {
    let newJsonSchema = operatorSchema.jsonSchema;

    newJsonSchema = DynamicSchemaService.mutateProperty(
      newJsonSchema,
      (k, v) => v.autofill === "attributeName",
      (attrName, old) => ({
        ...old,
        type: "string",
        enum: undefined,
        uniqueItems: undefined,
      })
    );

    newJsonSchema = DynamicSchemaService.mutateProperty(
      newJsonSchema,
      (k, v) => v.autofill === "attributeNameList",
      (attrName, old) => ({
        ...old,
        type: "array",
        uniqueItems: undefined,
        items: {
          ...(old.items as CustomJSONSchema7),
          type: "string",
          enum: undefined,
        },
      })
    );

    return {
      ...operatorSchema,
      jsonSchema: newJsonSchema,
    };
  }

  public getCompilationStateInfoChangedStream(): Observable<CompilationState> {
    return this.compilationStateInfoChangedStream.asObservable();
  }
}

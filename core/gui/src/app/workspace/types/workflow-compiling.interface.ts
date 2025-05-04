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

import { PhysicalPlan } from "../../common/type/physical-plan";
import { WorkflowFatalError } from "./workflow-websocket.interface";

/**
 * The backend interface of the return object of a successful/failed workflow compilation
 *
 * An example data format for AutocompleteSuccessResult will look like:
 * {
 *  physicalPlan: Physical Plan | Null(if compilation failed),
 *  operatorInputSchemas: {
 *    'operatorID1' : [ ['attribute1','attribute2','attribute3'] ],
 *    'operatorID2' : [ [ {attributeName: 'name', attributeType: 'string'},
 *                      {attributeName: 'text', attributeType: 'string'},
 *                      {attributeName: 'follower_count', attributeType: 'string'} ] ]
 *
 *  }
 * }
 */
export interface WorkflowCompilationResponse
  extends Readonly<{
    physicalPlan?: PhysicalPlan;
    operatorInputSchemas: {
      [key: string]: OperatorInputSchema;
    };
    operatorErrors: {
      [opId: string]: WorkflowFatalError;
    };
  }> {}

export enum CompilationState {
  Uninitialized = "Uninitialized",
  Succeeded = "Succeeded",
  Failed = "Failed",
}

export type CompilationStateInfo = Readonly<
  | {
      // indicates the compilation is successful
      state: CompilationState.Succeeded;
      // physicalPlan compiled from current logical plan
      physicalPlan: PhysicalPlan;
      // a map from opId to InputSchema, used for autocompletion of schema
      operatorInputSchemaMap: Readonly<Record<string, OperatorInputSchema>>;
    }
  | {
      state: CompilationState.Uninitialized;
    }
  | {
      state: CompilationState.Failed;
      operatorInputSchemaMap: Readonly<Record<string, OperatorInputSchema>>;
      operatorErrors: Readonly<Record<string, WorkflowFatalError>>;
    }
>;
// possible types of an attribute
export type AttributeType = "string" | "integer" | "double" | "boolean" | "long" | "timestamp" | "binary"; // schema: an array of attribute names and types
export interface SchemaAttribute
  extends Readonly<{
    attributeName: string;
    attributeType: AttributeType;
  }> {}

// input schema of an operator: an array of schemas at each input port
export type OperatorInputSchema = ReadonlyArray<PortInputSchema | undefined>;
export type PortInputSchema = ReadonlyArray<SchemaAttribute>;

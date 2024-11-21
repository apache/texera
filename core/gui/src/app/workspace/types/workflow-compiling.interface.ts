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

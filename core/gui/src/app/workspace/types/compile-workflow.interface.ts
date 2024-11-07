import { PhysicalPlan } from "../../common/type/physical-plan";
import { OperatorInputSchema } from "../service/compile-workflow/compile-workflow.service";
import { WorkflowFatalError } from "./workflow-websocket.interface";

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
  operatorErrors: Readonly<Array<WorkflowFatalError>>;
}
>;

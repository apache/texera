import {
  ExecutionIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity,
} from "./proto/edu/uci/ics/amber/engine/common/virtualidentity";
import { PhysicalLink } from "./proto/edu/uci/ics/amber/engine/common/workflow";

export interface PhysicalOp {
  id: PhysicalOpIdentity;
  workflowId: WorkflowIdentity;
  executionId: ExecutionIdentity;
  parallelizable: boolean;
  isOneToManyOp: boolean;
  suggestedWorkerNum: number | null;
  sourceOperator: boolean;
  sinkOperator: boolean;
}

export interface PhysicalPlan {
  operators: Set<PhysicalOp>;
  links: Set<PhysicalLink>;
}

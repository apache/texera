import { Workflow } from "../../common/type/workflow";

export function checkIfWorkflowBroken(workflow: Workflow): boolean {
  // Build a Set of all valid operatorIDs
  const validOperatorIDs = new Set(workflow.content.operators.map(o => o.operatorID));

  // Check if any link references non-existent operators
  return workflow.content.links.some(
    link => !validOperatorIDs.has(link.source.operatorID) || !validOperatorIDs.has(link.target.operatorID)
  );
}

import { Workflow} from "../../common/type/workflow";

export function removeInvalidLinks(workflow: Workflow): void {
  // Build a Set of all valid operatorIDs
  const validOperatorIDs = new Set(workflow.content.operators.map(o => o.operatorID));

  // Find the indices of links to remove
  const badIndices: number[] = [];

  workflow.content.links.forEach((link, i) => {
    if (
      !validOperatorIDs.has(link.source.operatorID) ||
      !validOperatorIDs.has(link.target.operatorID)
    ) {
      badIndices.push(i);
    }
  });

  const broken = badIndices.length > 0;

  // Remove them in reverse order
  badIndices
    .sort((a, b) => b - a)
    .forEach(i => workflow.content.links.splice(i, 1));

  if (broken) {
    console.log("bad content", workflow);
  } else {
    console.log("no bad content");
  }
}
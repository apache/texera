import { WorkflowMetadata } from "../../dashboard/user/type/workflow-metadata.interface";
import { PlainGroup } from "../../workspace/service/workflow-graph/model/operator-group";
import { OperatorLink, OperatorPredicate, Point, CommentBox } from "../../workspace/types/workflow-common.interface";

/**
 * WorkflowContent is used to store the information of the workflow
 *  1. all existing operators and their properties
 *  2. operator's position on the JointJS paper
 *  3. operator link predicates
 *  4. operator groups
 *
 * When the user refreshes the browser, the CachedWorkflow interface will be
 *  automatically cached and loaded once the refresh completes. This information
 *  will then be used to reload the entire workflow.
 *
 */
export interface WorkflowContent
  extends Readonly<{
    operators: OperatorPredicate[];
    operatorPositions: { [key: string]: Point };
    links: OperatorLink[];
    groups: PlainGroup[];
    commentBoxes: CommentBox[];
  }> {}

export type Workflow = { content: WorkflowContent } & WorkflowMetadata;

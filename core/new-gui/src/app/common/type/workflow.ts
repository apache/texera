import { Breakpoint, OperatorLink, OperatorPredicate, Point } from '../../workspace/types/workflow-common.interface';
import { jsonCast } from '../util/storage';

/**
 * CachedWorkflow is used to store the information of the workflow
 *  1. all existing operators and their properties
 *  2. operator's position on the JointJS paper
 *  3. operator link predicates
 *
 * When the user refreshes the browser, the CachedWorkflow interface will be
 *  automatically cached and loaded once the refresh completes. This information
 *  will then be used to reload the entire workflow.
 *
 */
export interface WorkflowInfo {
  operators: OperatorPredicate[];
  operatorPositions: { [key: string]: Point };
  links: OperatorLink[];
  breakpoints: Record<string, Breakpoint>;
}

export interface Workflow {
  name: string;
  wid: number | undefined;
  content: WorkflowInfo;
  creationTime: number;
  lastModifiedTime: number;
}

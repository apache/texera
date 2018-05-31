import { WorkflowGraph } from './../workflow-graph/model/workflow-graph';
import { mockScanPredicate, mockResultPredicate, mockScanResultLink } from './../workflow-graph/model/mock-workflow-data';


// TODO: unify the port handling interface
export const MOCK_WORKFLOW_PLAN: WorkflowGraph = new WorkflowGraph(
    [
        mockScanPredicate,
        mockResultPredicate
    ],
    [
        mockScanResultLink
    ]
);

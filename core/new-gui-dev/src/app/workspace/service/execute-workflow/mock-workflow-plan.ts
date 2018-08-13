import { WorkflowGraph } from '../../model/workflow-graph';

// TODO: unify the port handling interface
export const MOCK_WORKFLOW_PLAN: WorkflowGraph = new WorkflowGraph(
    [
        {
            operatorID: 'operator-1',
            operatorType: 'ScanSource',
            operatorProperties: { 'tableName': 'twitter_sample' },
            inputPorts: [],
            outputPorts: ['output-1']
        },
        {
            'operatorID': 'operator-2',
            'operatorType': 'ViewResults',
            'operatorProperties': { 'limit': 10, 'offset': 0 },
            inputPorts: ['input-1'],
            outputPorts: []
        }
    ],
    [
        {
            linkID: 'link-1',
            sourceOperator: 'operator-1',
            sourcePort: 'output-1',
            targetOperator: 'operator-2',
            targetPort: 'input-1'
        }
    ]
);

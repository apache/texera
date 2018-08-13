import { WorkflowJointGraphService } from '../../service/workflow-graph/model/workflow-joint-graph.service';

import * as dagre from 'dagre';
import { WorkflowTexeraGraphService } from '../../service/workflow-graph/model/workflow-texera-graph.service';
import { DEFAULT_OPERATOR_WIDTH, DEFAULT_OPERATOR_HEIGHT } from '../../service/operator-ui-element/operator-ui-element.service';

export class SmartOperatorLocation {

    // private g = new dagre.graphlib.Graph();

    constructor(
        private workflowTexeraGraphService: WorkflowTexeraGraphService,
        private workflowJointGraphService: WorkflowJointGraphService,
    ) { }

    public suggestNextLocation(operatorType: string): {x: number, y: number} {
        const g = new dagre.graphlib.Graph();
        g.setGraph({});

        const workflowGraph = this.workflowTexeraGraphService.texeraWorkflowGraph;
        const jointGraph = this.workflowJointGraphService.uiGraph;
        const operators = workflowGraph.getOperators();
        const links = workflowGraph.getLinks().filter(link => link.sourceOperator && link.targetOperator);


        for (let i = 0; i < operators.length; i++) {
            console.log(operators[i].operatorID);
            console.log({
                width: DEFAULT_OPERATOR_WIDTH, height: DEFAULT_OPERATOR_HEIGHT,
                x: (<joint.dia.Element>jointGraph.getCell(operators[i].operatorID)).get('position').x,
                y: (<joint.dia.Element>jointGraph.getCell(operators[i].operatorID)).get('position').y
            });
            g.setNode(operators[i].operatorID, {
                width: DEFAULT_OPERATOR_WIDTH, height: DEFAULT_OPERATOR_HEIGHT,
                x: (<joint.dia.Element>jointGraph.getCell(operators[i].operatorID)).get('position').x,
                y: (<joint.dia.Element>jointGraph.getCell(operators[i].operatorID)).get('position').y
            });
        }

        for (let i = 0; i < links.length; i++) {
            console.log(links[i]);
            g.setEdge(links[i].sourceOperator, links[i].targetOperator);
        }

        const tempOperator = 'tempOperatorID';
        g.setNode(tempOperator, {
            width: DEFAULT_OPERATOR_WIDTH, height: DEFAULT_OPERATOR_HEIGHT
        });

        dagre.layout(g);

        const suggestedX = g.node(tempOperator).x + this.workflowJointGraphService.uiPaper.pageOffset().x;
        const suggestedY = g.node(tempOperator).y + this.workflowJointGraphService.uiPaper.pageOffset().y;

        console.log(suggestedX);
        console.log(suggestedY);

        return {
            x: suggestedX,
            y: suggestedY
        };

    }



}

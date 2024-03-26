// import { mockResultPredicate, mockPoint } from './../workflow-graph/model/mock-workflow-data';
// import { TestBed, inject } from '@angular/core/testing';
// import * as joint from 'jointjs';

// import { JointUIService, deleteButtonPath, sourceOperatorHandle, targetOperatorHandle } from './joint-ui.service';
// import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
// import { StubOperatorMetadataService } from '../operator-metadata/stub-operator-metadata.service';
// import { mockScanPredicate, mockSentimentPredicate } from '../workflow-graph/model/mock-workflow-data';
// import { mockScanStatistic1, mockScanStatistic2 } from '../workflow-status/mock-workflow-status';

// describe('JointUIService', () => {
//   let service: JointUIService;

//   beforeEach(() => {
//     TestBed.configureTestingModule({
//       providers: [
//         JointUIService,
//         { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
//       ],
//     });
//     service = TestBed.get(JointUIService);
//   });

//   it('should be created', inject([JointUIService], (injectedService: JointUIService) => {
//     expect(injectedService).toBeTruthy();
//   }));

//   /**
//    * Check if the getJointOperatorElement() can successfully creates a JointJS Element
//    */
//   it('should create an JointJS Element successfully when the function is called', () => {
//     const result = service.getJointOperatorElement(
//       mockScanPredicate, mockPoint);
//     expect(result).toBeTruthy();
//   });

//   /**
//    * Check if the error in getJointOperatorElement() is correctly thrown
//    */
//   it('should throw an error with an non existing operator', () => {
//     expect(() => {
//       service.getJointOperatorElement(
//         {
//           operatorID: 'nonexistOperator',
//           operatorType: 'nonexistOperatorType',
//           operatorProperties: {},
//           inputPorts: [],
//           outputPorts: [],
//           showAdvanced: true
//         },
//         mockPoint
//       );
//     }).toThrowError(new RegExp(`doesn't exist`));
//   });

//   /**
//    * Check if the getJointTooltipElement() can successfully creates a JointJS Element
//    */
//   it('should create an JointJS Element successfully when the function is called', () => {
//     const result = service.getJointOperatorStatusTooltipElement(
//       mockScanPredicate, mockPoint);
//     expect(result).toBeTruthy();
//   });

//   /**
//    * Check if the error in getJointTooltipElement() is correctly thrown
//    */
//   it('should throw an error with an non existing operator', () => {
//     expect(() => {
//       service.getJointOperatorStatusTooltipElement(
//         {
//           operatorID: 'nonexistOperator',
//           operatorType: 'nonexistOperatorType',
//           operatorProperties: {},
//           inputPorts: [],
//           outputPorts: [],
//           showAdvanced: true
//         },
//         mockPoint
//       );
//     }).toThrowError(new RegExp(`doesn't exist`));
//   });

//   /**
//    * Check if showTooltip/hideTooltip works properly
//    */
//   it('should reveal/hide tooltip and its content when showToolTip/hideTooltip is called', () => {
//     // creating a JointJS graph with one operator and its tooltip
//     const jointGraph = new joint.dia.Graph();
//     const jointPaperOptions: joint.dia.Paper.Options = {model: jointGraph};
//     const paper = new joint.dia.Paper(jointPaperOptions);

//     jointGraph.addCell([
//       service.getJointOperatorElement(
//         mockScanPredicate,
//         mockPoint
//       ),
//       service.getJointOperatorStatusTooltipElement(
//         mockScanPredicate,
//         mockPoint
//       )
//     ]);
//     // tooltip should not be shown when operator is just created
//     // disply attr should be none
//     const tooltipId = JointUIService.getOperatorStatusTooltipElementID(mockScanPredicate.operatorID);
//     const graph_tooltip1 = jointGraph.getCell(tooltipId);
//     expect(graph_tooltip1.attr('polygon')['display']).toEqual('none');
//     expect(graph_tooltip1.attr('#operatorCount')['display']).toEqual('none');
//     expect(graph_tooltip1.attr('#operatorSpeed')['display']).toEqual('none');
//     // showTooltip removes display == none attr to show tooltip
//     service.showOperatorStatusToolTip(paper, tooltipId);
//     expect(graph_tooltip1.attr('polygon')['display']).toBeUndefined();
//     expect(graph_tooltip1.attr('#operatorCount')['display']).toBeUndefined();
//     expect(graph_tooltip1.attr('#operatorSpeed')['display']).toBeUndefined();
//     // hideTooltip adds display == none attr to hide tooltip
//     service.hideOperatorStatusToolTip(paper, tooltipId);
//     expect(graph_tooltip1.attr('polygon')['display']).toEqual('none');
//     expect(graph_tooltip1.attr('#operatorCount')['display']).toEqual('none');
//     expect(graph_tooltip1.attr('#operatorSpeed')['display']).toEqual('none');
//   });

//   /**
//    * check if tooltip content can be updated properly
//    */
//   it('should update the content in the tooltip when changeOperatorTooltipInfo is called', () => {
//     // creating a JointJS graph with one operator and its tooltip
//     const jointGraph = new joint.dia.Graph();
//     const jointPaperOptions: joint.dia.Paper.Options = {model: jointGraph};
//     const paper = new joint.dia.Paper(jointPaperOptions);

//     jointGraph.addCell([
//       service.getJointOperatorElement(
//         mockScanPredicate,
//         mockPoint
//       ),
//       service.getJointOperatorStatusTooltipElement(
//         mockScanPredicate,
//         mockPoint
//       )
//     ]);
//     const tooltipId = JointUIService.getOperatorStatusTooltipElementID(mockScanPredicate.operatorID);
//     const graph_tooltip = jointGraph.getCell(tooltipId);
//     // tooltip should not contain any content when created
//     expect(graph_tooltip.attr('#operatorCount')['text']).toBeUndefined();
//     expect(graph_tooltip.attr('#operatorSpeed')['text']).toBeUndefined();
//     // updating it with mock statistics
//     service.changeOperatorStatusTooltipInfo(paper, tooltipId, mockScanStatistic1);
//     expect(graph_tooltip.attr('#operatorCount')['text']).toEqual('Output:' + mockScanStatistic1.outputCount + ' tuples');
//     expect(graph_tooltip.attr('#operatorSpeed')['text']).toEqual('Speed:' + mockScanStatistic1.speed + ' tuples/s');
//     // updating it with another mock statistics
//     service.changeOperatorStatusTooltipInfo(paper, tooltipId, mockScanStatistic2);
//     expect(graph_tooltip.attr('#operatorCount')['text']).toEqual('Output:' + mockScanStatistic2.outputCount + ' tuples');
//     expect(graph_tooltip.attr('#operatorSpeed')['text']).toEqual('Speed:' + mockScanStatistic2.speed + ' tuples/s');
//   });

//   it('should change the operator state name and color when changeOperatorStates is called', () => {
//     // creating a JointJS graph with one operator and its tooltip
//     const jointGraph = new joint.dia.Graph();
//     const jointPaperOptions: joint.dia.Paper.Options = {model: jointGraph};
//     const paper = new joint.dia.Paper(jointPaperOptions);

//     jointGraph.addCell(
//       service.getJointOperatorElement(
//         mockScanPredicate,
//         mockPoint
//     ));

//     // operator state name and color should be changed correctly
//     const graph_operator = jointGraph.getCell(mockScanPredicate.operatorID);
//     expect(graph_operator.attr('#operatorStates')['text']).toEqual('Ready');
//     expect(graph_operator.attr('#operatorStates')['fill']).toEqual('green');
//     service.changeOperatorStates(paper, mockScanPredicate.operatorID, OperatorStates.Initializing);
//     expect(graph_operator.attr('#operatorStates')['text']).toEqual('Initializing');
//     expect(graph_operator.attr('#operatorStates')['fill']).toEqual('orange');
//     service.changeOperatorStates(paper, mockScanPredicate.operatorID, OperatorStates.Running);
//     expect(graph_operator.attr('#operatorStates')['text']).toEqual('Running');
//     expect(graph_operator.attr('#operatorStates')['fill']).toEqual('orange');
//     service.changeOperatorStates(paper, mockScanPredicate.operatorID, OperatorStates.Pausing);
//     expect(graph_operator.attr('#operatorStates')['text']).toEqual('Pausing');
//     expect(graph_operator.attr('#operatorStates')['fill']).toEqual('red');
//     service.changeOperatorStates(paper, mockScanPredicate.operatorID, OperatorStates.Paused);
//     expect(graph_operator.attr('#operatorStates')['text']).toEqual('Paused');
//     expect(graph_operator.attr('#operatorStates')['fill']).toEqual('red');
//     service.changeOperatorStates(paper, mockScanPredicate.operatorID, OperatorStates.Completed);
//     expect(graph_operator.attr('#operatorStates')['text']).toEqual('Completed');
//     expect(graph_operator.attr('#operatorStates')['fill']).toEqual('green');
//   });

//   /**
//    * Check if the number of inPorts and outPorts created by getJointOperatorElement()
//    * matches the port number specified by the operator metadata
//    */
//   it('should create correct number of inPorts and outPorts based on operator metadata', () => {
//     const element1 = service.getJointOperatorElement(mockScanPredicate, mockPoint);
//     const element2 = service.getJointOperatorElement(mockSentimentPredicate, mockPoint);
//     const element3 = service.getJointOperatorElement(mockResultPredicate, mockPoint);

//     const inPortCount1 = element1.getPorts().filter(port => port.group === 'in').length;
//     const outPortCount1 = element1.getPorts().filter(port => port.group === 'out').length;
//     const inPortCount2 = element2.getPorts().filter(port => port.group === 'in').length;
//     const outPortCount2 = element2.getPorts().filter(port => port.group === 'out').length;
//     const inPortCount3 = element3.getPorts().filter(port => port.group === 'in').length;
//     const outPortCount3 = element3.getPorts().filter(port => port.group === 'out').length;

//     expect(inPortCount1).toEqual(0);
//     expect(outPortCount1).toEqual(1);
//     expect(inPortCount2).toEqual(1);
//     expect(outPortCount2).toEqual(1);
//     expect(inPortCount3).toEqual(1);
//     expect(outPortCount3).toEqual(0);

//   });

//   /**
//    * Check if the custom attributes / svgs are correctly used by the JointJS graph
//    */
//   it('should apply the custom SVG styling to the JointJS element', () => {

//     const graph = new joint.dia.Graph();
//     // operator and its tooltip element should be added together
//     graph.addCell([
//       service.getJointOperatorElement(
//         mockScanPredicate,
//         mockPoint
//       ),
//       service.getJointOperatorStatusTooltipElement(
//         mockScanPredicate,
//         mockPoint
//       )
//     ]);
//     graph.addCell([
//       service.getJointOperatorElement(
//         mockResultPredicate,
//         { x: 500, y: 100 }
//       ),
//       service.getJointOperatorStatusTooltipElement(
//         mockResultPredicate,
//         { x: 500, y: 100 }
//       )
//       ]);

//     const link = JointUIService.getJointLinkCell({
//       linkID: 'link-1',
//       source: { operatorID: 'operator1', portID: 'out0' },
//       target: { operatorID: 'operator2', portID: 'in0' },
//     });

//     graph.addCell(link);

//     const graph_operator1 = graph.getCell(mockScanPredicate.operatorID);
//     const graph_operator2 = graph.getCell(mockResultPredicate.operatorID);
//     const graph_link = graph.getLinks()[0];
//     const graph_tooltip1 = graph.getCell(JointUIService.getOperatorStatusTooltipElementID(mockScanPredicate.operatorID));

//     // testing getCustomTooltipStyleAttrs()
//     // style: {'pointer-events': 'none'} makes tooltip unselectable thus not draggable
//     expect(graph_tooltip1.attr('polygon')).toEqual({
//       fill: '#FFFFFF', 'follow-scale': true, stroke: 'purple', 'stroke-width': '2',
//         rx: '5px', ry: '5px', refPoints: '0,30 150,30 150,120 85,120 75,150 65,120 0,120',
//         display: 'none', style: {'pointer-events': 'none'}
//     });
//     expect(graph_tooltip1.attr('#operatorCount')).toEqual({
//       fill: '#595959', 'font-size': '12px', ref: 'polygon',
//       'y-alignment': 'middle',
//       'x-alignment': 'left',
//       'ref-x': .05, 'ref-y': .2,
//       display: 'none', style: {'pointer-events': 'none'}
//     });
//     expect(graph_tooltip1.attr('#operatorSpeed')).toEqual({
//       fill: '#595959',
//       ref: 'polygon',
//       'x-alignment': 'left',
//       'font-size': '12px',
//       'ref-x': .05, 'ref-y': .5,
//       display: 'none', style: {'pointer-events': 'none'}
//     });

//     // testing getCustomOperatorStyleAttrs()
//     expect(graph_operator1.attr('#operatorStates')).toEqual({
//       text:  'Ready' , fill: 'green', 'font-size': '14px', 'visible' : false,
//       'ref-x': 0.5, 'ref-y': -10, ref: 'rect', 'y-alignment': 'middle', 'x-alignment': 'middle'
//     });
//     expect(graph_operator1.attr('rect')).toEqual(
//       { fill: '#FFFFFF', 'follow-scale': true, stroke: 'red', 'stroke-width': '2',
//       rx: '5px', ry: '5px' }
//     );
//     expect(graph_operator2.attr('rect')).toEqual(
//       { fill: '#FFFFFF', 'follow-scale': true, stroke: 'red', 'stroke-width': '2',
//       rx: '5px', ry: '5px' }
//     );
//     expect(graph_operator1.attr('.delete-button')).toEqual(
//       {
//         x: 60, y: -20, cursor: 'pointer',
//         fill: '#D8656A', event: 'element:delete'
//       }
//     );
//     expect(graph_operator2.attr('.delete-button')).toEqual(
//       {
//         x: 60, y: -20, cursor: 'pointer',
//         fill: '#D8656A', event: 'element:delete'
//       }
//     );

//     // testing getDefaultLinkElement()
//     expect(graph_link.attr('.marker-source/d')).toEqual(sourceOperatorHandle);
//     expect(graph_link.attr('.marker-target/d')).toEqual(targetOperatorHandle);
//     expect(graph_link.attr('.tool-remove path/d')).toEqual(deleteButtonPath);
//   });
// });

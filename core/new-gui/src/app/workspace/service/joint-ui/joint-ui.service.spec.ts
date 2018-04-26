import { TestBed, inject } from '@angular/core/testing';
import * as joint from 'jointjs';

import { JointUIService } from './joint-ui.service';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../operator-metadata/stub-operator-metadata.service';
import { MOCK_OPERATOR_METADATA } from '../operator-metadata/mock-operator-metadata.data';

describe('JointUIService', () => {
  let service: JointUIService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        JointUIService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
      ],
    });
    service = TestBed.get(JointUIService);
  });

  it('should be created', inject([JointUIService], (injectedService: JointUIService) => {
    expect(injectedService).toBeTruthy();
  }));

  /**
   * Check if the getJointjsOperatorElement() can successfully creates a JointJS Element
   */
  it('should create an JointJS Element successfully when the function is called', () => {
    const result = service.getJointjsOperatorElement(
      'ScanSource', 'operator1', { x: 100, y: 100 } );
    expect(result).toBeTruthy();
  });

  /**
   * Check if the error in getJointjsOperatorElement() is correctly thrown
   */
  it('should throw an error with an non existing operator', () => {
    const nonExistingOperator = 'NotExistOperator';
    expect(
      function() {
        service.getJointjsOperatorElement(
          nonExistingOperator, 'operatorNaN', { x: 100, y: 100 });
      }
    )
    .toThrow(new Error('JointUIService.getJointUI: ' +
    'cannot find operatorType: ' + nonExistingOperator));
  });


  /**
   * Check if the number of inPorts and outPorts created by getJointjsOperatorElement()
   * matches the port number specified by the operator metadata
   */
  it('should create correct number of inPorts and outPorts based on operator metadata', () => {
    const element1 = service.getJointjsOperatorElement('ScanSource', 'operator1', { x: 100, y: 100 });
    const element2 = service.getJointjsOperatorElement('NlpSentiment', 'operator1', { x: 100, y: 100 });
    const element3 = service.getJointjsOperatorElement('ViewResults', 'operator1', { x: 100, y: 100 });

    const inPortCount1 = element1.getPorts().filter(port => port.group === 'in').length;
    const outPortCount1 = element1.getPorts().filter(port => port.group === 'out').length;
    const inPortCount2 = element2.getPorts().filter(port => port.group === 'in').length;
    const outPortCount2 = element2.getPorts().filter(port => port.group === 'out').length;
    const inPortCount3 = element3.getPorts().filter(port => port.group === 'in').length;
    const outPortCount3 = element3.getPorts().filter(port => port.group === 'out').length;

    expect(inPortCount1).toEqual(0);
    expect(outPortCount1).toEqual(1);
    expect(inPortCount2).toEqual(1);
    expect(outPortCount2).toEqual(1);
    expect(inPortCount3).toEqual(1);
    expect(outPortCount3).toEqual(0);

  });

  /**
   * Check if the TexeraOperatorShape defined in setupCustomJointjsModel() is
   * correctly registered in the joint.shapes.devs
   */
  it('should create Texera\'s custom jointjs shape template in constructor', () => {
    expect(joint.shapes.devs['TexeraOperatorShape']).toBeTruthy();
  });

  /**
   * Check if the custom attributes / svgs are correctly used by the JointJS graph
   */
  it('should apply the custom SVG styling to the JointJS element', () => {

    const graph = new joint.dia.Graph();

    graph.addCell(
      service.getJointjsOperatorElement(
        'ScanSource',
        'operator1',
        { x: 100, y: 100 }
      )
    );

    graph.addCell(
      service.getJointjsOperatorElement(
        'ViewResults',
        'operator2',
        { x: 500, y: 100 }
      )
    );

    const link = service.getJointjsLinkElement(
      { operatorID: 'operator1', portID: 'out0' },
      { operatorID: 'operator2', portID: 'in0' }
    );

    graph.addCell(link);

    const graph_operator1 = graph.getCell('operator1');
    const graph_operator2 = graph.getCell('operator2');
    const graph_link = graph.getLinks()[0];

    // testing getCustomOperatorStyleAttrs()
    expect(graph_operator1.attr('rect')).toEqual(
      { fill: '#FFFFFF', 'follow-scale': true, stroke: '#CFCFCF', 'stroke-width': '2' }
    );
    expect(graph_operator2.attr('rect')).toEqual(
      { fill: '#FFFFFF', 'follow-scale': true, stroke: '#CFCFCF', 'stroke-width': '2' }
    );
    expect(graph_operator1.attr('.delete-button')).toEqual(
      {
        x: 135, y: -20, cursor: 'pointer',
        fill: '#D8656A', event: 'element:delete'
      }
    );
    expect(graph_operator2.attr('.delete-button')).toEqual(
      {
        x: 135, y: -20, cursor: 'pointer',
        fill: '#D8656A', event: 'element:delete'
      }
    );

    // testing getDefaultLinkElement()
    expect(graph_link.attr('.marker-source/d')).toEqual(service.sourceOperatorHandle);
    expect(graph_link.attr('.marker-target/d')).toEqual(service.targetOperatorHandle);
    expect(graph_link.attr('.tool-remove path/d')).toEqual(service.deleteButtonPath);
  });
});

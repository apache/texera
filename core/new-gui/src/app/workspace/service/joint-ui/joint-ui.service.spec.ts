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
  });

  it('should be created', inject([JointUIService], (ser: JointUIService) => {
    service = ser;
    expect(service).toBeTruthy();
  }));

  /**
   * Check if the getJointjsOperatorElement() can successfully creates a JointJS Element
   */
  it('getJointjsOperatorElement() should create an operatorElement', () => {
    const result = service.getJointjsOperatorElement('ScanSource', 'operator1', 100, 100);
    expect(result).toBeTruthy();
  });

  /**
   * Check if the error in getJointjsOperatorElement() is correctly thrown
   */
  it('getJointjsOperatorElement() should throw an error', () => {
    const nonExistingOperator = 'NotExistOperator';
    expect(
      function() {
        service.getJointjsOperatorElement(nonExistingOperator, 'operatorNaN', 100, 100);
      }
    )
    .toThrow(new Error('JointUIService.getJointUI: ' +
    'cannot find operatorType: ' + nonExistingOperator));
  });


  // TODOs
  it('getJointjsOperatorElement() should create correct number of inPorts', () => {
    const result = service.getJointjsOperatorElement('ScanSource', 'operator1', 100, 100);
  });

  it('getJointjsOperatorElement() should create correct number of outPorts', () => {
    const result = service.getJointjsOperatorElement('ScanSource', 'operator1', 100, 100);
  });

  /**
   * Check if the TexeraOperatorShape defined in setupCustomJointjsModel() is
   * correctly registered in the joint.shapes.devs
   */
  it('setupCustomJointjsModel() should create a custom jointjs model in constructor', () => {
    expect(joint.shapes.devs['TexeraOperatorShape']).toBeTruthy();
  });

  /**
   * Check if the custom attributes / svgs are correctly used by the JointJS graph
   */
  it('should apply the custom svgs defined be getCustomOperatorStyleAttrs() and ' +
  'getDefaultLinkElement() to the JointJS operator', () => {

    const graph = new joint.dia.Graph();

    graph.addCell(
      service.getJointjsOperatorElement(
        'ScanSource',
        'operator1',
        100, 100
      )
    );

    graph.addCell(
      service.getJointjsOperatorElement(
        'ViewResults',
        'operator2',
        500, 100
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

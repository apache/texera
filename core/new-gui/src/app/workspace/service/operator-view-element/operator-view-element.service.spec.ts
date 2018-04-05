import { TestBed, inject } from '@angular/core/testing';
import * as joint from 'jointjs';

import { OperatorViewElementService } from './operator-view-element.service';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../operator-metadata/stub-operator-metadata.service';
import { MOCK_OPERATOR_METADATA } from '../operator-metadata/mock-operator-metadata.data';

describe('OperatorViewElementService', () => {
  let service: OperatorViewElementService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        OperatorViewElementService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
      ],
    });
  });

  it('should be created', inject([OperatorViewElementService], (ser: OperatorViewElementService) => {
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
    .toThrow(new Error('OperatorViewElementService.getOperatorViewElement: ' +
    'cannot find operatorType: ' + nonExistingOperator));
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
  it('should change the style of the attributes', () => {
    const graph = new joint.dia.Graph();
    const deleteButtonPath = 'M14.59 8L12 10.59 9.41 8 8 9.41 10.59 12 8 14.59' +
    ' 9.41 16 12 13.41 14.59 16 16 14.59 13.41 12 16 9.41 14.59 8zM12 2C6.47' +
    ' 2 2 6.47 2 12s4.47 10 10 10 10-4.47 10-10S17.53 2 12 2zm0 18c-4.41 0-8-3.59-8-8s3.59-8 8-8 8 3.59 8 8-3.59 8-8 8z';

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

    expect(graph_link.attr('.marker-source/d')).toEqual('M 0 0 L 0 8 L 8 8 L 8 0 z');
    expect(graph_link.attr('.marker-target/d')).toEqual('M 12 0 L 0 6 L 12 12 z');
    expect(graph_link.attr('.tool-remove path/d')).toEqual(deleteButtonPath);
  });
});

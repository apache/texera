import { Injectable } from '@angular/core';

import * as joint from 'jointjs';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';


export const DEFAULT_OPERATOR_WIDTH = 140;
export const DEFAULT_OPERATOR_HEIGHT = 40;

/**
 * OperatorUIElementService controls the shape of an operator
 *  when the operator element is displayed by JointJS.
 *
 * This service alters the basic JointJS element by:
 *  - setting the ID of the JointJS element to be the same as Texera's OperatorID
 *  - changing the look of the operator box (size, colors, lines, etc..)
 *  - adding input and output ports to the box based on the operator metadata
 *  - changing the look of the ports
 *  - adding a new delete button and the callback function of the delete button,
 *      (original JointJS element doesn't have a built-in delete button)
 *
 * @author Henry Chen
 * @author Zuozhi Wang
 */
@Injectable()
export class OperatorUIElementService {

  private operatorSchemaList: OperatorSchema[] = [];

  constructor(private operatorMetadataService: OperatorMetadataService) {
    this.operatorMetadataService.metadataChanged$.subscribe(
      value => this.operatorSchemaList = value.operators
    );

  }

  /**
   * Gets the JointJS UI Element Object based on OperatorType OperatorID.
   *
   * The JointJS Element could be added to the JointJS graph to let JointJS display the operator accordingly.
   *
   * @param operatorType the type of the operator
   * @param operatorID the ID of the operator, the JointJS element ID would be the same as operatorID
   *
   * @returns JointJS Element
   */
  public getOperatorUIElement(operatorType: string, operatorID: string): joint.dia.Element {
    const operatorSchema = this.operatorSchemaList.find(schema => schema.operatorType === operatorType);

    joint.shapes.devs['TexeraModel'] = joint.shapes.devs.Model.extend({
      type: 'devs.TexeraModel',
      markup: '<g class="element-node">' +
        '<rect class="body" stroke-width="2" stroke="blue" rx="5px" ry="5px"></rect>' +
        '<svg class="delete-button" fill="#000000" height="24" viewBox="0 0 24 24" width="24" xmlns="http://www.w3.org/2000/svg">' +
        '<path d="M0 0h24v24H0z" fill="none" pointer-events="visible" />' +
        // tslint:disable-next-line:max-line-length
        '<path d="M14.59 8L12 10.59 9.41 8 8 9.41 10.59 12 8 14.59 9.41 16 12 13.41 14.59 16 16 14.59 13.41 12 16 9.41 14.59 8zM12 2C6.47 2 2 6.47 2 12s4.47 10 10 10 10-4.47 10-10S17.53 2 12 2zm0 18c-4.41 0-8-3.59-8-8s3.59-8 8-8 8 3.59 8 8-3.59 8-8 8z"/>' +
        '</svg>' +
        '<text>' +
        '</text>' +
        '</g>'
    });

    const portStyleAttrs = {
      '.port-body': {
        fill: '#A0A0A0',
        r: 6,
        stroke: 'none'
      },
      '.port-label': {
        display: 'none'
      }
    };


    const operatorElement = new joint.shapes.devs['TexeraModel']({
      id: operatorID,
      position: { x: 0, y: 0 },
      size: { width: DEFAULT_OPERATOR_WIDTH, height: DEFAULT_OPERATOR_HEIGHT },
      attrs: {
        'rect': { fill: '#FFFFFF', 'follow-scale': true, stroke: '#CFCFCF', 'stroke-width': '2' },
        'text': {
          text: operatorSchema.additionalMetadata.userFriendlyName, fill: 'black', 'font-size': '12px',
          'ref-x': 0.5, 'ref-y': 0.5, ref: 'rect', 'y-alignment': 'middle', 'x-alignment': 'middle'
        },
        '.delete-button': {
          x: 135, y: -20, cursor: 'pointer',
          fill: '#D8656A', event: 'element:delete'
        },
      },
      ports: {
        groups: {
          'in': { attrs: portStyleAttrs },
          'out': { attrs: portStyleAttrs },
        }
      }
    });


    for (let i = 0; i < operatorSchema.additionalMetadata.numInputPorts; i++) {
      operatorElement.addInPort(`in${i}`);
    }
    // set output ports
    for (let i = 0; i < operatorSchema.additionalMetadata.numOutputPorts; i++) {
      operatorElement.addOutPort(`out${i}`);
    }
    return operatorElement;
  }

}

import { Injectable } from '@angular/core';
import { OperatorSchema } from '../../types/operator-schema';
import { MOCK_OPERATOR_METADATA } from '../operator-metadata/mock-operator-metadata.data';

import * as joint from 'jointjs';
import { OperatorPort } from '../../types/operator-port';

export const DEFAULT_OPERATOR_WIDTH = 140;
export const DEFAULT_OPERATOR_HEIGHT = 40;


@Injectable()
export class StubOperatorViewElementService {


  private operators: OperatorSchema[] = [];

  // tslint:disable:max-line-length
  private readonly deleteButtonPath =
  'M14.59 8L12 10.59 9.41 8 8 9.41 10.59 12 8 14.59 9.41 16 12 13.41 14.59 16 16 14.59 13.41 12 16 9.41 14.59 8zM12 2C6.47 2 2 6.47 2 12s4.47 10 10 10 10-4.47 10-10S17.53 2 12 2zm0 18c-4.41 0-8-3.59-8-8s3.59-8 8-8 8 3.59 8 8-3.59 8-8 8z';

  private readonly deleteButtonSVG =
  `<svg class="delete-button" fill="#000000" height="24" viewBox="0 0 24 24" width="24" xmlns="http://www.w3.org/2000/svg">
    <path d="M0 0h24v24H0z" fill="none" pointer-events="visible" />
    <path d="${this.deleteButtonPath}"/>
  </svg>`;

  constructor() {
    this.operators = MOCK_OPERATOR_METADATA.operators;
    this.setupCustomJointjsModel();
  }



  /**
   * Gets the JointJS UI Element Object based on OperatorType OperatorID.
   *
   * The JointJS Element could be added to the JointJS graph to let JointJS display the operator accordingly.
   *
   * @param operatorType the type of the operator
   * @param operatorID the ID of the operator, the JointJS element ID would be the same as operatorID
   * @param xPosition the topleft x position of the operator element (relative to JointJS paper, not absolute position)
   * @param yPosition the topleft y position of the operator element (relative to JointJS paper, not absolute position)
   *
   * @returns JointJS Element
   */
  public getJointjsOperatorElement(
    operatorType: string, operatorID: string, xPosition: number, yPosition: number
  ): joint.dia.Element {

    const operatorSchema = this.operators.find(op => op.operatorType === operatorType);
    if (operatorSchema === null || operatorSchema === undefined) {
      throw new Error(
        'OperatorViewElementService.getOperatorViewElement: ' +
        'cannot find operatorType: ' + operatorType);
    }

    const operatorElement: joint.shapes.devs.Model = new joint.shapes.devs['TexeraOperatorShape']({
      id: operatorID,
      position: { x: xPosition, y: yPosition },
      size: { width: DEFAULT_OPERATOR_WIDTH, height: DEFAULT_OPERATOR_HEIGHT },
      attrs: getCustomOperatorStyleAttrs(operatorSchema.additionalMetadata.userFriendlyName),
      ports: {
        groups: {
          'in': { attrs: getCustomPortStyleAttrs() },
          'out': { attrs: getCustomPortStyleAttrs() }
        }
      }
    });

    for (let i = 0; i < operatorSchema.additionalMetadata.numInputPorts; i++) {
      operatorElement.addInPort(`in${i}`);
    }
    for (let i = 0; i < operatorSchema.additionalMetadata.numOutputPorts; i++) {
      operatorElement.addOutPort(`out${i}`);
    }

    return operatorElement;
  }

  public getJointjsLinkElement(
    source: OperatorPort, target: OperatorPort
  ): joint.dia.Link {
    const link = this.getDefaultLinkElement();
    link.set('source', { id: source.operatorID, port: source.portID });
    link.set('target', { id: target.operatorID, port: target.portID });
    return link;
  }

  public getDefaultLinkElement(): joint.dia.Link {
    const link = new joint.dia.Link({
      attrs: {
        '.connection-wrap': {
          'stroke-width': 0
        },
        '.marker-source': {
          d: 'M 0 0 L 0 8 L 8 8 L 8 0 z',
          stroke: 'none',
          fill: '#919191'
        },
        '.marker-arrowhead-group-source .marker-arrowhead': {
          d: 'M 0 0 L 0 8 L 8 8 L 8 0 z',
        },
        '.marker-target': {
          d: 'M 12 0 L 0 6 L 12 12 z',
          stroke: 'none',
          fill: '#919191'
        },
        '.marker-arrowhead-group-target .marker-arrowhead': {
          d: 'M 12 0 L 0 6 L 12 12 z',
        },
        '.tool-remove': {
          fill: '#D8656A',
          width: 24
        },
        '.tool-remove path': {
          d: this.deleteButtonPath,

        },
        '.tool-remove circle': {

          // visibility: 'hidden'
        }
      }
    });
    return link;
  }

  private setupCustomJointjsModel(): void {

    joint.shapes.devs['TexeraOperatorShape'] = joint.shapes.devs.Model.extend({
      type: 'devs.TexeraModel',
      markup:
        `<g class="element-node">
          <rect class="body" stroke-width="2" stroke="blue" rx="5px" ry="5px"></rect>
          ${this.deleteButtonSVG}
          <text></text>
        </g>`

    });
  }

}




export function getCustomPortStyleAttrs(): Object {
  const portStyleAttrs = {
    '.port-body': {
      fill: '#A0A0A0',
      r: 5,
      stroke: 'none'
    },
    '.port-label': {
      display: 'none'
    }
  };
  return portStyleAttrs;
}

export function getCustomOperatorStyleAttrs(operatorDisplayName: string): Object {
  const operatorStyleAttrs = {
    'rect': { fill: '#FFFFFF', 'follow-scale': true, stroke: '#CFCFCF', 'stroke-width': '2' },
    'text': {
      text: operatorDisplayName, fill: 'black', 'font-size': '12px',
      'ref-x': 0.5, 'ref-y': 0.5, ref: 'rect', 'y-alignment': 'middle', 'x-alignment': 'middle'
    },
    '.delete-button': {
      x: 135, y: -20, cursor: 'pointer',
      fill: '#D8656A', event: 'element:delete'
    },
  };
  return operatorStyleAttrs;
}

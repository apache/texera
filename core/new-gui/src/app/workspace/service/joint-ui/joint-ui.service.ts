import { Injectable } from '@angular/core';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { OperatorSchema } from '../../types/operator-schema';

import * as joint from 'jointjs';
import { OperatorPort } from '../../types/operator-port';

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
export class JointUIService {

  private operators: OperatorSchema[] = [];

  /**
   * This variable defines the svg property
   * of a delete button
   */
  public readonly deleteButtonPath =
  'M14.59 8L12 10.59 9.41 8 8 9.41 10.59 12 8 14.59 9.41 16 12 13.41' +
  ' 14.59 16 16 14.59 13.41 12 16 9.41 14.59 8zM12 2C6.47 2 2 6.47 2' +
  ' 12s4.47 10 10 10 10-4.47 10-10S17.53 2 12 2zm0 18c-4.41 0-8-3.59-8-8s3.59-8 8-8 8 3.59 8 8-3.59 8-8 8z';

  /**
   * This variable defines the wrapper around
   * the button, deciding the size and other styles
   * for the delte button.
   */
  public readonly deleteButtonSVG =
  `<svg class="delete-button" height="24" width="24">
    <path d="M0 0h24v24H0z" fill="none" pointer-events="visible" />
    <path d="${this.deleteButtonPath}"/>
  </svg>`;

  /**
   * This variable defines the handle (the square at the end)
   * of the source operator for a link
   */
  public readonly sourceOperatorHandle = 'M 0 0 L 0 8 L 8 8 L 8 0 z';

  /**
   * This variable defines the handle (the arrow at the end)
   * of the target operator for a link
   */
  public readonly targetOperatorHandle = 'M 12 0 L 0 6 L 12 12 z';

  constructor(
    private operatorMetadataService: OperatorMetadataService
  ) {
    this.operatorMetadataService.getOperatorMetadata().subscribe(
      value => this.operators = value.operators
    );

    this.setupCustomJointjsModel();
  }

  /**
   * Gets the JointJS UI Element Object based on OperatorType OperatorID.
   *
   * The JointJS Element could be added to the JointJS graph to let JointJS display the operator accordingly.
   * The function first check if the operatorType exists in the metadata, if it doesn't,
   * the program will throw an error. Then it creates a JointJS shape created from the
   * setupCustomJointjsModel() function called in the constructor above. In this function,
   * the custom JointJS shape will be populated with the operator's data. The style of the operator
   * will be changed by getCustomOperatorStyleAttrs() and getCustomPortStyleAttrs().
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
    if (operatorSchema === undefined) {
      throw new Error(
        'JointUIService.getJointUI: ' +
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

  /**
   * This function takes an source and target OperatorPort and
   * creates a JointJS link element that would be added to the JointJS
   * graph to let JointJS display the operator accordingly. This function
   * will connect the source and the target operator together on the graph.
   *
   * @param source the OperatorPort of the source of a link
   * @param target the OperatorPort of the target of a link
   * @returns JointJS Link Element
   */
  public getJointjsLinkElement(
    source: OperatorPort, target: OperatorPort
  ): joint.dia.Link {
    const link = this.getDefaultLinkElement();
    link.set('source', { id: source.operatorID, port: source.portID });
    link.set('target', { id: target.operatorID, port: target.portID });
    return link;
  }

  /**
   * This function will creates a custom JointJS link element using
   * custom attributes / styles to display the operator. This function
   * defines the svg properties for each part of link, such as the
   * shape of the arrow or the link. Other styles are defined in the
   * "app/workspace/component/workflow-editor/workflow-editor.component.scss".
   * The reason for separating styles in svg and css is that while we can
   * change the shape of the operators in svg, according to JointJS official
   * website, https://resources.jointjs.com/tutorial/element-styling, "
   * CSS properties have higher precedence over SVG element attributes."
   * As a result, a separate css/scss file is required to override the default
   * style of the operatorLink.
   *
   * @returns JointJS Link
   */
  public getDefaultLinkElement(): joint.dia.Link {
    const link = new joint.dia.Link({
      attrs: {
        '.connection-wrap': {
          'stroke-width': 0
        },
        '.marker-source': {
          d: this.sourceOperatorHandle,
          stroke: 'none',
          fill: '#919191'
        },
        '.marker-arrowhead-group-source .marker-arrowhead': {
          d: this.sourceOperatorHandle,
        },
        '.marker-target': {
          d: this.targetOperatorHandle,
          stroke: 'none',
          fill: '#919191'
        },
        '.marker-arrowhead-group-target .marker-arrowhead': {
          d: this.targetOperatorHandle,
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

  /**
   * This function registers a custom JointJS shape in the
   * joint.shapes.devs map so it can be used later on. This will also
   * attach the delete button svg created to this custom operator
   * The custom shape created will be a rectangle with a red delete
   * button on the top-right hand corner.
   */
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

/**
 * This function changes the default svg of the operator ports.
 * It hides the port label that will display 'out/in' beside
 * the operators.
 *
 * @returns the custom attributes of the ports
 */
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

/**
 * This function creates a custom svg style for the operator. Also, this function
 * will make the delete button defined above to emit the delete event that will
 * be captured by JointJS.
 *
 * @param operatorDisplayName the name of the operator that will display on the UI
 * @returns the custom attributes of the operator
 */
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

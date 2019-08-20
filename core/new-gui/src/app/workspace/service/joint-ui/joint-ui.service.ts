import { Injectable } from '@angular/core';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { OperatorSchema } from '../../types/operator-schema.interface';

import * as joint from 'jointjs';
import { Point, OperatorPredicate, OperatorLink, TooltipPredicate } from '../../types/workflow-common.interface';
import { Subject, Observable } from 'rxjs';

/**
 * Defines the SVG path for the delete button
 */
export const deleteButtonPath =
  'M14.59 8L12 10.59 9.41 8 8 9.41 10.59 12 8 14.59 9.41 16 12 13.41' +
  ' 14.59 16 16 14.59 13.41 12 16 9.41 14.59 8zM12 2C6.47 2 2 6.47 2' +
  ' 12s4.47 10 10 10 10-4.47 10-10S17.53 2 12 2zm0 18c-4.41 0-8-3.59-8-8s3.59-8 8-8 8 3.59 8 8-3.59 8-8 8z';
/**
 * Defines the HTML SVG element for the delete button and customizes the look
 */
export const deleteButtonSVG =
  `<svg class="delete-button" height="24" width="24">
    <path d="M0 0h24v24H0z" fill="none" pointer-events="visible" />
    <path d="${deleteButtonPath}"/>
  </svg>`;

/**
 * Defines the handle (the square at the end) of the source operator for a link
 */
export const sourceOperatorHandle = 'M 0 0 L 0 8 L 8 8 L 8 0 z';

/**
 * Defines the handle (the arrow at the end) of the target operator for a link
 */
export const targetOperatorHandle = 'M 12 0 L 0 6 L 12 12 z';

/**
 * Extends a basic Joint operator element and adds our own HTML markup.
 * Our own HTML markup includes the SVG element for the delete button,
 *   which will show a red delete button on the top right corner
 */
class TexeraCustomJointElement extends joint.shapes.devs.Model {
  markup =
    `<g class="element-node">
      <text id="operatorStatus"></text>
      <rect class="body"></rect>
      ${deleteButtonSVG}
      <image></image>
      <text id="operatorName"></text>
    </g>`;
}

class TexeraCustomTooltipElement extends joint.shapes.devs.Model {
  markup =
  `<g class="element-node">
    <rect class="body"></rect>
  </g>`;
}
/**
 * JointUIService controls the shape of an operator and a link
 *  when they is displayed by JointJS.
 *
 * This service alters the basic JointJS element by:
 *  - setting the ID of the JointJS element to be the same as Texera's OperatorID
 *  - changing the look of the operator box (size, colors, lines, etc..)
 *  - adding input and output ports to the box based on the operator metadata
 *  - changing the SVG element and CSS styles of operators, links, ports, etc..
 *  - adding a new delete button and the callback function of the delete button,
 *      (original JointJS element doesn't have a built-in delete button)
 *
 * @author Henry Chen
 * @author Zuozhi Wang
 */
@Injectable()
export class JointUIService {

  public static readonly DEFAULT_OPERATOR_WIDTH = 60;
  public static readonly DEFAULT_OPERATOR_HEIGHT = 60;

  private operatorStates: string;
  private operatorStatesSubject: Subject<string> = new Subject<string>();
  private operators: ReadonlyArray<OperatorSchema> = [];
  private operatorCount: string;
  private operatorCountSubject: Subject<string> = new Subject<string>();
  private operatorPopUpWindowDisable: boolean = false;
  constructor(
    private operatorMetadataService: OperatorMetadataService,
  ) {
    // initialize the operator status
    this.operatorStates = 'Ready';
    this.operatorCount = '';
    // subscribe to operator metadata observable
    this.operatorMetadataService.getOperatorMetadata().subscribe(
      value => this.operators = value.operators
    );

  }

  public initializeOperatorState(): void {
    this.operatorStates = 'Ready';
    this.operatorCount = '';
  }
  /**
   * Gets the JointJS UI element object based on the tooltip predicate
   * @param tooltipID the id of the tool tip, in this case we set the id the same as the corresponding operator id
   * @param processedCount the number of the data that operator handle
   * @param tooltipType the type of the tool tip for further use (i.e to show the flow chart to represent the speed of * the data handling)
   */
  public getJointTooltipElement(
    operator: OperatorPredicate, tooltip: TooltipPredicate, point: Point
  ): joint.dia.Element {
      // check if the operatorType exists in the operator metadata
    const operatorSchema = this.operators.find(op => op.operatorType === operator.operatorType);
    if (operatorSchema === undefined) {
      throw new Error(`operator type ${operator.operatorType} doesn't exist`);
    }
    const tooltipPoint = {x: point.x - JointUIService.DEFAULT_OPERATOR_WIDTH / 2,
       y: point.y - JointUIService.DEFAULT_OPERATOR_HEIGHT * 2 - 10};

    const toolTipElement = new TexeraCustomTooltipElement({
      position: tooltipPoint,
      size: {width: JointUIService.DEFAULT_OPERATOR_WIDTH * 2, height: JointUIService.DEFAULT_OPERATOR_HEIGHT * 2},
      attrs: JointUIService.getCustomTooltipStyleAttrs(tooltip)
    });


    toolTipElement.set('id', 'tooltip-' + operator.operatorID);

    return toolTipElement;
  }

  /**
   * Gets the JointJS UI Element object based on the operator predicate.
   * A JointJS Element could be added to the JointJS graph to let JointJS display the operator accordingly.
   *
   * The function checks if the operatorType exists in the metadata,
   *  if it doesn't, the program will throw an error.
   *
   * The function returns an element that has our custom style,
   *  which are specified in getCustomOperatorStyleAttrs() and getCustomPortStyleAttrs()
   *
   *
   * @param operatorType the type of the operator
   * @param operatorID the ID of the operator, the JointJS element ID would be the same as operatorID
   * @param xPosition the topleft x position of the operator element (relative to JointJS paper, not absolute position)
   * @param yPosition the topleft y position of the operator element (relative to JointJS paper, not absolute position)
   *
   * @returns JointJS Element
   */
  public getJointOperatorElement(
    operator: OperatorPredicate, point: Point
  ): joint.dia.Element {

    // check if the operatorType exists in the operator metadata
    const operatorSchema = this.operators.find(op => op.operatorType === operator.operatorType);
    if (operatorSchema === undefined) {
      throw new Error(`operator type ${operator.operatorType} doesn't exist`);
    }

    // construct a custom Texera JointJS operator element
    //   and customize the styles of the operator box and ports
    const operatorElement = new TexeraCustomJointElement({

      position: point,
      size: { width: JointUIService.DEFAULT_OPERATOR_WIDTH, height: JointUIService.DEFAULT_OPERATOR_HEIGHT },
      attrs: JointUIService.getCustomOperatorStyleAttrs( this.operatorCount,
        this.operatorStates, operatorSchema.additionalMetadata.userFriendlyName, operatorSchema.operatorType),
      ports: {
        groups: {
          'in': { attrs: JointUIService.getCustomPortStyleAttrs() },
          'out': { attrs: JointUIService.getCustomPortStyleAttrs() }
        }
      }
    });
    // set operator element ID to be operator ID
    operatorElement.set('id', operator.operatorID);

    // set the input ports and output ports based on operator predicate
    operator.inputPorts.forEach(
      port => operatorElement.addInPort(port)
    );
    operator.outputPorts.forEach(
      port => operatorElement.addOutPort(port)
    );

    return operatorElement;
  }

  public sendOperatorStateMessage(): void {
    this.operatorStatesSubject.next();
  }

  public getOperatorStateStream(): Observable<string> {
    return this.operatorStatesSubject.asObservable();
  }

  public showToolTip(showtooltip: boolean): void {

  }
  public changeOperatorCountWindow(jointPaper: joint.dia.Paper, operatorID: string, canShow: boolean, count: string) {
    if (canShow === true) {
      jointPaper.getModelById(operatorID).attr('#operatorCount/text', count);
    } else {
      jointPaper.getModelById(operatorID).attr('#operatorCount/text', '');
    }
  }

  public changeOperatorStatus(jointPaper: joint.dia.Paper, operatorID: string, status: string): void {
      this.operatorStates = status;
      if (status === '"Processing"') {
        jointPaper.getModelById(operatorID).attr('#operatorStatus/text', 'Process...');
        jointPaper.getModelById(operatorID).attr('#operatorStatus/fill', 'orange');
      } else if (status === '"Finished"') {
        jointPaper.getModelById(operatorID).attr('#operatorStatus/text', 'Finished');
        jointPaper.getModelById(operatorID).attr('#operatorStatus/fill', 'green');
      } else if (status === '"Paused"') {
        jointPaper.getModelById(operatorID).attr('#operatorStatus/text', 'Pause');
        jointPaper.getModelById(operatorID).attr('#operatorStatus/fill', 'orange');
      } else if (status === '"ProcessCompleted"') {
        jointPaper.getModelById(operatorID).attr('#operatorStatus/text', 'Finished');
        jointPaper.getModelById(operatorID).attr('#operatorStatus/fill', 'green');
      } else if (status === '"Pending"') {
        jointPaper.getModelById(operatorID).attr('#operatorStatus/text', 'Pending');
        jointPaper.getModelById(operatorID).attr('#operatorStatus/fill', 'orange');
      }
  }

  /**
   * This method will change the operator's color based on the validation status
   *  valid  : default color
   *  invalid: red
   *
   * @param jointPaper
   * @param operatorID
   * @param status
   */
  public changeOperatorColor(jointPaper: joint.dia.Paper, operatorID: string, status: boolean): void {
    if (status) {
      jointPaper.getModelById(operatorID).attr('rect/stroke', '#CFCFCF');
    } else {
      jointPaper.getModelById(operatorID).attr('rect/stroke', 'red');
    }
  }
  /**
   * This function converts a Texera source and target OperatorPort to
   *   a JointJS link cell <joint.dia.Link> that could be added to the JointJS.
   *
   * @param source the OperatorPort of the source of a link
   * @param target the OperatorPort of the target of a link
   * @returns JointJS Link Cell
   */
  public static getJointLinkCell(
    link: OperatorLink
  ): joint.dia.Link {
    const jointLinkCell = JointUIService.getDefaultLinkCell();
    jointLinkCell.set('source', { id: link.source.operatorID, port: link.source.portID });
    jointLinkCell.set('target', { id: link.target.operatorID, port: link.target.portID });
    jointLinkCell.set('id', link.linkID);
    return jointLinkCell;
  }

  /**
   * This function will creates a custom JointJS link cell using
   *  custom attributes / styles to display the operator.
   *
   * This function defines the svg properties for each part of link, such as the
   *   shape of the arrow or the link. Other styles are defined in the
   *   "app/workspace/component/workflow-editor/workflow-editor.component.scss".
   *
   * The reason for separating styles in svg and css is that while we can
   *   change the shape of the operators in svg, according to JointJS official
   *   website, https://resources.jointjs.com/tutorial/element-styling ,
   *   CSS properties have higher precedence over SVG attributes.
   *
   * As a result, a separate css/scss file is required to override the default
   * style of the operatorLink.
   *
   * @returns JointJS Link
   */
  public static getDefaultLinkCell(): joint.dia.Link {
    const link = new joint.dia.Link({
      router: {
        name: 'manhattan'
      },
      connector: {
        name: 'rounded'
      },
      attrs: {
        '.connection-wrap': {
          'stroke-width': 0
        },
        '.marker-source': {
          d: sourceOperatorHandle,
          stroke: 'none',
          fill: '#919191'
        },
        '.marker-arrowhead-group-source .marker-arrowhead': {
          d: sourceOperatorHandle,
        },
        '.marker-target': {
          d: targetOperatorHandle,
          stroke: 'none',
          fill: '#919191'
        },
        '.marker-arrowhead-group-target .marker-arrowhead': {
          d: targetOperatorHandle,
        },
        '.tool-remove': {
          fill: '#D8656A',
          width: 24
        },
        '.tool-remove path': {
          d: deleteButtonPath,
        },
        '.tool-remove circle': {
        }
      }
    });
    return link;
  }

  /**
   * This function changes the default svg of the operator ports.
   * It hides the port label that will display 'out/in' beside the operators.
   *
   * @returns the custom attributes of the ports
   */
  public static getCustomPortStyleAttrs(): joint.attributes.SVGAttributes {
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

  public static getCustomTooltipStyleAttrs(
    tooltip: TooltipPredicate): joint.shapes.devs.ModelSelectors {
    const tooltipStyleAttrs = {
      'rect': {
        text: 'hi, this is tool tip', fill: '#FFFFFF', 'follow-scale': true, stroke: 'green', 'stroke-width': '2',
      }
    };
    return tooltipStyleAttrs;
  }
  /**
   * This function creates a custom svg style for the operator.
   * This function also make sthe delete button defined above to emit the delete event that will
   *   be captured by JointJS paper using event name *element:delete*
   *
   * @param operatorDisplayName the name of the operator that will display on the UI
   * @returns the custom attributes of the operator
   */
  public static getCustomOperatorStyleAttrs( operatorCount: string,
    operatorStates: string, operatorDisplayName: string, operatorType: string): joint.shapes.devs.ModelSelectors {
    const operatorStyleAttrs = {

      '#operatorStatus': {
        text:  operatorStates , fill: 'red', 'font-size': '14px', 'visible' : false,
        'ref-x': 0.5, 'ref-y': -10, ref: 'rect', 'y-alignment': 'middle', 'x-alignment': 'middle'
      },
      'rect': {
        fill: '#FFFFFF', 'follow-scale': true, stroke: 'red', 'stroke-width': '2',
        rx: '5px', ry: '5px', width: 10, height: 10,
      },

      '#operatorName': {
        text: operatorDisplayName, fill: '#595959', 'font-size': '14px',
        'ref-x': 0.5, 'ref-y': 80, ref: 'rect', 'y-alignment': 'middle', 'x-alignment': 'middle'
      },
      '.delete-button': {
        x: 60, y: -20, cursor: 'pointer',
        fill: '#D8656A', event: 'element:delete'
      },
      'image': {
        'xlink:href': 'assets/operator_images/' + operatorType + '.png',
        width: 35, height: 35,
        'ref-x': .5, 'ref-y': .5,
        ref: 'rect',
        'x-alignment': 'middle',
        'y-alignment': 'middle',

      },
    };
    return operatorStyleAttrs;
  }



}

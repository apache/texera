import { Injectable } from "@angular/core";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { OperatorSchema } from "../../types/operator-schema.interface";

import { OperatorResultCacheStatus } from "../../types/workflow-websocket.interface";
import { abbreviateNumber } from "js-abbreviation-number";
import { Point, OperatorPredicate, OperatorLink, CommentBox } from "../../types/workflow-common.interface";
import { Group, GroupBoundingBox } from "../workflow-graph/model/operator-group";
import { OperatorState, OperatorStatistics } from "../../types/execute-workflow.interface";
import * as joint from "jointjs";
import { fromEventPattern, Observable } from "rxjs";
import { Coeditor, User } from "../../../common/type/user";

/**
 * Defines the SVG element for the breakpoint button
 */
export const breakpointButtonSVG = `
  <svg class="breakpoint-button" height = "24" width = "24">
    <path d="M0 0h24v24H0z" fill="none" /> +
    <polygon points="8,2 16,2 22,8 22,16 16,22 8,22 2,16 2,8" fill="red" />
    <title>add breakpoint</title>
  </svg>
  `;
/**
 * Defines the SVG path for the delete button
 */
export const deleteButtonPath =
  "M14.59 8L12 10.59 9.41 8 8 9.41 10.59 12 8 14.59 9.41 16 12 13.41" +
  " 14.59 16 16 14.59 13.41 12 16 9.41 14.59 8zM12 2C6.47 2 2 6.47 2" +
  " 12s4.47 10 10 10 10-4.47 10-10S17.53 2 12 2zm0 18c-4.41 0-8-3.59-8-8s3.59-8 8-8 8 3.59 8 8-3.59 8-8 8z";

/**
 * Defines the HTML SVG element for the delete button and customizes the look
 */
export const deleteButtonSVG = `
  <svg class="delete-button" height="24" width="24">
    <path d="M0 0h24v24H0z" fill="none" pointer-events="visible" />
    <path d="${deleteButtonPath}"/>
    <title>delete operator</title>
  </svg>`;

export const addPortButtonPath = `
<path d="M215.037,36.846c-49.129-49.128-129.063-49.128-178.191,0c-49.127,49.127-49.127,129.063,0,178.19
c24.564,24.564,56.83,36.846,89.096,36.846s64.531-12.282,89.096-36.846C264.164,165.909,264.164,85.973,215.037,36.846z
 M49.574,202.309c-42.109-42.109-42.109-110.626,0-152.735c21.055-21.054,48.711-31.582,76.367-31.582s55.313,10.527,76.367,31.582
c42.109,42.109,42.109,110.626,0,152.735C160.199,244.417,91.683,244.417,49.574,202.309z"/>
<path d="M194.823,116.941h-59.882V57.059c0-4.971-4.029-9-9-9s-9,4.029-9,9v59.882H57.059c-4.971,0-9,4.029-9,9s4.029,9,9,9h59.882
v59.882c0,4.971,4.029,9,9,9s9-4.029,9-9v-59.882h59.882c4.971,0,9-4.029,9-9S199.794,116.941,194.823,116.941z"/>
`;

export const removePortButtonPath = `
<path d="M215.037,36.846c-49.129-49.128-129.063-49.128-178.191,0c-49.127,49.127-49.127,129.063,0,178.19
c24.564,24.564,56.83,36.846,89.096,36.846s64.531-12.282,89.096-36.846C264.164,165.909,264.164,85.973,215.037,36.846z
 M49.574,202.309c-42.109-42.109-42.109-110.626,0-152.735c21.055-21.054,48.711-31.582,76.367-31.582s55.313,10.527,76.367,31.582
c42.109,42.109,42.109,110.626,0,152.735C160.199,244.417,91.683,244.417,49.574,202.309z"/>
<path d="M194.823,116.941H57.059c-4.971,0-9,4.029-9,9s4.029,9,9,9h137.764c4.971,0,9-4.029,9-9S199.794,116.941,194.823,116.941z"
/>`;

export const addInputPortButtonSVG = `
  <svg class="add-input-port-button">
    <g transform="scale(0.075)">${addPortButtonPath}</g>
    <title>add port</title>
  </svg>
`;

export const removeInputPortButtonSVG = `
  <svg class="remove-input-port-button">
  <g transform="scale(0.075)">${removePortButtonPath}</g>
    <title>remove port</title>
  </svg>
`;

export const addOutputPortButtonSVG = `
  <svg class="add-output-port-button">
    <g transform="scale(0.075)">${addPortButtonPath}</g>
    <title>add port</title>
  </svg>
`;

export const removeOutputPortButtonSVG = `
  <svg class="remove-output-port-button">
    <g transform="scale(0.075)">${removePortButtonPath}</g>
    <title>remove port</title>
  </svg>
`;

/**
 * Defines the SVG path for the collapse button
 */
export const collapseButtonPath =
  "M4 7 H12 v2 H4 z" +
  " M0 3 Q0 0 3 0 h10 Q16 0 16 3 v10 H14 V3 Q14 2 13 2 H3 Q2 2 2 3 z" +
  " M0 3 v10 Q0 16 3 16 h10 Q16 16 16 13 H14 Q14 14 13 14 H3 Q2 14 2 13 V3 z";

/**
 * Defines the HTML SVG element for the collapse button and customizes the look
 */
export const collapseButtonSVG = `<svg class="collapse-button" height="16" width="16">
    <path d="M0 0 h16 v16 H0 z" fill="none" pointer-events="visible" />
    <path d="${collapseButtonPath}" />
  </svg>`;

/**
 * Defines the SVG path for the expand button
 */
export const expandButtonPath =
  "M4 7 h3 V4 h2 V7 h3 v2 h-3 V12 h-2 V9 h-3 z" +
  " M0 3 Q0 0 3 0 h10 Q16 0 16 3 v10 H14 V3 Q14 2 13 2 H3 Q2 2 2 3 z" +
  " M0 3 v10 Q0 16 3 16 h10 Q16 16 16 13 H14 Q14 14 13 14 H3 Q2 14 2 13 V3 z";

/**
 * Defines the HTML SVG element for the expand button and customizes the look
 */
export const expandButtonSVG = `<svg class="expand-button" height="16" width="16">
    <path d="M0 0 h16 v16 H0 z" fill="none" pointer-events="visible" />
    <path d="${expandButtonPath}" />
  </svg>`;

/**
 * Defines the handle (the square at the end) of the source operator for a link
 */
export const sourceOperatorHandle = "M 0 0 L 0 8 L 8 8 L 8 0 z";

/**
 * Defines the handle (the arrow at the end) of the target operator for a link
 */
export const targetOperatorHandle = "M 12 0 L 0 6 L 12 12 z";

export const operatorCacheTextClass = "texera-operator-result-cache-text";
export const operatorCacheIconClass = "texera-operator-result-cache-icon";
export const operatorStateBGClass = "texera-operator-state-background";
export const operatorStateClass = "texera-operator-state";

export const operatorProcessedCountBGClass = "texera-operator-processed-count-background";
export const operatorProcessedCountClass = "texera-operator-processed-count";
export const operatorOutputCountBGClass = "texera-operator-output-count-background";
export const operatorOutputCountClass = "texera-operator-output-count";
export const operatorAbbreviatedCountBGClass = "texera-operator-abbreviated-count-background";
export const operatorAbbreviatedCountClass = "texera-operator-abbreviated-count";
export const operatorCoeditorEditingClass = "texera-operator-coeditor-editing";
export const operatorCoeditorEditingBGClass = "texera-operator-coeditor-editing-background";
export const operatorCoeditorChangedPropertyClass = "texera-operator-coeditor-changed-property";
export const operatorCoeditorChangedPropertyBGClass = "texera-operator-coeditor-changed-property-background";

export const operatorIconClass = "texera-operator-icon";
export const operatorNameClass = "texera-operator-name";
export const operatorNameBGClass = "texera-operator-name-background";

export const linkPathStrokeColor = "#919191";

/**
 * Extends a basic Joint operator element and adds our own HTML markup.
 * Our own HTML markup includes the SVG element for the delete button,
 *   which will show a red delete button on the top right corner
 */
class TexeraCustomJointElement extends joint.shapes.devs.Model {
  static getMarkup(dynamicInputPorts: boolean, dynamicOutputPorts: boolean): string {
    return `
    <g class="element-node">
      <rect class="body"></rect>
      <image class="${operatorIconClass}"></image>
      <text class="${operatorNameBGClass}"></text>
      <text class="${operatorNameClass}"></text>
      <text class="${operatorProcessedCountBGClass}"></text>
      <text class="${operatorProcessedCountClass}"></text>
      <text class="${operatorOutputCountBGClass}"></text>
      <text class="${operatorOutputCountClass}"></text>
      <text class="${operatorAbbreviatedCountBGClass}"></text>
      <text class="${operatorAbbreviatedCountClass}"></text>
      <text class="${operatorStateBGClass}"></text>
      <text class="${operatorStateClass}"></text>
      <text class="${operatorCacheTextClass}"></text>
      <text class="${operatorCoeditorEditingBGClass}"></text>
      <text class="${operatorCoeditorEditingClass}"></text>
      <text class="${operatorCoeditorChangedPropertyBGClass}"></text>
      <text class="${operatorCoeditorChangedPropertyClass}"></text>
      <image class="${operatorCacheIconClass}"></image>
      <rect class="boundary"></rect>
      <path class="left-boundary"></path>
      <path class="right-boundary"></path>
      ${deleteButtonSVG}
      ${dynamicInputPorts ? addInputPortButtonSVG : ""}
      ${dynamicInputPorts ? removeInputPortButtonSVG : ""}
      ${dynamicOutputPorts ? addOutputPortButtonSVG : ""}
      ${dynamicOutputPorts ? removeOutputPortButtonSVG : ""}
    </g>
    `;
  }
}

/**
 * Extends a basic Joint shape element and adds our own HTML markup.
 */
class TexeraCustomGroupElement extends joint.shapes.devs.Model {
  markup = `<g class="element-node">
      <rect class="body"></rect>
      <text>New Group</text>
      ${collapseButtonSVG}
      ${expandButtonSVG}
    </g>`;
}

class TexeraCustomCommentElement extends joint.shapes.devs.Model {
  markup = `<g class = "element-node">
  <rect class = "body"></rect>
  ${deleteButtonSVG}
  <image></image>
  </g>`;
}
/**
 * JointUIService controls the shape of an operator and a link
 *  when they are displayed by JointJS.
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
@Injectable({
  providedIn: "root",
})
export class JointUIService {
  public static readonly DEFAULT_OPERATOR_WIDTH = 60;
  public static readonly DEFAULT_OPERATOR_HEIGHT = 60;

  public static readonly DEFAULT_TOOLTIP_WIDTH = 140;
  public static readonly DEFAULT_TOOLTIP_HEIGHT = 60;

  public static readonly DEFAULT_GROUP_MARGIN = 50;
  public static readonly DEFAULT_GROUP_MARGIN_BOTTOM = 40;

  private operatorSchemas: ReadonlyArray<OperatorSchema> = [];
  public static readonly DEFAULT_COMMENT_WIDTH = 32;
  public static readonly DEFAULT_COMMENT_HEIGHT = 32;

  constructor(private operatorMetadataService: OperatorMetadataService) {
    // initialize the operator information
    // subscribe to operator metadata observable
    this.operatorMetadataService.getOperatorMetadata().subscribe(value => (this.operatorSchemas = value.operators));
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
   * @param operator OperatorPredicate, the type of the operator
   * @param point Point, the top-left-originated position of the operator element (relative to JointJS paper, not absolute position)
   *
   * @returns JointJS Element
   */

  public getJointOperatorElement(operator: OperatorPredicate, point: Point): joint.dia.Element {
    // check if the operatorType exists in the operator metadata
    const operatorSchema = this.operatorSchemas.find(op => op.operatorType === operator.operatorType);
    if (operatorSchema === undefined) {
      throw new Error(`operator type ${operator.operatorType} doesn't exist`);
    }

    // construct a custom Texera JointJS operator element
    //   and customize the styles of the operator box and ports
    const operatorElement = new TexeraCustomJointElement({
      position: point,
      size: {
        width: JointUIService.DEFAULT_OPERATOR_WIDTH,
        height: JointUIService.DEFAULT_OPERATOR_HEIGHT,
      },
      attrs: JointUIService.getCustomOperatorStyleAttrs(
        operator,
        operator.customDisplayName ?? operatorSchema.additionalMetadata.userFriendlyName,
        operatorSchema.operatorType
      ),
      ports: {
        groups: {
          in: { attrs: JointUIService.getCustomPortStyleAttrs() },
          out: { attrs: JointUIService.getCustomPortStyleAttrs() },
        },
      },
      markup: TexeraCustomJointElement.getMarkup(
        operator.dynamicInputPorts ?? false,
        operator.dynamicOutputPorts ?? false
      ),
    });

    // set operator element ID to be operator ID
    operatorElement.set("id", operator.operatorID);

    // set the input ports and output ports based on operator predicate
    operator.inputPorts.forEach(port =>
      operatorElement.addPort({
        group: "in",
        id: port.portID,
        attrs: {
          ".port-label": {
            text: port.displayName ?? "",
            event: "input-port-label:pointerdown",
          },
        },
      })
    );
    operator.outputPorts.forEach(port =>
      operatorElement.addPort({
        group: "out",
        id: port.portID,
        attrs: {
          ".port-label": {
            text: port.displayName ?? "",
            event: "output-port-label:pointerdown",
          },
        },
      })
    );

    return operatorElement;
  }

  public changeOperatorStatistics(
    jointPaper: joint.dia.Paper,
    operatorID: string,
    statistics: OperatorStatistics,
    isSource: boolean,
    isSink: boolean
  ): void {
    this.changeOperatorState(jointPaper, operatorID, statistics.operatorState);

    const processedText = isSource ? "" : "Processed: " + statistics.aggregatedInputRowCount.toLocaleString();
    const outputText = isSink ? "" : "Output: " + statistics.aggregatedOutputRowCount.toLocaleString();
    const processedCountText = isSource ? "" : abbreviateNumber(statistics.aggregatedInputRowCount);
    const outputCountText = isSink ? "" : abbreviateNumber(statistics.aggregatedOutputRowCount);
    const abbreviatedText = processedCountText + (isSource || isSink ? "" : " → ") + outputCountText;
    jointPaper.getModelById(operatorID).attr({
      [`.${operatorProcessedCountBGClass}`]: isSink ? { text: processedText, "ref-y": -30 } : { text: processedText },
      [`.${operatorProcessedCountClass}`]: isSink ? { text: processedText, "ref-y": -30 } : { text: processedText },
      [`.${operatorOutputCountClass}`]: { text: outputText },
      [`.${operatorOutputCountBGClass}`]: { text: outputText },
      [`.${operatorAbbreviatedCountClass}`]: { text: abbreviatedText },
      [`.${operatorAbbreviatedCountBGClass}`]: { text: abbreviatedText },
    });
  }

  public changeOperatorEditingStatus(jointPaper: joint.dia.Paper, operatorID: string, users?: User[]): void {
    console.log(operatorID);
    if (users) {
      const statusText = users[0].name + " is editing properties...";
      const color = users[0].color;
      jointPaper.getModelById(operatorID).attr({
        [`.${operatorCoeditorEditingClass}`]: {
          text: statusText,
          fillColor: color,
        },
      });
    } else {
      jointPaper.getModelById(operatorID).attr({
        [`.${operatorCoeditorEditingClass}`]: {
          text: "",
        },
      });
    }
  }

  public foldOperatorDetails(jointPaper: joint.dia.Paper, operatorID: string): void {
    jointPaper.getModelById(operatorID).attr({
      [`.${operatorAbbreviatedCountBGClass}`]: { visibility: "visible" },
      [`.${operatorAbbreviatedCountClass}`]: { visibility: "visible" },
      [`.${operatorProcessedCountClass}`]: { visibility: "hidden" },
      [`.${operatorProcessedCountBGClass}`]: { visibility: "hidden" },
      [`.${operatorOutputCountBGClass}`]: { visibility: "hidden" },
      [`.${operatorOutputCountClass}`]: { visibility: "hidden" },
      [`.${operatorStateBGClass}`]: { visibility: "hidden" },
      [`.${operatorStateClass}`]: { visibility: "hidden" },
      ".delete-button": { visibility: "hidden" },
      ".add-input-port-button": { visibility: "hidden" },
      ".add-output-port-button": { visibility: "hidden" },
      ".remove-input-port-button": { visibility: "hidden" },
      ".remove-output-port-button": { visibility: "hidden" },
    });
  }

  public unfoldOperatorDetails(jointPaper: joint.dia.Paper, operatorID: string): void {
    jointPaper.getModelById(operatorID).attr({
      [`.${operatorAbbreviatedCountBGClass}`]: { visibility: "hidden" },
      [`.${operatorAbbreviatedCountClass}`]: { visibility: "hidden" },
      [`.${operatorProcessedCountClass}`]: { visibility: "visible" },
      [`.${operatorProcessedCountBGClass}`]: { visibility: "visible" },
      [`.${operatorOutputCountBGClass}`]: { visibility: "visible" },
      [`.${operatorOutputCountClass}`]: { visibility: "visible" },
      [`.${operatorStateBGClass}`]: { visibility: "visible" },
      [`.${operatorStateClass}`]: { visibility: "visible" },
      ".delete-button": { visibility: "visible" },
      ".add-input-port-button": { visibility: "visible" },
      ".add-output-port-button": { visibility: "visible" },
      ".remove-input-port-button": { visibility: "visible" },
      ".remove-output-port-button": { visibility: "visible" },
    });
  }

  /**
   * Gets the JointJS UI Element object based on the group.
   * A JointJS Element could be added to the JointJS graph to let JointJS display the group accordingly.
   *
   * The function returns an element that has our custom style,
   *  which is specified in getCustomGroupStyleAttrs().
   *
   * @param group
   * @param topLeft the position of the operator (if there was one) that's in the top left corner of the group
   * @param bottomRight the position of the operator (if there was one) that's in the bottom right corner of the group
   */
  public getJointGroupElement(group: Group, boundingBox: GroupBoundingBox): joint.dia.Element {
    const { topLeft, bottomRight } = boundingBox;

    const groupElementPosition = {
      x: topLeft.x - JointUIService.DEFAULT_GROUP_MARGIN,
      y: topLeft.y - JointUIService.DEFAULT_GROUP_MARGIN,
    };
    const widthMargin = JointUIService.DEFAULT_OPERATOR_WIDTH + 2 * JointUIService.DEFAULT_GROUP_MARGIN;
    const heightMargin =
      JointUIService.DEFAULT_OPERATOR_HEIGHT +
      JointUIService.DEFAULT_GROUP_MARGIN +
      JointUIService.DEFAULT_GROUP_MARGIN_BOTTOM;

    const groupElement = new TexeraCustomGroupElement({
      position: groupElementPosition,
      size: {
        width: bottomRight.x - topLeft.x + widthMargin,
        height: bottomRight.y - topLeft.y + heightMargin,
      },
      attrs: JointUIService.getCustomGroupStyleAttrs(bottomRight.x - topLeft.x + widthMargin),
    });

    groupElement.set("id", group.groupID);
    return groupElement;
  }

  public changeOperatorState(jointPaper: joint.dia.Paper, operatorID: string, operatorState: OperatorState): void {
    let fillColor: string;
    switch (operatorState) {
      case OperatorState.Ready:
        fillColor = "#a6bd37";
        break;
      case OperatorState.Completed:
        fillColor = "green";
        break;
      case OperatorState.Pausing:
      case OperatorState.Paused:
        fillColor = "magenta";
        break;
      case OperatorState.Running:
        fillColor = "orange";
        break;
      default:
        fillColor = "gray";
        break;
    }
    jointPaper.getModelById(operatorID).attr({
      [`.${operatorStateClass}`]: { text: operatorState.toString() },
      [`.${operatorStateBGClass}`]: { text: operatorState.toString() },
      [`.${operatorStateClass}`]: { fill: fillColor },
      "rect.body": { stroke: fillColor },
      [`.${operatorAbbreviatedCountClass}`]: { fill: fillColor },
      [`.${operatorProcessedCountClass}`]: { fill: fillColor },
      [`.${operatorOutputCountClass}`]: { fill: fillColor },
    });
  }

  /**
   * Hides the expand button and shows the collapse button of
   * the given group on joint paper.
   *
   * @param jointPaper
   * @param groupID
   */
  public hideGroupExpandButton(jointPaper: joint.dia.Paper, groupID: string): void {
    jointPaper.getModelById(groupID).attr(".expand-button/display", "none");
    jointPaper.getModelById(groupID).removeAttr(".collapse-button/display");
  }

  /**
   * Hides the collapse button and shows the expand button of
   * the given group on joint paper.
   *
   * @param jointPaper
   * @param groupID
   */
  public hideGroupCollapseButton(jointPaper: joint.dia.Paper, groupID: string): void {
    jointPaper.getModelById(groupID).attr(".collapse-button/display", "none");
    jointPaper.getModelById(groupID).removeAttr(".expand-button/display");
  }

  /**
   * Repositions the collapse button of the given group according
   * to the group's (new) width.
   *
   * @param jointPaper
   * @param groupID
   * @param width
   */
  public repositionGroupCollapseButton(jointPaper: joint.dia.Paper, groupID: string, width: number): void {
    jointPaper.getModelById(groupID).attr(".collapse-button/x", `${width - 23}`);
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
  public changeOperatorColor(jointPaper: joint.dia.Paper, operatorID: string, isOperatorValid: boolean): void {
    if (isOperatorValid) {
      jointPaper.getModelById(operatorID).attr("rect.body/stroke", "#CFCFCF");
    } else {
      jointPaper.getModelById(operatorID).attr("rect.body/stroke", "red");
    }
  }

  public changeOperatorDisableStatus(jointPaper: joint.dia.Paper, operator: OperatorPredicate): void {
    jointPaper.getModelById(operator.operatorID).attr("rect.body/fill", JointUIService.getOperatorFillColor(operator));
  }

  public changeOperatorCacheStatus(
    jointPaper: joint.dia.Paper,
    operator: OperatorPredicate,
    cacheStatus?: OperatorResultCacheStatus
  ): void {
    const cacheText = JointUIService.getOperatorCacheDisplayText(operator, cacheStatus);
    const cacheIcon = JointUIService.getOperatorCacheIcon(operator, cacheStatus);

    const cacheIndicatorText = cacheText === "" ? "" : "cache";
    jointPaper.getModelById(operator.operatorID).attr(`.${operatorCacheTextClass}/text`, cacheIndicatorText);
    jointPaper.getModelById(operator.operatorID).attr(`.${operatorCacheIconClass}/xlink:href`, cacheIcon);
    jointPaper.getModelById(operator.operatorID).attr(`.${operatorCacheIconClass}/title`, cacheText);
  }

  public changeOperatorJointDisplayName(
    operator: OperatorPredicate,
    jointPaper: joint.dia.Paper,
    displayName: string
  ): void {
    jointPaper.getModelById(operator.operatorID).attr(`.${operatorNameClass}/text`, displayName);
    jointPaper.getModelById(operator.operatorID).attr(`.${operatorNameBGClass}/text`, displayName);
  }

  public getBreakpointButton(): new () => joint.linkTools.Button {
    return joint.linkTools.Button.extend({
      name: "info-button",
      options: {
        markup: [
          {
            tagName: "circle",
            selector: "info-button",
            attributes: {
              r: 10,
              fill: "#001DFF",
              cursor: "pointer",
            },
          },
          {
            tagName: "path",
            selector: "icon",
            attributes: {
              d: "M -2 4 2 4 M 0 3 0 0 M -2 -1 1 -1 M -1 -4 1 -4",
              fill: "none",
              stroke: "#FFFFFF",
              "stroke-width": 2,
              "pointer-events": "none",
            },
          },
        ],
        distance: 60,
        offset: 0,
        action: function (event: JQuery.Event, linkView: joint.dia.LinkView) {
          // when this button is clicked, it triggers an joint paper event
          if (linkView.paper) {
            linkView.paper.trigger("tool:breakpoint", linkView, event);
          }
        },
      },
    });
  }

  public getCommentElement(commentBox: CommentBox): joint.dia.Element {
    const basic = new joint.shapes.standard.Rectangle();
    if (commentBox.commentBoxPosition) basic.position(commentBox.commentBoxPosition.x, commentBox.commentBoxPosition.y);
    else basic.position(0, 0);
    basic.resize(120, 50);
    const commentElement = new TexeraCustomCommentElement({
      position: commentBox.commentBoxPosition || { x: 0, y: 0 },
      size: {
        width: JointUIService.DEFAULT_COMMENT_WIDTH,
        height: JointUIService.DEFAULT_COMMENT_HEIGHT,
      },
      attrs: JointUIService.getCustomCommentStyleAttrs(),
    });
    commentElement.set("id", commentBox.commentBoxID);
    return commentElement;
  }
  /**
   * This function converts a Texera source and target OperatorPort to
   *   a JointJS link cell <joint.dia.Link> that could be added to the JointJS.
   *
   * @param source the OperatorPort of the source of a link
   * @param target the OperatorPort of the target of a link
   * @returns JointJS Link Cell
   */
  public static getJointLinkCell(link: OperatorLink): joint.dia.Link {
    const jointLinkCell = JointUIService.getDefaultLinkCell();
    jointLinkCell.set("source", {
      id: link.source.operatorID,
      port: link.source.portID,
    });
    jointLinkCell.set("target", {
      id: link.target.operatorID,
      port: link.target.portID,
    });
    jointLinkCell.set("id", link.linkID);
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
        name: "manhattan",
      },
      connector: {
        name: "rounded",
      },
      toolMarkup: `<g class="link-tool">
          <g class="tool-remove" event="tool:remove">
          <circle r="11" />
            <path transform="scale(.8) translate(-16, -16)" d="M24.778,21.419 19.276,15.917 24.777
            10.415 21.949,7.585 16.447,13.087 10.945,7.585 8.117,10.415 13.618,15.917 8.116,21.419
            10.946,24.248 16.447,18.746 21.948,24.248z"/>
            <title>Remove link.</title>
           </g>
         </g>`,
      attrs: {
        ".connection": {
          stroke: linkPathStrokeColor,
          "stroke-width": "2px",
        },
        ".connection-wrap": {
          "stroke-width": "0px",
          // 'display': 'inline'
        },
        ".marker-source": {
          d: sourceOperatorHandle,
          stroke: "none",
          fill: "#919191",
        },
        ".marker-arrowhead-group-source .marker-arrowhead": {
          d: sourceOperatorHandle,
        },
        ".marker-target": {
          d: targetOperatorHandle,
          stroke: "none",
          fill: "#919191",
        },
        ".marker-arrowhead-group-target .marker-arrowhead": {
          d: targetOperatorHandle,
        },
        ".tool-remove": {
          fill: "#D8656A",
          width: 24,
          display: "none",
        },
        ".tool-remove path": {
          d: deleteButtonPath,
        },
        ".tool-remove circle": {},
      },
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
      ".port-body": {
        fill: "#A0A0A0",
        r: 5,
        stroke: "none",
      },
      ".port-label": {
        event: "input-label:evt",
        dblclick: "input-label:dbclick",
        pointerdblclick: "input-label:pointerdblclick",
      },
    };
    return portStyleAttrs;
  }

  /**
   * This function create a custom svg style for the operator
   * @returns the custom attributes of the tooltip.
   */
  public static getCustomOperatorStatusTooltipStyleAttrs(): joint.shapes.devs.ModelSelectors {
    const tooltipStyleAttrs = {
      "element-node": {
        style: { "pointer-events": "none" },
      },
      polygon: {
        fill: "#FFFFFF",
        "follow-scale": true,
        stroke: "purple",
        "stroke-width": "2",
        rx: "5px",
        ry: "5px",
        refPoints: "0,30 150,30 150,120 85,120 75,150 65,120 0,120",
        display: "none",
        style: { "pointer-events": "none" },
      },
      "#operatorCount": {
        fill: "#595959",
        "font-size": "12px",
        ref: "polygon",
        "y-alignment": "middle",
        "x-alignment": "left",
        "ref-x": 0.05,
        "ref-y": 0.2,
        display: "none",
        style: { "pointer-events": "none" },
      },
    };
    return tooltipStyleAttrs;
  }

  /**
   * This function creates a custom svg style for the operator.
   * This function also makes the delete button defined above to emit the delete event that will
   *   be captured by JointJS paper using event name *element:delete*
   *
   * @param operatorDisplayName the name of the operator that will display on the UI
   * @returns the custom attributes of the operator
   */
  public static getCustomOperatorStyleAttrs(
    operator: OperatorPredicate,
    operatorDisplayName: string,
    operatorType: string
  ): joint.shapes.devs.ModelSelectors {
    const operatorStyleAttrs = {
      ".texera-operator-coeditor-editing-background": {
        text: "",
        "font-size": "14px",
        "font-weight": "bold",
        stroke: "#f5f5f5",
        "stroke-width": "1em",
        visibility: "hidden",
        "ref-x": -50,
        "ref-y": 100,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "start",
      },
      ".texera-operator-coeditor-editing": {
        text: "",
        "font-size": "14px",
        "font-weight": "bold",
        visibility: "hidden",
        "ref-x": -50,
        "ref-y": 100,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "start",
      },
      ".texera-operator-coeditor-changed-property-background": {
        text: "",
        "font-size": "14px",
        "font-weight": "bold",
        stroke: "#f5f5f5",
        "stroke-width": "1em",
        visibility: "hidden",
        "ref-x": 0.5,
        "ref-y": 120,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "middle",
      },
      ".texera-operator-coeditor-changed-property": {
        text: "",
        "font-weight": "bold",
        "font-size": "14px",
        visibility: "hidden",
        "ref-x": 0.5,
        "ref-y": 120,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "middle",
      },
      ".texera-operator-state-background": {
        text: "",
        "font-size": "14px",
        stroke: "#f5f5f5",
        "stroke-width": "1em",
        visibility: "hidden",
        "ref-x": 0.5,
        "ref-y": 100,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "middle",
      },
      ".texera-operator-state": {
        text: "",
        "font-size": "14px",
        visibility: "hidden",
        "ref-x": 0.5,
        "ref-y": 100,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "middle",
      },
      ".texera-operator-abbreviated-count-background": {
        text: "",
        "font-size": "14px",
        stroke: "#f5f5f5",
        "stroke-width": "1em",
        visibility: "visible",
        "ref-x": 0.5,
        "ref-y": -30,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "middle",
      },
      ".texera-operator-abbreviated-count": {
        text: "",
        fill: "green",
        "font-size": "14px",
        visibility: "visible",
        "ref-x": 0.5,
        "ref-y": -30,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "middle",
      },
      ".texera-operator-processed-count-background": {
        text: "",
        "font-size": "14px",
        stroke: "#f5f5f5",
        "stroke-width": "1em",
        visibility: "hidden",
        "ref-x": 0.5,
        "ref-y": -50,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "middle",
      },
      ".texera-operator-processed-count": {
        text: "",
        fill: "green",
        "font-size": "14px",
        visibility: "hidden",
        "ref-x": 0.5,
        "ref-y": -50,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "middle",
      },
      ".texera-operator-output-count-background": {
        text: "",
        "font-size": "14px",
        stroke: "#f5f5f5",
        "stroke-width": "1em",
        visibility: "hidden",
        "ref-x": 0.5,
        "ref-y": -30,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "middle",
      },
      ".texera-operator-output-count": {
        text: "",
        fill: "green",
        "font-size": "14px",
        visibility: "hidden",
        "ref-x": 0.5,
        "ref-y": -30,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "middle",
      },
      "rect.body": {
        fill: JointUIService.getOperatorFillColor(operator),
        "follow-scale": true,
        stroke: "red",
        "stroke-width": "2",
        rx: "5px",
        ry: "5px",
      },
      "rect.boundary": {
        fill: "rgba(0, 0, 0, 0)",
        width: this.DEFAULT_OPERATOR_WIDTH + 20,
        height: this.DEFAULT_OPERATOR_HEIGHT + 20,
        ref: "rect.body",
        "ref-x": -10,
        "ref-y": -10,
      },
      "path.right-boundary": {
        ref: "rect.body",
        d: "M 20 80 C 0 60 0 20 20 0",
        stroke: "rgba(0,0,0,0)",
        "stroke-width": "10",
        fill: "transparent",
        "ref-x": 70,
        "ref-y": -10,
      },
      "path.left-boundary": {
        ref: "rect.body",
        d: "M 0 80 C 20 60 20 20 0 0",
        stroke: "rgba(0,0,0,0)",
        "stroke-width": "10",
        fill: "transparent",
        "ref-x": -30,
        "ref-y": -10,
      },
      ".texera-operator-name-background": {
        text: operatorDisplayName,
        "font-size": "14px",
        stroke: "#f5f5f5",
        "stroke-width": "1em",
        "ref-x": 0.5,
        "ref-y": 80,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "middle",
      },
      ".texera-operator-name": {
        text: operatorDisplayName,
        fill: "#595959",
        "font-size": "14px",
        "ref-x": 0.5,
        "ref-y": 80,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "middle",
      },
      ".delete-button": {
        x: 60,
        y: -20,
        cursor: "pointer",
        fill: "#D8656A",
        event: "element:delete",
        visibility: "hidden",
      },
      ".add-input-port-button": {
        x: -22,
        y: 40,
        cursor: "pointer",
        fill: "#565656",
        event: "element:add-input-port",
        visibility: "hidden",
      },
      ".remove-input-port-button": {
        x: -22,
        y: 60,
        cursor: "pointer",
        fill: "#565656",
        event: "element:remove-input-port",
        visibility: "hidden",
      },
      ".add-output-port-button": {
        x: 62,
        y: 40,
        cursor: "pointer",
        fill: "#565656",
        event: "element:add-output-port",
        visibility: "hidden",
      },
      ".remove-output-port-button": {
        x: 62,
        y: 60,
        cursor: "pointer",
        fill: "#565656",
        event: "element:remove-output-port",
        visibility: "hidden",
      },
      ".texera-operator-icon": {
        "xlink:href": "assets/operator_images/" + operatorType + ".png",
        width: 35,
        height: 35,
        "ref-x": 0.5,
        "ref-y": 0.5,
        ref: "rect.body",
        "x-alignment": "middle",
        "y-alignment": "middle",
      },
      ".texera-operator-result-cache-text": {
        text: JointUIService.getOperatorCacheDisplayText(operator) === "" ? "" : "cache",
        fill: "#595959",
        "font-size": "14px",
        visible: true,
        "ref-x": 80,
        "ref-y": 60,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "middle",
      },
      ".texera-operator-result-cache-icon": {
        "xlink:href": JointUIService.getOperatorCacheIcon(operator),
        title: JointUIService.getOperatorCacheDisplayText(operator),
        width: 40,
        height: 40,
        "ref-x": 75,
        "ref-y": 50,
        ref: "rect.body",
        "x-alignment": "middle",
        "y-alignment": "middle",
      },
    };
    return operatorStyleAttrs;
  }

  public static getOperatorFillColor(operator: OperatorPredicate): string {
    const isDisabled = operator.isDisabled ?? false;
    return isDisabled ? "#E0E0E0" : "#FFFFFF";
  }

  public static getOperatorCacheDisplayText(
    operator: OperatorPredicate,
    cacheStatus?: OperatorResultCacheStatus
  ): string {
    if (cacheStatus && cacheStatus !== "cache not enabled") {
      return cacheStatus;
    }
    const isCached = operator.isCached ?? false;
    return isCached ? "to be cached" : "";
  }

  public static getOperatorCacheIcon(operator: OperatorPredicate, cacheStatus?: OperatorResultCacheStatus): string {
    if (cacheStatus && cacheStatus !== "cache not enabled") {
      if (cacheStatus === "cache valid") {
        return "assets/svg/operator-result-cache-successful.svg";
      } else if (cacheStatus === "cache invalid") {
        return "assets/svg/operator-result-cache-invalid.svg";
      } else {
        const _exhaustiveCheck: never = cacheStatus;
        return "";
      }
    } else {
      const isCached = operator.isCached ?? false;
      if (isCached) {
        return "assets/svg/operator-result-cache-to-be-cached.svg";
      } else {
        return "";
      }
    }
  }

  /**
   * This function creates a custom svg style for the group.
   * This function also makes collapse button and expand button defined above to emit
   *   the collapse event and expand event that will be captured by JointJS paper
   *   using event names *element:collapse* and *element:expand*
   *
   * @param width width of the group (used to position the collapse button)
   * @returns the custom attributes of the group
   */
  public static getCustomGroupStyleAttrs(width: number): joint.shapes.devs.ModelSelectors {
    const groupStyleAttrs = {
      rect: {
        fill: "#F2F4F5",
        "follow-scale": true,
        stroke: "#CED4D9",
        "stroke-width": "2",
        rx: "5px",
        ry: "5px",
      },
      text: {
        fill: "#595959",
        "font-size": "16px",
        "ref-x": 15,
        "ref-y": 20,
        ref: "rect",
      },
      ".collapse-button": {
        x: width - 23,
        y: 6,
        cursor: "pointer",
        fill: "#728393",
        event: "element:collapse",
      },
      ".expand-button": {
        x: 147,
        y: 6,
        cursor: "pointer",
        fill: "#728393",
        event: "element:expand",
        display: "none",
      },
    };
    return groupStyleAttrs;
  }

  public static getCustomCommentStyleAttrs(): joint.shapes.devs.ModelSelectors {
    const commentStyleAttrs = {
      rect: {
        fill: "#F2F4F5",
        "follow-scale": true,
        stroke: "#CED4D9",
        "stroke-width": "0",
        rx: "5px",
        ry: "5px",
      },
      image: {
        "xlink:href": "assets/operator_images/icons8-chat_bubble.png",
        width: 32,
        height: 32,
        "ref-x": 0.5,
        "ref-y": 0.5,
        ref: "rect",
        "x-alignment": "middle",
        "y-alignment": "middle",
      },
      ".delete-button": {
        x: 22,
        y: -16,
        cursor: "pointer",
        fill: "#D8656A",
        event: "element:delete",
      },
    };
    return commentStyleAttrs;
  }

  public static getJointUserPointerCell(coeditor: Coeditor, position: Point, color: string): joint.dia.Element {
    const userCursor = new joint.shapes.standard.Circle({
      id: this.getJointUserPointerName(coeditor),
    });
    userCursor.resize(15, 15);
    userCursor.position(position.x, position.y);
    userCursor.attr("body/fill", color);
    userCursor.attr("body/stroke", color);
    userCursor.attr("text", {
      text: coeditor.name,
      "ref-x": 15,
      "ref-y": 20,
      stroke: coeditor.color,
    });
    return userCursor;
  }

  public static getJointUserPointerName(coeditor: Coeditor) {
    return "pointer_" + coeditor.clientId;
  }
}

export function fromJointPaperEvent<T extends keyof joint.dia.Paper.EventMap = keyof joint.dia.Paper.EventMap>(
  paper: joint.dia.Paper,
  eventName: T,
  context?: any
): Observable<Parameters<joint.dia.Paper.EventMap[T]>> {
  return fromEventPattern(
    handler => paper.on(eventName, handler, context), // addHandler
    (handler, signal) => paper.off(eventName as string, handler, context) // removeHandler
  );
}

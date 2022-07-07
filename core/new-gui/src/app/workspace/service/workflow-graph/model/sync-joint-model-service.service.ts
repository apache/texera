import {WorkflowGraph} from "./workflow-graph";
import {JointGraphWrapper} from "./joint-graph-wrapper";
import * as Y from "yjs";
import {OperatorPredicate, Point, YType} from "../../../types/workflow-common.interface";
import { Injectable } from "@angular/core";
import {JointUIService} from "../../joint-ui/joint-ui.service";
import {WorkflowActionService} from "./workflow-action.service";
import * as joint from "jointjs";
import {environment} from "../../../../../environments/environment";

@Injectable({
  providedIn: "root"
})
export class SyncJointModelService {
  private texeraGraph: WorkflowGraph;
  private jointGraph: joint.dia.Graph;
  private jointGraphWrapper: JointGraphWrapper;

  constructor(
    private workflowActionService: WorkflowActionService,
    private jointUIService: JointUIService
  ) {
    this.texeraGraph = workflowActionService.texeraGraph;
    this.jointGraph = workflowActionService.jointGraph;
    this.jointGraphWrapper = workflowActionService.jointGraphWrapper;
    this.texeraGraph.newYDocLoadedSubject.subscribe( _ =>
      this.handleOperatorAddAndDelete()
    );
  }


  /**
   * Reflects add and delete operator changes from TexeraGraph onto JointGraph.
   * @private
   */
  private handleOperatorAddAndDelete(): void {
    // A new key in the map means a new operator
    this.texeraGraph.operatorIDMap.observe((event: Y.YMapEvent<any>) => {
      // TODO: try grouping this!
      const jointElementsToAdd: joint.dia.Element[] = [];
      event.changes.keys.forEach((change, key) => {
        if (change.action === "add") {
          const newOperator = this.texeraGraph.operatorIDMap.get(key) as YType<OperatorPredicate>;
          // Also find its position
          if (this.texeraGraph.yOperatorPositionMap?.has(key)) {
            const newPos = this.texeraGraph.yOperatorPositionMap?.get(key) as Point;
            // Add the operator into joint graph
            const jointOperator = this.jointUIService.getJointOperatorElement(newOperator.toJSON(), newPos);
            jointElementsToAdd.push(jointOperator);
          } else {
            throw new Error(`operator with key ${key} does not exist in position map`);
          }
        }
        if (change.action === "delete") {
          this.jointGraph.getCell(key).remove();
        }
      });

      if (environment.asyncRenderingEnabled) {
        // Group add
        this.jointGraphWrapper.jointGraphContext.withContext({ async: true }, () => {
          this.jointGraph.addCells(jointElementsToAdd);
        });
      } else {
        // Add one by one
        for (let i = 0; i < jointElementsToAdd.length; i++) {
          this.jointGraph.addCell(jointElementsToAdd[i]);
        }
      }
    });
  }


}

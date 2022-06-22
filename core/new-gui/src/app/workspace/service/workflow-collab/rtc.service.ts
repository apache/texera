import { Injectable } from "@angular/core";
import {YModel} from "./y-model";
import {WorkflowActionService} from "../workflow-graph/model/workflow-action.service";
import * as Y from "yjs";
import {OperatorLink, OperatorPredicate, Point} from "../../types/workflow-common.interface";
import {Subscription} from "rxjs";

@Injectable({
  providedIn: "root"
})
export class RtcService {

  private currentYModel?: YModel;
  private observers: Subscription[] = [];

  constructor(private workflowActionService: WorkflowActionService) {
  }

  public setNewYModel(workflowId: number) {
    this.currentYModel = new YModel(workflowId);
    this.observeFromTexeraGraph();
    this.observeFromYModel();
  }

  public getCurrentYModel(): YModel | undefined {
    return this.currentYModel;
  }

  public destroyYModel(): void {
    this.observers.forEach(ob => {
      ob.unsubscribe();
    });
    this.observers = [];
    this.currentYModel?.destroy();
    this.currentYModel = undefined;
  }

  public observeFromTexeraGraph(): void {
    this.observers.push(
      this.workflowActionService.getTexeraGraph().getOperatorAddStream().subscribe(newOp => {
        this.currentYModel?.yOperatorIDMap?.set(newOp.operatorID, newOp);
        this.currentYModel?.yOperatorPositionMap?.set(newOp.operatorID,
          this.workflowActionService.getJointGraphWrapper().getElementPosition(newOp.operatorID));
      }),
      this.workflowActionService.getTexeraGraph().getOperatorDeleteStream().subscribe(deletedOpSubj => {
        this.currentYModel?.yOperatorIDMap?.delete(deletedOpSubj.deletedOperator.operatorID);
      }),
      this.workflowActionService.getJointGraphWrapper().getElementPositionChangeEvent().subscribe(element => {
        this.currentYModel?.yOperatorPositionMap?.set(element.elementID, element.newPosition);
      }),
      this.workflowActionService.getTexeraGraph().getLinkAddStream().subscribe(newLink => {
        this.currentYModel?.yOperatorLinkMap?.set(newLink.linkID, newLink);
      }),
      this.workflowActionService.getTexeraGraph().getLinkDeleteStream().subscribe(deletedLinkSubj =>{
        this.currentYModel?.yOperatorLinkMap?.delete(deletedLinkSubj.deletedLink.linkID);
      })
    );
  }

  public observeFromYModel(): void {
    this.currentYModel?.yOperatorIDMap?.observe((event: Y.YMapEvent<any>) => {
      console.log(event);
      if (!event.transaction.local) {
        event.changes.keys.forEach((change, key)=>{
          if (change.action === "add") {
            const newOperator = this.currentYModel?.yOperatorIDMap?.get(key) as OperatorPredicate;
            const syncedPosition = this.currentYModel?.yOperatorPositionMap?.get(newOperator.operatorID);
            this.workflowActionService.addOperator(newOperator, syncedPosition || {x: 0, y: 0});
          }
          if (change.action === "delete") {
            this.workflowActionService.deleteOperator(key);
          }
        });
      }
    });

    this.currentYModel?.yOperatorPositionMap?.observe((event: Y.YMapEvent<Point>) => {
      if (!event.transaction.local) {
        event.changes.keys.forEach((change, key)=> {
          if (change.action === "update") {
            const newPosition = this.currentYModel?.yOperatorPositionMap?.get(key);
            if (newPosition) this.workflowActionService.getJointGraphWrapper().setAbsolutePosition(key, newPosition.x, newPosition.y);
          }
        });
      }
    });

    this.currentYModel?.yOperatorLinkMap?.observe((event: Y.YMapEvent<OperatorLink>) => {
      if (!event.transaction.local) {
        event.changes.keys.forEach((change, key)=>{
          if (change.action === "add") {
            const newLink = this.currentYModel?.yOperatorLinkMap?.get(key) as OperatorLink;
            this.workflowActionService.addLink(newLink);
          }
          if (change.action === "delete") {
            this.workflowActionService.deleteLinkWithID(key);
          }
        });
      }
    });
  }
}

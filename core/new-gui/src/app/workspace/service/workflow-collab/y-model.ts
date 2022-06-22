import * as Y from "yjs";
import {WebsocketProvider} from "y-websocket";
import {YMap} from "yjs/dist/src/types/YMap";
import {OperatorLink, OperatorPredicate, Point} from "../../types/workflow-common.interface";
import {UntilDestroy} from "@ngneat/until-destroy";

@UntilDestroy()
export class YModel {
  public yDoc?: Y.Doc;
  public wsProvider?: WebsocketProvider;
  public readonly yOperatorIDMap?: YMap<OperatorPredicate>;
  public readonly yOperatorPositionMap?: YMap<Point>;
  public readonly yOperatorLinkMap?: YMap<OperatorLink>;

  constructor(workflowId: number) {
    this.yDoc = new Y.Doc();
    this.wsProvider = new WebsocketProvider("ws://localhost:1234", `workflow-${workflowId}`, this.yDoc);
    this.yOperatorIDMap = this.yDoc.getMap("yOperatorIDMap");
    this.yOperatorPositionMap = this.yDoc.getMap("yOperatorPositionMap");
    this.yOperatorLinkMap = this.yDoc.getMap("yOperatorLinkMap");
  }

  public destroy(): void {
    this.wsProvider?.disconnect();
    this.yDoc?.destroy();
  }
}

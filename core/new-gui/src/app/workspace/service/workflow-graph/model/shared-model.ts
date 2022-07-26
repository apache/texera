import * as Y from "yjs";
import {WebsocketProvider} from "y-websocket";
import {Awareness} from "y-protocols/awareness";
import {
  Breakpoint,
  CommentBox,
  OperatorLink,
  OperatorPredicate,
  Point,
  YType
} from "../../../types/workflow-common.interface";
import {User, UserState} from "../../../../common/type/user";

export class SharedModel {
  public yDoc: Y.Doc = new Y.Doc();
  public wsProvider: WebsocketProvider;
  public awareness: Awareness;
  public operatorIDMap: Y.Map<YType<OperatorPredicate>>;
  public commentBoxMap: Y.Map<YType<CommentBox>>;
  public operatorLinkMap: Y.Map<OperatorLink>;
  public elementPositionMap: Y.Map<Point>;
  public linkBreakpointMap: Y.Map<Breakpoint>;
  public undoManager: Y.UndoManager;

  constructor(private wid?: number,
              private user?: User) {
    this.operatorIDMap = this.yDoc.getMap("operatorIDMap");
    this.commentBoxMap = this.yDoc.getMap("commentBoxMap");
    this.operatorLinkMap = this.yDoc.getMap("operatorLinkMap");
    this.elementPositionMap = this.yDoc.getMap("elementPositionMap");
    this.linkBreakpointMap = this.yDoc.getMap("linkBreakPointMap");
    this.undoManager = new Y.UndoManager([
      this.operatorIDMap,
      this.elementPositionMap,
      this.operatorLinkMap,
      this.commentBoxMap,
      this.linkBreakpointMap
    ]);
    this.wsProvider = new WebsocketProvider("ws://localhost:1234", `${wid}`, this.yDoc);
    if (!wid) this.wsProvider.disconnect();
    this.awareness = this.wsProvider.awareness;
    if (this.user) {
      const userState: UserState = {
        user: this.user,
        clientID: this.awareness.clientID,
        isActive: true,
        userCursor: {x: 0, y: 0}
      };
      this.awareness.setLocalState(userState);
    }
  }

  public updateAwareness(field: string, value: any): void {
    this.awareness.setLocalStateField(field, value);
  }
}

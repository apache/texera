import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";
import { Awareness } from "y-protocols/awareness";
import { CommentBox, OperatorLink, OperatorPredicate, Point } from "../../../types/workflow-common.interface";
import { User, CoeditorState } from "../../../../common/type/user";
import { getWebsocketUrl } from "../../../../common/util/url";
import { v4 as uuid } from "uuid";
import { YType } from "../../../types/shared-editing.interface";
import { environment } from "../../../../../environments/environment";

/**
 * SharedModel encapsulates everything related to real-time shared editing for the current workflow.
 * Most of the yjs-related implementations are within this class.
 */
export class SharedModel {
  public yDoc: Y.Doc = new Y.Doc();
  public wsProvider: WebsocketProvider;
  public awareness: Awareness;
  public operatorIDMap: Y.Map<YType<OperatorPredicate>>;
  public commentBoxMap: Y.Map<YType<CommentBox>>;
  public operatorLinkMap: Y.Map<OperatorLink>;
  public elementPositionMap: Y.Map<Point>;
  public undoManager: Y.UndoManager;
  public clientId: string;

  /**
   * Initializes yjs-related structures and join the shared-editing room. A room number is required for initialization.
   * When wid is present, it will be used as part of the room number to enable shared editing.
   * When no wid is provided (new workflow canvas, landing page, etc.), a random room number will be assigned so that
   * users don't interfere with each other.
   * @param wid workflow ID number, used as part of the address for the shared-editing room.
   * @param user current (local) user info, used for initializing local awareness (user presence).
   */
  constructor(
    public wid?: number,
    public user?: User
  ) {
    // Initialize Y-structures.
    this.operatorIDMap = this.yDoc.getMap("operatorIDMap");
    this.commentBoxMap = this.yDoc.getMap("commentBoxMap");
    this.operatorLinkMap = this.yDoc.getMap("operatorLinkMap");
    this.elementPositionMap = this.yDoc.getMap("elementPositionMap");

    // Initialize Y-undo manager by aggregating intended  Y-structures. Only structures included here will be undoable.
    this.undoManager = new Y.UndoManager(
      [this.operatorIDMap, this.elementPositionMap, this.operatorLinkMap, this.commentBoxMap],
      {
        captureTimeout: 100,
      }
    );

    // Generate editing room number.
    const websocketUrl = SharedModel.getYWebSocketBaseUrl();
    const suffix = wid ? `${wid}` : uuid();
    this.wsProvider = new WebsocketProvider(websocketUrl, suffix, this.yDoc);

    // Initialize local user awareness information.
    this.awareness = this.wsProvider.awareness;
    this.clientId = this.awareness.clientID.toString();
    if (this.user) {
      const userState: CoeditorState = {
        user: { ...this.user, clientId: this.clientId },
        isActive: true,
        userCursor: { x: 0, y: 0 },
      };
      this.awareness.setLocalState(userState);
    }
  }

  /**
   * Shared editing needs y-websocket to be running. The base url depends on whether reverse proxy is set up. For local
   * development, we need to use localhost; For production server which has reverse proxy, we can use the same base url
   * as the server.
   * @private
   */
  private static getYWebSocketBaseUrl() {
    return environment.productionSharedEditingServer ? getWebsocketUrl("rtc", "") : "ws://localhost:1234";
  }

  /**
   * Updates a particular field of local awareness state info. Will only execute update when user info is provided.
   * @param field the name of the particular state info.
   * @param value the updated state info.
   */
  public updateAwareness<K extends keyof CoeditorState>(field: K, value: CoeditorState[K]): void {
    if (this.user) this.awareness.setLocalStateField(field, value);
  }

  /**
   * Groups a bunch of actions into one atomic transaction, so that they can be undone/redone in one call.
   * @param callback Put whatever need to be atomically done within this callback function.
   */
  public transact(callback: Function) {
    this.yDoc.transact(() => callback());
  }

  /**
   * Destroys internal structures related to Yjs and quit the editing room.
   */
  public destroy(): void {
    this.awareness.destroy();
    try {
      if (this.wsProvider.shouldConnect && this.wsProvider.wsconnected) this.wsProvider.disconnect();
    } catch (e) {}
    this.yDoc.destroy();
  }
}

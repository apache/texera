/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";
import { Awareness } from "y-protocols/awareness";
import {
  BreakpointInfo,
  CommentBox,
  OperatorLink,
  OperatorPredicate,
  Point,
} from "../../../types/workflow-common.interface";
import { CoeditorState, User } from "../../../../common/type/user";
import { getWebsocketUrl } from "../../../../common/util/url";
import { YType } from "../../../types/shared-editing.interface";

/**
 * SharedModel encapsulates everything related to real-time shared editing for the current workflow.
 * Most of the yjs-related implementations are within this class.
 */
export class SharedModel {
  public yDoc: Y.Doc = new Y.Doc();
  public wsProvider?: WebsocketProvider;
  public awareness: Awareness;
  public operatorIDMap: Y.Map<YType<OperatorPredicate>>;
  public commentBoxMap: Y.Map<YType<CommentBox>>;
  public operatorLinkMap: Y.Map<OperatorLink>;
  public elementPositionMap: Y.Map<Point>;
  public debugState: Y.Map<Y.Map<BreakpointInfo>>;
  public undoManager: Y.UndoManager;
  public clientId: string;

  /**
   * Initializes yjs-related structures and lazily joins the shared-editing room. A shared-editing `/rtc` connection is
   * opened over the network only when a workflow ID ({@link wid}) is provided: its presence is the signal that there is
   * a real, saved workflow to collaborate on. When no wid is provided (throwaway validation/compilation graphs, a new
   * unsaved canvas, the landing page, etc.) the model stays purely local — no {@link WebsocketProvider}/`/rtc` socket is
   * opened and a standalone {@link Awareness} is used. This lazy gating is what prevents short-lived graphs from leaking
   * self-reconnecting `/rtc` connections.
   * @param wid workflow ID number, used as the shared-editing room address. Its presence is what enables shared editing.
   * @param user current (local) user info, used for initializing local awareness (user presence).
   * @param productionSharedEditingServer whether to use production shared editing server
   */
  constructor(
    public wid?: number,
    public user?: User,
    private productionSharedEditingServer?: boolean
  ) {
    // Initialize Y-structures.
    this.debugState = this.yDoc.getMap("debugActions");
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

    if (this.wid !== undefined) {
      // A workflow ID is present: join the shared-editing room for that workflow over the network.
      const websocketUrl = this.getYWebSocketBaseUrl();
      this.wsProvider = new WebsocketProvider(websocketUrl, `${this.wid}`, this.yDoc);
      // Initialize local user awareness information from the provider's awareness.
      this.awareness = this.wsProvider.awareness;
    } else {
      // No workflow ID: stay local-only, no network. A standalone awareness keeps clientId/awareness reads valid
      // downstream.
      this.awareness = new Awareness(this.yDoc);
    }
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
  private getYWebSocketBaseUrl() {
    return getWebsocketUrl("rtc", "");
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
    try {
      // Fully tear down the provider: destroy() clears the _checkInterval/_resyncInterval reconnect timers,
      // removes the beforeunload handler, broadcasts awareness removal (peers drop our cursor), and closes the
      // socket. Calling disconnect() alone (the previous behavior) leaves the reconnect timer running, so the
      // provider is never garbage-collected and keeps re-opening a zombie /rtc socket for the life of the tab.
      this.wsProvider?.destroy();
    } catch (e) {}
    this.awareness.destroy();
    this.yDoc.destroy();
  }
}

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

import { Injectable } from "@angular/core";
import { webSocket, WebSocketSubject } from "rxjs/webSocket";
import { interval, Observable, Subject, Subscription, timer } from "rxjs";
import { delayWhen, filter, map, retryWhen, tap } from "rxjs/operators";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { getWebsocketUrl } from "../../../common/util/url";

export type PythonWSRequestTypeMap = {
  CreateSessionRequest: {}; // NEW
  HeartBeatRequest: {};
  ChatUserMessageRequest: {
    message: string;
  };
  OperatorSchemaResponse: { schema: string; requestId: string };
  AddOperatorAndLinksResponse: { status: string; requestId: string };
};

export type PythonWSEventTypeMap = {
  CreateSessionResponse: { sessionId: string }; // NEW
  HeartBeatResponse: {};
  ChatStreamResponseEvent: {
    delta: string;
  };
  ChatStreamResponseComplete: {};

  getOperatorSchema: { operatorType: string; requestId: string };
  addOperatorAndLinks: {
    operatorAndPosition: any;
    links: any[];
    requestId: string;
  };
};

// helper type definitions to generate the request and event types
type ValueOf<T> = T[keyof T];
type CustomUnionType<T> = ValueOf<{
  [P in keyof T]: {
    type: P;
  } & T[P];
}>;

export type PythonWSRequestTypes = keyof PythonWSRequestTypeMap;
export type PythonWSRequest = CustomUnionType<PythonWSRequestTypeMap>;

export type PythonWSEventTypes = keyof PythonWSEventTypeMap;
export type PythonWSEvent = CustomUnionType<PythonWSEventTypeMap>;

export const WS_HEARTBEAT_INTERVAL_MS = 10000;
export const WS_RECONNECT_INTERVAL_MS = 3000;

@Injectable({
  providedIn: "root",
})
export class ChatAssistantWebsocketService {
  private static readonly WS_ENDPOINT = "chat-assistant";
  private websocket?: WebSocketSubject<PythonWSEvent | PythonWSRequest>;
  private wsWithReconnectSubscription?: Subscription;
  private webSocketResponseSubject: Subject<PythonWSEvent> = new Subject<PythonWSEvent>();

  private sessionId?: string; // ← NEW
  public isConnected = false;

  constructor(
    private operatorMetadataService: OperatorMetadataService,
    private workflowActionService: WorkflowActionService
  ) {
    // Send heartbeat periodically
    interval(WS_HEARTBEAT_INTERVAL_MS).subscribe(() => {
      this.send("HeartBeatRequest", {});
    });
    this.subscribeToEvent("CreateSessionResponse").subscribe(evt => {
      console.log("New sessionId:", evt.sessionId);
      this.sessionId = evt.sessionId;
    });
    this.subscribeToEvent("HeartBeatResponse").subscribe(heartBeatResponse => console.log(heartBeatResponse));
    this.subscribeToEvent("getOperatorSchema").subscribe(getOperatorSchemaRequest => {
      this.handleGetOperatorSchema(getOperatorSchemaRequest);
    });
    this.subscribeToEvent("addOperatorAndLinks").subscribe(evt => this.handleAddOperatorAndLinks(evt));
  }

  public websocketEvent(): Observable<PythonWSEvent> {
    return this.webSocketResponseSubject;
  }

  // Subscribe to a particular type of event
  public subscribeToEvent<T extends PythonWSEventTypes>(type: T): Observable<{ type: T } & PythonWSEventTypeMap[T]> {
    return this.websocketEvent().pipe(
      filter(event => event.type === type),
      map(event => event as { type: T } & PythonWSEventTypeMap[T])
    );
  }

  // Send a request to the Python server
  public send<T extends PythonWSRequestTypes>(type: T, payload: PythonWSRequestTypeMap[T]): void {
    if (!this.websocket) {
      console.warn("WebSocket not connected; cannot send request:", payload);
      return;
    }

    // Only attach the sessionId for per-chat requests
    const needsSid = !["CreateSessionRequest", "HeartBeatRequest"].includes(type);
    const augmented = needsSid && this.sessionId ? { sessionId: this.sessionId, ...payload } : payload;

    const request = { type, ...augmented } as any as PythonWSRequest;
    console.log("Sending", request);
    this.websocket.next(request);
  }

  public closeWebsocket(): void {
    this.wsWithReconnectSubscription?.unsubscribe();
    this.websocket?.complete();
  }

  public openWebsocket(): void {
    const websocketUrl = getWebsocketUrl(ChatAssistantWebsocketService.WS_ENDPOINT, "");
    this.websocket = webSocket<PythonWSEvent | PythonWSRequest>(websocketUrl);

    // ask server for a logical chat session
    this.send("CreateSessionRequest", {}); // ← NEW

    // setup reconnection logic
    const wsWithReconnect = this.websocket.pipe(
      retryWhen(errors =>
        errors.pipe(
          tap((_: unknown) => (this.isConnected = false)), // update connection status
          tap((_: unknown) =>
            console.log(`websocket connection lost, reconnecting in ${WS_RECONNECT_INTERVAL_MS / 1000} seconds`)
          ),
          delayWhen(_ => timer(WS_RECONNECT_INTERVAL_MS)), // reconnect after delay
          tap((_: unknown) => {
            this.send("HeartBeatRequest", {}); // try to send heartbeat immediately after reconnect
          })
        )
      )
    );
    // set up event listener on re-connectable websocket observable
    // @ts-ignore
    this.wsWithReconnectSubscription = wsWithReconnect.subscribe({
      next: event => {
        this.webSocketResponseSubject.next(event as PythonWSEvent);
        this.isConnected = true;
      },
      error: (err: unknown) => {
        console.error("WebSocket error:", err);
        this.isConnected = false;
      },
      complete: (_: unknown) => {
        console.log("WebSocket connection closed.");
        this.isConnected = false;
      },
    });
  }

  /**
   * Handles incoming "getOperatorSchema" requests from the Python server.
   * Retrieves the operator schema from local data or an API and sends it back.
   */
  private handleGetOperatorSchema(event: { type: "getOperatorSchema"; operatorType: string; requestId: string }): void {
    console.log("Received getOperatorSchema request for:", event.operatorType);
    // Replace the following with your actual logic to retrieve the operator schema.
    const operatorSchema = this.getOperatorSchema(event.operatorType);
    // Send the response back to the Python server.
    const response = {
      schema: operatorSchema,
      requestId: event.requestId,
    };
    // Use next() to send the message over the WebSocket.
    this.send("OperatorSchemaResponse", response);
  }

  /**
   * Retrieve operator schema.
   * Replace this with a call to your real data if needed.
   */
  private getOperatorSchema(operatorType: string): string {
    if (!this.operatorMetadataService.operatorTypeExists(operatorType)) {
      return "operatorType does not exist in Texera, please generate operator again";
    }
    return JSON.stringify(this.operatorMetadataService.getOperatorSchema(operatorType));
  }

  private handleAddOperatorAndLinks(evt: {
    type: "addOperatorAndLinks";
    operatorAndPosition: any;
    links: any[];
    requestId: string;
  }) {
    let status = "success";
    try {
      this.workflowActionService.addOperatorsAndLinks([evt.operatorAndPosition], evt.links);
      this.workflowActionService.autoLayoutWorkflow();
    } catch (e) {
      console.error(e);
      status = String(e);
    }
    this.send("AddOperatorAndLinksResponse", {
      requestId: evt.requestId,
      status,
    });
  }
}

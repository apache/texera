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
import { BehaviorSubject, interval, Observable, Subject, Subscription, timer } from "rxjs";
import { webSocket, WebSocketSubject } from "rxjs/webSocket";
import {
  TexeraWebsocketEvent,
  TexeraWebsocketEventTypeMap,
  TexeraWebsocketEventTypes,
  TexeraWebsocketRequest,
  TexeraWebsocketRequestTypeMap,
  TexeraWebsocketRequestTypes,
} from "../../types/workflow-websocket.interface";
import { delayWhen, filter, map, retryWhen, tap } from "rxjs/operators";
import { environment } from "../../../../environments/environment";
import { AuthService } from "../../../common/service/user/auth.service";
import { getWebsocketUrl } from "src/app/common/util/url";
import { isDefined } from "../../../common/util/predicate";

export const WS_HEARTBEAT_INTERVAL_MS = 10000;
export const WS_RECONNECT_INTERVAL_MS = 3000;

@Injectable({
  providedIn: "root",
})
export class WorkflowWebsocketService {
  private static readonly TEXERA_WEBSOCKET_ENDPOINT = "wsapi/workflow-websocket";

  public numWorkers: number = -1;

  private websocket?: WebSocketSubject<TexeraWebsocketEvent | TexeraWebsocketRequest>;
  private wsWithReconnectSubscription?: Subscription;
  private readonly webSocketResponseSubject: Subject<TexeraWebsocketEvent> = new Subject();
  private readonly connectionStatusSubject = new BehaviorSubject<boolean>(false);

  constructor() {
    // setup heartbeat
    interval(WS_HEARTBEAT_INTERVAL_MS).subscribe(_ => this.send("HeartBeatRequest", {}));
  }

  /** Emit `true` when the socket is up, `false` when it is closed or lost. */
  public getConnectionStatusStream(): Observable<boolean> {
    return this.connectionStatusSubject.asObservable();
  }

  public websocketEvent(): Observable<TexeraWebsocketEvent> {
    return this.webSocketResponseSubject;
  }

  /**
   * Subscribe to a particular type of workflow websocket event
   */
  public subscribeToEvent<T extends TexeraWebsocketEventTypes>(
    type: T
  ): Observable<{ type: T } & TexeraWebsocketEventTypeMap[T]> {
    return this.websocketEvent().pipe(
      filter(event => event.type === type),
      map(event => event as { type: T } & TexeraWebsocketEventTypeMap[T])
    );
  }

  public send<T extends TexeraWebsocketRequestTypes>(type: T, payload: TexeraWebsocketRequestTypeMap[T]): void {
    const request = {
      type,
      ...payload,
    } as any as TexeraWebsocketRequest;
    this.websocket?.next(request);
  }

  public get isConnected(): boolean {
    return this.connectionStatusSubject.value;
  }

  public closeWebsocket() {
    this.wsWithReconnectSubscription?.unsubscribe();
    this.websocket?.complete();
    this.updateConnectionStatus(false);
  }

  public openWebsocket(wId: number, uId?: number, cuId?: number) {
    this.closeWebsocket();

    if (uId == undefined) {
      console.log(`uId is ${uId}, defaulting to uId = 1`);
      uId = 1;
    }
    const websocketUrl =
      getWebsocketUrl(WorkflowWebsocketService.TEXERA_WEBSOCKET_ENDPOINT, "") +
      "?wid=" +
      wId +
      "&uid=" +
      uId +
      (isDefined(cuId) ? `&cuid=${cuId}` : "") +
      (environment.userSystemEnabled && AuthService.getAccessToken() !== null
        ? "&access-token=" + AuthService.getAccessToken()
        : "");
    console.log("websocketUrl", websocketUrl);
    this.websocket = webSocket<TexeraWebsocketEvent | TexeraWebsocketRequest>(websocketUrl);
    // setup reconnection logic
    const wsWithReconnect = this.websocket.pipe(
      retryWhen(errors =>
        errors.pipe(
          tap(_ => this.updateConnectionStatus(false)), // update connection status
          tap(_ =>
            console.log(`websocket connection lost, reconnecting in ${WS_RECONNECT_INTERVAL_MS / 1000} seconds`)
          ),
          delayWhen(_ => timer(WS_RECONNECT_INTERVAL_MS)), // reconnect after delay
          tap(_ => {
            this.send("HeartBeatRequest", {}); // try to send heartbeat immediately after reconnect
          })
        )
      )
    );
    // set up event listener on re-connectable websocket observable
    this.wsWithReconnectSubscription = wsWithReconnect.subscribe(event =>
      this.webSocketResponseSubject.next(event as TexeraWebsocketEvent)
    );

    // refresh connection status
    this.websocketEvent().subscribe(evt => {
      if (evt.type === "ClusterStatusUpdateEvent") {
        this.numWorkers = evt.numWorkers;
      }
      this.updateConnectionStatus(true);
    });
  }

  private updateConnectionStatus(connected: boolean) {
    if (this.isConnected !== connected) {
      this.connectionStatusSubject.next(connected);
    }
  }
}

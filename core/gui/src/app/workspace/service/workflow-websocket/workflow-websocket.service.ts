import { Injectable } from "@angular/core";
import { interval, Observable, Subject, Subscription, timer } from "rxjs";
import { webSocket, WebSocketSubject } from "rxjs/webSocket";
import {
  DebugCommandRequest,
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
import { ExecutionState } from "../../types/execute-workflow.interface";

export const WS_HEARTBEAT_INTERVAL_MS = 10000;
export const WS_RECONNECT_INTERVAL_MS = 3000;

@Injectable({
  providedIn: "root",
})
export class WorkflowWebsocketService {
  private static readonly TEXERA_WEBSOCKET_ENDPOINT = "wsapi/workflow-websocket";

  public isConnected: boolean = false;
  public numWorkers: number = -1;

  private websocket?: WebSocketSubject<TexeraWebsocketEvent | TexeraWebsocketRequest>;
  private wsWithReconnectSubscription?: Subscription;
  private readonly webSocketResponseSubject: Subject<TexeraWebsocketEvent> = new Subject();
  private requestQueue: Array<DebugCommandRequest> = [];
  private assignedWorkerIds: Map<string, readonly string[]> = new Map();
  public executionInitiator = false;

  constructor() {
    // setup heartbeat
    interval(WS_HEARTBEAT_INTERVAL_MS).subscribe(_ => this.send("HeartBeatRequest", {}));
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
    if(request.type === "WorkflowExecuteRequest"){
      this.executionInitiator = true;
    }
    this.websocket?.next(request);
  }

  public closeWebsocket() {
    this.wsWithReconnectSubscription?.unsubscribe();
    this.websocket?.complete();
  }

  public openWebsocket(wId: number) {
    const websocketUrl =
      getWebsocketUrl(WorkflowWebsocketService.TEXERA_WEBSOCKET_ENDPOINT, "") +
      "?wid=" +
      wId +
      (environment.userSystemEnabled && AuthService.getAccessToken() !== null
        ? "&access-token=" + AuthService.getAccessToken()
        : "");
    this.websocket = webSocket<TexeraWebsocketEvent | TexeraWebsocketRequest>(websocketUrl);
    // setup reconnection logic
    const wsWithReconnect = this.websocket.pipe(
      retryWhen(errors =>
        errors.pipe(
          tap(_ => (this.isConnected = false)), // update connection status
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
      if(evt.type === "WorkflowStateEvent"){
        if(evt.state === ExecutionState.Completed || evt.state === ExecutionState.Killed || evt.state === ExecutionState.Failed){
          this.executionInitiator = false;
        }
      }
      this.isConnected = true;
    });
  }

  public reopenWebsocket(wId: number) {
    this.closeWebsocket();
    this.openWebsocket(wId);
  }


  public clearDebugCommands(){
    this.requestQueue = [];
  }



  public sendDebugCommand(payload:DebugCommandRequest){
    this.requestQueue.push(payload);
    this.processQueue();
  }

  private sendDebugCommandRequest( cmd: DebugCommandRequest): void {

    console.log("sending", cmd);
    this.send("DebugCommandRequest", cmd);
  }

  private processQueue(): void {
    // Process the request queue
    let initialQueueLength = this.requestQueue.length;

    // Loop through the initial length of the queue to prevent infinite loops with continuously failing items
    for (let i = 0; i < initialQueueLength; i++) {
      const request = this.requestQueue.shift();
      if (request) {
          this.sendDebugCommandRequest(request);
      }
    }
  }
}

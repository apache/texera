import { Injectable } from "@angular/core";
import { interval, Observable, Subject, timer } from "rxjs";
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
import { UserService } from "../../../common/service/user/user.service";

export const WS_HEARTBEAT_INTERVAL_MS = 10000;
export const WS_RECONNECT_INTERVAL_MS = 3000;

@Injectable({
  providedIn: "root",
})
export class WorkflowWebsocketService {
  private static readonly TEXERA_WEBSOCKET_ENDPOINT = "wsapi/workflow-websocket";

  public isConnected: boolean = false;

  private websocket?: WebSocketSubject<TexeraWebsocketEvent | TexeraWebsocketRequest>;
  private readonly webSocketResponseSubject: Subject<TexeraWebsocketEvent> = new Subject();

  constructor(private userService: UserService) {
    this.startWebsocket();

    // set up heartbeat
    interval(WS_HEARTBEAT_INTERVAL_MS).subscribe(_ => this.send("HeartBeatRequest", {}));

    // refresh connection status
    this.websocketEvent().subscribe(_ => (this.isConnected = true));

    // send hello world
    this.send("HelloWorldRequest", { message: "Texera on Amber" });

    if (environment.userSystemEnabled) {
      this.registerRestartWebsocketUponUserChanges();
    }
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

  public reStartWebsocket() {
    this.websocket?.complete();
    this.startWebsocket();
  }

  private startWebsocket() {
    this.websocket = webSocket<TexeraWebsocketEvent | TexeraWebsocketRequest>(
      WorkflowWebsocketService.getWorkflowWebsocketUrl() +
        (UserService.getAccessToken() === null ? "" : "?token=" + UserService.getAccessToken())
    );
    // setup reconnection logic
    const wsWithReconnect = this.websocket.pipe(
      retryWhen(error =>
        error.pipe(
          tap(_ => (this.isConnected = false)), // update connection status
          tap(_ =>
            console.log(`websocket connection lost, reconnecting in ${WS_RECONNECT_INTERVAL_MS / 1000} seconds`)
          ),
          delayWhen(_ => timer(WS_RECONNECT_INTERVAL_MS)), // reconnect after delay
          tap(
            _ => this.send("HeartBeatRequest", {}) // try to send heartbeat immediately after reconnect
          )
        )
      )
    );

    // set up event listener on re-connectable websocket observable
    wsWithReconnect.subscribe(event => this.webSocketResponseSubject.next(event as TexeraWebsocketEvent));
  }

  private registerRestartWebsocketUponUserChanges() {
    this.userService.userChanged().subscribe(() => this.reStartWebsocket());
  }

  public static getWorkflowWebsocketUrl(): string {
    const websocketUrl = new URL(WorkflowWebsocketService.TEXERA_WEBSOCKET_ENDPOINT, document.baseURI);
    // replace protocol, so that http -> ws, https -> wss
    websocketUrl.protocol = websocketUrl.protocol.replace("http", "ws");
    return websocketUrl.toString();
  }
}

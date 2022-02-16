import { Injectable } from "@angular/core";
import { webSocket, WebSocketSubject } from "rxjs/webSocket";
import { environment } from "../../../../environments/environment";
import { Subject, Observable, timer, Subscription, interval, ReplaySubject } from "rxjs";
import { getWebsocketUrl } from "src/app/common/util/url";
import { AuthService } from "src/app/common/service/user/auth.service";
import { delayWhen, filter, map, retryWhen, tap } from "rxjs/operators";
import {
  CollabWebsocketEvent,
  CollabWebsocketRequest,
  CollabWebsocketEventTypes,
  CollabWebsocketEventTypeMap,
  CollabWebsocketRequestTypes,
  CollabWebsocketRequestTypeMap,
} from "../../types/collab-websocket.interface";
import { CommandMessage } from "../../types/command.interface";

export const WS_HEARTBEAT_INTERVAL_MS = 10000;
export const WS_RECONNECT_INTERVAL_MS = 3000;

/**
 *
 * WorkflowCollabService manages core functionalities related to workflow collaboration. It will only work when the user
 * system is enabled. For now it supports locking the currently-editing workflow so that its changes can be seen by other
 * clients of the same workflow, but cannot edit it.
 */

@Injectable({
  providedIn: "root",
})
export class WorkflowCollabService {
  private static readonly TEXERA_COLLAB_ENDPOINT = "wsapi/collab";

  private propagationEnabled: boolean = false; // Only pertains to commandMessage propagation
  private lockGranted: boolean = true;
  private connected: boolean = false; // Might be used in the future
  private workflowReadonly: boolean = false; // Whether this workflow is shared from other users and is read only.

  private websocket?: WebSocketSubject<CollabWebsocketEvent | CollabWebsocketRequest>;
  private wsWithReconnectSubscription?: Subscription;

  private readonly webSocketEventSubject: Subject<CollabWebsocketEvent> = new Subject();
  private readonly commandMessageSubject: Subject<CommandMessage> = new Subject<CommandMessage>();
  private readonly lockGrantedSubject: ReplaySubject<boolean> = new ReplaySubject(1);
  private readonly restoreVersionSubject: ReplaySubject<boolean> = new ReplaySubject(1);
  private readonly workflowAccessSubject: ReplaySubject<boolean> = new ReplaySubject(1);

  constructor() {
    // In case collab is not enabled, lock should always be granted.
    this.setLockStatus(true);

    if (this.isCollabEnabled()) {
      this.setPropagationEnabled(true);
      interval(WS_HEARTBEAT_INTERVAL_MS).subscribe(_ => this.send("HeartBeatRequest", {}));

      this.subscribeToEvent("CommandEvent").subscribe(commandEvent => {
        const message = JSON.parse(commandEvent.commandMessage) as CommandMessage;
        this.commandMessageSubject.next(message);
      });

      this.subscribeToEvent("LockGrantedEvent").subscribe(_ => {
        this.setLockStatus(true);
      });

      this.subscribeToEvent("LockRejectedEvent").subscribe(_ => {
        this.setLockStatus(false);
      });

      this.subscribeToEvent("ReleaseLockEvent").subscribe(_ => {
        this.setLockStatus(false);
      });

      this.subscribeToEvent("RestoreVersionEvent").subscribe(_ => {
        this.restoreVersionSubject.next(true);
      });

      this.subscribeToEvent("WorkflowAccessEvent").subscribe(WorkflowAccessEvent => {
        this.setWorkflowAccess(WorkflowAccessEvent.workflowReadonly);
      });
    }
  }

  /**
   * Gets whether to use service and connect to the endpoint.
   */
  public isCollabEnabled(): boolean {
    return environment.workflowCollabEnabled;
  }

  /**
   * Opens the websocket and connects to the server ws endpoint. Must be used with a wId.
   */
  public openWebsocket(wId: number) {
    if (this.isCollabEnabled()) {
      this.setLockStatus(true);
      const websocketUrl =
        getWebsocketUrl(WorkflowCollabService.TEXERA_COLLAB_ENDPOINT) +
        (environment.userSystemEnabled && AuthService.getAccessToken() !== null
          ? "?access-token=" + AuthService.getAccessToken()
          : "");
      this.websocket = webSocket<CollabWebsocketEvent | CollabWebsocketRequest>(websocketUrl);

      // setup reconnection logic
      const wsWithReconnect = this.websocket.pipe(
        retryWhen(errors =>
          errors.pipe(
            tap(_ => (this.connected = false)), // update connection status
            tap(_ =>
              console.log(`websocket connection lost, reconnecting in ${WS_RECONNECT_INTERVAL_MS / 1000} seconds`)
            ),
            delayWhen(_ => timer(WS_RECONNECT_INTERVAL_MS)), // reconnect after delay
            tap(_ => {
              this.send("WIdRequest", { wId }); // Informs the websocket server of this client's wid
              this.send("HeartBeatRequest", {}); // try to send heartbeat immediately after reconnect
              this.send("TryLockRequest", {}); // Asks server about lock status
            })
          )
        )
      );

      // set up event listener on re-connectable websocket observable
      this.wsWithReconnectSubscription = wsWithReconnect.subscribe(event =>
        this.webSocketEventSubject.next(event as CollabWebsocketEvent)
      );

      // inform wId and try to see if lock can be granted
      this.send("WIdRequest", { wId });
      this.send("TryLockRequest", {});

      // refresh connection status
      this.websocketEvent().subscribe(_ => (this.connected = true));
    }
  }

  /**
   * On wid change, reopen the websocket so that server can get the new wid.
   */
  public reopenWebsocket(wId: number) {
    if (this.isCollabEnabled()) {
      this.closeWebsocket();
      this.openWebsocket(wId);
    }
  }

  /**
   * Closes the websocket.
   */
  public closeWebsocket() {
    if (this.isCollabEnabled()) {
      this.wsWithReconnectSubscription?.unsubscribe();
      this.websocket?.complete();
    }
  }

  /**
   * Gets the event received from the websocket server.
   */
  public websocketEvent(): Observable<CollabWebsocketEvent> {
    return this.webSocketEventSubject;
  }

  /**
   * Subscribe to a particular type of workflow websocket event
   */
  public subscribeToEvent<T extends CollabWebsocketEventTypes>(
    type: T
  ): Observable<{ type: T } & CollabWebsocketEventTypeMap[T]> {
    return this.websocketEvent().pipe(
      filter(event => event.type === type),
      map(event => event as { type: T } & CollabWebsocketEventTypeMap[T])
    );
  }

  /**
   * Sends a request to ws server.
   */
  public send<T extends CollabWebsocketRequestTypes>(type: T, payload: CollabWebsocketRequestTypeMap[T]): void {
    const request = {
      type,
      ...payload,
    } as any as CollabWebsocketRequest;
    this.websocket?.next(request);
  }

  /**
   * Changes whether to propagate updates (changes in the workflow) to other clients.
   * If set to false, no command will be sent.
   */
  public setPropagationEnabled(enabled: boolean): void {
    this.propagationEnabled = enabled;
  }

  /**
   * Gets whether command propagation is enabled.
   */
  public isPropagationEnabled(): boolean {
    return this.propagationEnabled;
  }

  /**
   * Gets current lock status.
   */
  public isLockGranted(): boolean {
    if (this.isCollabEnabled()) return this.lockGranted;
    else return true;
  }

  /**
   * Sets lock and the lock subject so that it can be observed.
   */
  public setLockStatus(granted: boolean): void {
    this.lockGranted = granted;
    this.lockGrantedSubject.next(this.lockGranted);
  }

  /**
   * Sets the workflowReadOnly status to true. Should not be further modified.
   */
  public setWorkflowAccess(isReadonly: boolean): void {
    this.workflowReadonly = isReadonly;
    this.workflowAccessSubject.next(isReadonly);
    if (isReadonly) this.setLockStatus(false);
  }

  /**
   * Executes changes received from other clients and preventing further propagation.
   * Whenever a remote change needs to be executed, the executor must use call collabService to use this method
   * instead of directly executing, and should provide the executed action as the callback function.
   */
  public handleRemoteChange(callback: Function): void {
    this.setPropagationEnabled(false);
    callback();
    this.setPropagationEnabled(true);
  }

  // Below are the requests that can be sent by other services.

  /**
   * Propagates a specific change to other active clients of the same wid.
   */
  public propagateChange(change: CommandMessage): void {
    if (this.isCollabEnabled() && this.isPropagationEnabled() && this.isLockGranted() && !this.workflowReadonly) {
      const commandMessage = JSON.stringify(change);
      this.send("CommandRequest", { commandMessage });
    }
  }

  /**
   * Request to be the lockholder
   */
  public acquireLock(): void {
    this.send("AcquireLockRequest", {});
  }

  /**
   * Whenever whole workflow needs to restore a version, should also tell other clients to reload.
   */
  public requestOthersToReload(): void {
    this.send("RestoreVersionRequest", {});
  }

  // Below are the events that need to be observed by relevant services/components.

  /**
   * Gets an observable for changes from other clients so that they can be applied to this client.
   */
  public getChangeStream(): Observable<CommandMessage> {
    return this.commandMessageSubject.asObservable();
  }

  /**
   * Gets an observable for changes on the lock status, should be enforced on anything
   * that needs to be enabled/disabled depending on the lock.
   */
  public getLockStatusStream(): Observable<boolean> {
    return this.lockGrantedSubject.asObservable();
  }

  /**
   * Gets an observable for reloading requests.
   */
  public getRestoreVersionStream(): Observable<boolean> {
    return this.restoreVersionSubject.asObservable();
  }

  /**
   * Gets an observable for whether the workflow is readonly.
   */
  public getWorkflowAccessStream(): Observable<boolean> {
    return this.workflowAccessSubject.asObservable();
  }
}

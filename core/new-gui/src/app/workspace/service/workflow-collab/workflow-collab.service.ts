import { Injectable } from "@angular/core";
import { webSocket } from "rxjs/webSocket";
import { CommandMessage } from "../workflow-graph/model/workflow-action.service";
import { environment } from "../../../../environments/environment";
import { Subject, Observable } from "rxjs";
import { getWebsocketUrl } from "src/app/common/util/url";

/**
 *
 * WorkflowCollabService manages functions related to workflow collaboration. For now it only supports
 * sending commands to the backend to propagate actions.
 *
 */

@Injectable({
  providedIn: "root",
})
export class WorkflowCollabService {
  private static readonly TEXERA_COLLAB_ENDPOINT = "wsapi/collab";
  private isCollabEnabled: boolean = false; // set initial value to false to disable service

  private socket = webSocket({
    url: getWebsocketUrl(WorkflowCollabService.TEXERA_COLLAB_ENDPOINT),
    deserializer: msg => msg["data"],
  });

  private messageSubject: Subject<CommandMessage> = new Subject<CommandMessage>();

  constructor() {
    if (environment.workflowCollabEnabled) {
      this.toggleCollabEnabled(true);
      this.socket.subscribe({
        next: response => {
          const message = JSON.parse(response) as CommandMessage;
          this.messageSubject.next(message);
        },
        complete: () => {
          console.log("websocket finished and disconnected");
        },
      });
    }
  }

  public toggleCollabEnabled(toggle: boolean): void {
    this.isCollabEnabled = toggle;
  }

  public getIsCollabEnabled(): boolean {
    return this.isCollabEnabled;
  }

  public sendCommand(update: CommandMessage): void {
    if (this.getIsCollabEnabled()) this.socket.next(update);
  }

  public getCommandMessageStream(): Observable<CommandMessage> {
    return this.messageSubject.asObservable();
  }

  public handleRemoteChange(callback: Function): void {
    this.toggleCollabEnabled(false);
    callback();
    this.toggleCollabEnabled(true);
  }
}

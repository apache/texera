declare module "y-websocket" {
  import { Awareness } from "y-protocols/awareness";
  import type { Doc } from "yjs";

  export class WebsocketProvider {
    constructor(serverUrl: string, roomname: string, doc: Doc);
    awareness: Awareness;
    shouldConnect: boolean;
    wsconnected: boolean;
    connect(): void;
    disconnect(): void;
  }
}

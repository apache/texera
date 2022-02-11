export interface InformWIdRequest
  extends Readonly<{
    wId: number;
  }> {}

export interface InformWIdEvent extends Readonly<{ message: string }> {}

export interface CommandRequest
  extends Readonly<{
    commandMessage: string;
  }> {}

export interface CommandEvent
  extends Readonly<{
    commandMessage: string;
  }> {}

export type CollabWebsocketRequestTypeMap = {
  InformWIdRequest: InformWIdRequest;
  HeartBeatRequest: {};
  CommandRequest: CommandRequest;
  AcquireLockRequest: {};
  TryLockRequest: {};
  RestoreVersionRequest: {};
};

export type CollabWebsocketEventTypeMap = {
  InformWIdResponse: InformWIdEvent;
  HeartBeatResponse: {};
  CommandEvent: CommandEvent;
  ReleaseLockEvent: {};
  LockGrantedEvent: {};
  LockRejectedEvent: {};
  RestoreVersionEvent: {};
  ReadOnlyAccessEvent: {};
};

// helper type definitions to generate the request and event types
type ValueOf<T> = T[keyof T];
type CustomUnionType<T> = ValueOf<
  {
    [P in keyof T]: {
      type: P;
    } & T[P];
  }
>;

export type CollabWebsocketRequestTypes = keyof CollabWebsocketRequestTypeMap;
export type CollabWebsocketRequest = CustomUnionType<CollabWebsocketRequestTypeMap>;

export type CollabWebsocketEventTypes = keyof CollabWebsocketEventTypeMap;
export type CollabWebsocketEvent = CustomUnionType<CollabWebsocketEventTypeMap>;

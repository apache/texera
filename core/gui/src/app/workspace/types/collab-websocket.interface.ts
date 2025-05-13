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

export interface WIdRequest
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

export interface WorkflowAccessEvent
  extends Readonly<{
    workflowReadonly: boolean;
  }> {}

export type CollabWebsocketRequestTypeMap = {
  WIdRequest: WIdRequest;
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
  WorkflowAccessEvent: WorkflowAccessEvent;
};

// helper type definitions to generate the request and event types
type ValueOf<T> = T[keyof T];
type CustomUnionType<T> = ValueOf<{
  [P in keyof T]: {
    type: P;
  } & T[P];
}>;

export type CollabWebsocketRequestTypes = keyof CollabWebsocketRequestTypeMap;
export type CollabWebsocketRequest = CustomUnionType<CollabWebsocketRequestTypeMap>;

export type CollabWebsocketEventTypes = keyof CollabWebsocketEventTypeMap;
export type CollabWebsocketEvent = CustomUnionType<CollabWebsocketEventTypeMap>;

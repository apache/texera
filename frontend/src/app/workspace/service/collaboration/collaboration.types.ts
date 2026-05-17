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

export type CollaborationTab = "chat" | "comments" | "online";

export type ChatMessageKind = "user" | "system";

export interface ChatMessage {
  id: string;
  userId: number | string;
  userName: string;
  color: string;
  content: string;
  timestamp: number;
  kind: ChatMessageKind;
}

export interface OperatorComment {
  id: string;
  operatorId: string;
  /** Undefined for the root of a thread; set for replies. */
  parentId?: string;
  content: string;
  userId: number | string;
  userName: string;
  color: string;
  timestamp: number;
  /** Only meaningful on the root comment of a thread. */
  resolved?: boolean;
}

export interface OperatorCommentThread {
  operatorId: string;
  operatorName?: string;
  root: OperatorComment;
  replies: OperatorComment[];
  resolved: boolean;
}

export interface OnlineUserSnapshot {
  clientId: string;
  name: string;
  color: string;
  isLocal: boolean;
  isActive: boolean;
  isIdle: boolean;
  lastActivity: number;
  currentlyEditing?: string;
  highlighted?: readonly string[];
}

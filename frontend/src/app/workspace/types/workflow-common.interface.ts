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

import { JSONSchema7 } from "json-schema";
import type { PartitionInfo } from "@texera/workflow-macros";
export type {
  LogicalPort,
  OperatorLink,
  OperatorPredicate,
  PartitionInfo,
  PortDescription,
} from "@texera/workflow-macros";

/**
 * This file contains multiple type declarations related to workflow-graph.
 * These type declarations should be identical to the backend API.
 */

export interface Point
  extends Readonly<{
    x: number;
    y: number;
  }> {}

export interface PortSchema
  extends Readonly<{
    jsonSchema: Readonly<JSONSchema7>;
  }> {}

export interface PortProperty
  extends Readonly<{ partitionInfo: PartitionInfo; dependencies: { id: number; internal: boolean }[] }> {}

export interface Comment
  extends Readonly<{
    content: string;
    creationTime: string;
    creatorName: string;
    creatorID: number;
  }> {}

export interface CommentBox {
  commentBoxID: string;
  comments: Comment[];
  commentBoxPosition: Point;
}

/**
 * refer to src/main/scalapb/org/apache/texera/web/workflowruntimestate/ConsoleMessage.scala
 */
export type ConsoleMessage = Readonly<{
  workerId: string;
  timestamp: {
    nanos: number;
    seconds: number;
  };
  msgType: {
    name: string;
  };
  source: string;
  title: string;
  message: string;
}>;

export type ConsoleUpdateEvent = Readonly<{
  operatorId: string;
  messages: ReadonlyArray<ConsoleMessage>;
}>;

export type BreakpointInfo = Readonly<{
  breakpointId: number | undefined;
  condition: string;
  hit: boolean;
}>;

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

export enum EntityType {
  Workflow = "workflow",
  Dataset = "dataset",
  Model = "model",
  ComputingUnit = "computing-unit",
}

export enum ActionType {
  View = "view",
  Like = "like",
  Clone = "clone",
  Unlike = "unlike",
}

export type LikedStatus = {
  entityId: number;
  entityType: EntityType;
  isLiked: boolean;
};

export interface CountResponse {
  entityId: number;
  entityType: EntityType;
  counts: Partial<Record<ActionType, number>>;
}

export interface AccessResponse {
  entityType: EntityType;
  entityId: number;
  userIds: number[];
}

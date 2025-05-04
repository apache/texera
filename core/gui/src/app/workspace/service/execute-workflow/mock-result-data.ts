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

import { WebDataUpdate, WebPaginationUpdate } from "../../types/execute-workflow.interface";
import { Point, OperatorPredicate } from "../../types/workflow-common.interface";
import { IndexableObject } from "ng-zorro-antd/core/types";

export const mockData: IndexableObject[] = [
  {
    id: 1,
    layer: "Disk Space and I/O Managers",
    duty: "Manage space on disk (pages), including extents",
    slides: "slide 2",
  },
  {
    id: 2,
    layer: "Buffer Manager",
    duty: "DB-oriented page replacement schemes",
    slides: "slide 3",
  },
  {
    id: 3,
    layer: "System Catalog",
    duty: "Info about physical data, tables, indexes",
    slides: "slides 4 and 5",
  },
  {
    id: 4,
    layer: "Access methods",
    duty: "Index structures for access based on field values.",
    slides: "B+ tree: slides 6 and 7. Hashing: slide 8. Indexing Performance: slide 9.",
  },
  {
    id: 5,
    layer: "Plan Executor + Relational Operators",
    duty: "Runtime side of query processing",
    slides: "Sorting: slide 10. Selection+Projection: slide 11. Join: slide 12. Set operations: slide 13.",
  },
  {
    id: 6,
    layer: "Query Optimizer",
    duty: "Rewrite query logically. Perform cost-based optimization",
    slides: "Cost estimation: slide 14. SystemR Optimizer: slide 15",
  },
];

export const mockResultSnapshotUpdate: WebDataUpdate = {
  mode: { type: "SetSnapshotMode" },
  table: mockData,
};

export const mockResultPaginationUpdate: WebPaginationUpdate = {
  mode: { type: "PaginationMode" },
  dirtyPageIndices: [1],
  totalNumTuples: mockData.length,
};

export const mockResultOperator: OperatorPredicate = {
  operatorID: "operator-sink",
  operatorType: "ViewResults",
  operatorProperties: {},
  inputPorts: [],
  outputPorts: [],
  showAdvanced: false,
  isDisabled: false,
  operatorVersion: "1.0",
};

export const mockResultPoint: Point = {
  x: 1,
  y: 1,
};

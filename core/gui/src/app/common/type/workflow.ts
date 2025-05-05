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

import { WorkflowMetadata } from "../../dashboard/type/workflow-metadata.interface";
import { OperatorLink, OperatorPredicate, Point, CommentBox } from "../../workspace/types/workflow-common.interface";

export interface WorkflowSettings {
  dataTransferBatchSize: number;
}

/**
 * WorkflowContent is used to store the information of the workflow
 *  1. all existing operators and their properties
 *  2. operator's position on the JointJS paper
 *  3. operator link predicates
 *
 * When the user refreshes the browser, the CachedWorkflow interface will be
 *  automatically cached and loaded once the refresh completes. This information
 *  will then be used to reload the entire workflow.
 *
 */

export interface WorkflowContent
  extends Readonly<{
    operators: OperatorPredicate[];
    operatorPositions: { [key: string]: Point };
    links: OperatorLink[];
    commentBoxes: CommentBox[];
    settings: WorkflowSettings;
  }> {}

export type Workflow = { content: WorkflowContent } & WorkflowMetadata;

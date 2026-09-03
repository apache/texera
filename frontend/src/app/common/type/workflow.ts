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
import { CommentBox, OperatorLink, OperatorPredicate, Point } from "../../workspace/types/workflow-common.interface";

export enum ExecutionMode {
  PIPELINED = "PIPELINED",
  MATERIALIZED = "MATERIALIZED",
}

export interface WorkflowSettings {
  dataTransferBatchSize: number;
  executionMode: ExecutionMode;
}

/**
 * One input exposed on the Form View: a binding to a single operator property. That property
 * is always the live value (filling the form is the same edit as changing it on the canvas);
 * the rest is presentation. `id` is a stable identity so reorder/remove never use the raw key.
 */
export interface FormFieldBinding {
  id: string;
  operatorID: string;
  /** The operator property this input writes to. */
  propertyKey: string;
  displayName: string;
  helpText?: string;
  /** Per-sub-field overrides within the property, keyed by field path (`alias`, `predicates.value`
   *  -- array indices dropped, so one entry covers every row). Only where the author changed it. */
  overrides?: { [path: string]: FormFieldOverride };
}

export interface FormFieldOverride {
  /** Kept out of the reader's form. The value the author set still applies. */
  hidden?: boolean;
  /** Replaces the schema's label. Empty or absent keeps the schema's own. */
  displayName?: string;
}

/**
 * How a workflow presents itself on the Form View. Which view a workflow opens in by default
 * lives in `workflow.default_view` (canvas or form), and nothing here affects execution.
 */
export interface FormBindingConfig {
  instruction?: {
    /** Empty title hides the heading rather than showing a placeholder. */
    title?: string;
    /** Markdown. */
    body: string;
  };
  /** Array order is display order; the author reorders by dragging. */
  fields: FormFieldBinding[];
  /** Operators whose results are shown under the workflow after a run. */
  resultOperatorIds: string[];
}

export function getDefaultFormBinding(): FormBindingConfig {
  return { fields: [], resultOperatorIds: [] };
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
    /** Present once an author set up the Form View. Rides in the content (like `settings`),
     *  so it is saved/cloned/versioned/published with the workflow for free. */
    formBinding?: FormBindingConfig;
  }> {}

export type Workflow = { content: WorkflowContent } & WorkflowMetadata;

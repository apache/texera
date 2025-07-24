/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Workflow } from "../../common/type/workflow";

/**
 * Checks if the given workflow is "broken".
 * A workflow is considered broken if any of its links reference an operator ID
 * that does not exist in the list of operators within the workflow.
 *
 * @param workflow - The workflow to validate, containing operators and links.
 * @returns 'true' if the workflow is broken, 'false' otherwise.
 */
export function checkIfWorkflowBroken(workflow: Workflow): boolean {
  const validOperatorIDs = new Set(workflow.content.operators.map(o => o.operatorID));
  return workflow.content.links.some(
    link => !validOperatorIDs.has(link.source.operatorID) || !validOperatorIDs.has(link.target.operatorID)
  );
}

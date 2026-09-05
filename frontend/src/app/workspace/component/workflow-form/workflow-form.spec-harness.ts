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

import { of, Subject } from "rxjs";
import { vi } from "vitest";

import { DefaultView } from "../../../dashboard/type/workflow-metadata.interface";

/** The workflow every test opens by default: a form-default workflow, writable, empty content. */
export const formViewWorkflow = { name: "scGPT", defaultView: DefaultView.FORM, readonly: false, content: {} };

/**
 * Mocks shared by every workflow-form spec, plus the component factory. Only what the current
 * slices exercise is mocked; later slices add the dependencies (and streams) they introduce, so
 * each PR's additions are covered by that PR's own spec. `setupHarness()` runs once per
 * `beforeEach`; `build(workflow)` (in each spec) constructs the component with the subset its
 * constructor takes.
 */
export function setupHarness() {
  const router = { navigate: vi.fn() };
  const workflowChangedStream = new Subject<unknown>();
  const workflowMetaDataChangedStream = new Subject<unknown>();
  // The preview centres the embedded graph once it is built; tests assert this fired.
  const triggerCenterEvent = vi.fn();

  const workflowActionService = {
    resetAsNewWorkflow: vi.fn(),
    setNewSharedModel: vi.fn(),
    reloadWorkflow: vi.fn(),
    enableWorkflowModification: vi.fn(),
    disableWorkflowModification: vi.fn(),
    clearWorkflow: vi.fn(),
    workflowChanged: () => workflowChangedStream.asObservable(),
    workflowMetaDataChanged: () => workflowMetaDataChangedStream.asObservable(),
    getWorkflow: vi.fn().mockReturnValue({ wid: 7, content: { operators: [], operatorPositions: {} } }),
    getWorkflowMetadata: () => ({ name: "scGPT", lastModifiedTime: 1767225600000 }),
    setWorkflowName: vi.fn(),
    setWorkflowMetadata: vi.fn(),
    getTexeraGraph: () => ({ triggerCenterEvent }),
  };
  const workflowPersistService = {
    retrieveWorkflow: vi.fn().mockReturnValue(of(formViewWorkflow)),
    // Off by default so opening a workflow does not save; the save tests turn it on.
    isWorkflowPersistEnabled: vi.fn().mockReturnValue(false),
    persistWorkflow: vi.fn().mockReturnValue(of(formViewWorkflow)),
  };
  const coeditorPresenceService = { coeditors: [] };
  const route = { snapshot: { params: { id: "7" } } };
  const operatorMetadataService = { getOperatorMetadata: () => of({}) };
  const executeWorkflowService = { resetExecutionAndWorkers: vi.fn() };
  const workflowResultService = { clearResults: vi.fn() };
  const notificationService = { error: vi.fn() };
  // Not logged in by default so opening a workflow does not save; the save tests log in.
  const userService = { getCurrentUser: () => undefined, isLogin: vi.fn().mockReturnValue(false) };
  const cdr = { detectChanges: vi.fn() };
  const computingUnitStatusService = { disconnect: vi.fn() };
  const workflowConsoleService = { clearConsoleMessages: vi.fn() };
  // The name field is measured off the host; querySelector returns null so the measuring
  // (DOM-layout, jsdom has none) short-circuits.
  const host = { nativeElement: { querySelector: () => null } };
  const datePipe = { transform: () => "01/01/2026 00:00:00" };
  const config = { env: { formViewEnabled: true } };

  // Point the persist mock at `workflow`; each spec supplies the remaining constructor
  // arguments in its own order via the named mocks above.
  const useWorkflow = (workflow: any) => {
    workflowPersistService.retrieveWorkflow.mockReturnValue(of(workflow));
    workflowPersistService.persistWorkflow.mockReturnValue(of(workflow));
  };

  return {
    useWorkflow,
    router,
    coeditorPresenceService,
    route,
    workflowActionService,
    workflowPersistService,
    operatorMetadataService,
    executeWorkflowService,
    workflowResultService,
    notificationService,
    userService,
    cdr,
    computingUnitStatusService,
    workflowConsoleService,
    host,
    datePipe,
    config,
    workflowChangedStream,
    workflowMetaDataChangedStream,
    triggerCenterEvent,
  };
}

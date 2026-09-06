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
import { ResolvedField } from "../../service/form-binding/form-binding.service";

/** The workflow every test opens by default: a form-default workflow, writable, empty content. */
export const formViewWorkflow = { name: "scGPT", defaultView: DefaultView.FORM, readonly: false, content: {} };

/** A binding for one operator property, keyed by id (operator "op-1"). */
export const binding = (id: string, displayName: string) => ({
  id,
  operatorID: "op-1",
  propertyKey: id,
  displayName,
});

/** A resolved (non-broken) input, ready to render. Override `binding`/`brokenReason` per test. */
export const resolved = (id: string, displayName: string, extra: Partial<ResolvedField> = {}): ResolvedField => ({
  binding: binding(id, displayName),
  value: "seed",
  operatorLabel: "Source: Scan",
  schema: { type: "string" } as any,
  ...extra,
});

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
  // Compilation reports column names late; the form rebuilds its inputs off this stream.
  const compilationChanged = new Subject<unknown>();
  // The operators the graph holds: `hasOperatorIds` gates operatorSchemaFor, `graphOperators`
  // supplies each operator's type (which picks the custom widget). Tests add to them as needed.
  const hasOperatorIds = new Set<string>();
  const graphOperators: any[] = [];
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
    getTexeraGraph: () => ({
      triggerCenterEvent,
      hasOperator: (id: string) => hasOperatorIds.has(id),
      getOperator: (id: string) => graphOperators.find(o => o.operatorID === id),
    }),
    // Exposing or un-exposing a property announces on this stream; the form re-reads its config.
    formBindingChanged$: new Subject<unknown>(),
  };
  // Resolves the exposed inputs and reads/writes their values. Tests point `resolveFields` at the
  // inputs they want rendered; `readValue` seeds the write-back guard.
  const formBindingService = {
    resolveFields: vi.fn().mockReturnValue([]),
    readValue: vi.fn().mockReturnValue(undefined),
    writeValue: vi.fn(),
  };
  // A field per property the tests expose. Real formly json-schema conversion is exercised by the
  // property panel's own spec; here a deterministic map keeps these tests about the component's
  // own decisions (which field, which widget, the write-back), and drives the `map` callback.
  const formlyJsonschema = {
    toFieldConfig: (_schema: any, opts: any) => {
      const fields = [
        { key: "n_hvg", props: { label: "N" } },
        { key: "fileName", props: { label: "File" } },
        { key: "modelId", props: { label: "Model" } },
        { key: "datasetVersionPath", props: { label: "Dataset" } },
        // An object property with sub-fields (drives the override walk over a fieldGroup). The
        // schema descriptions are here so a test can assert the walk drops them.
        {
          key: "nested",
          props: { label: "Nested", description: "obj note" },
          fieldGroup: [{ key: "sub", props: { label: "Sub", description: "sub note" } }],
        },
        // A repeated section whose row template is a builder (drives the fieldArray-wrapping path).
        // The returned row carries its own description (the schema's items.description), so a test
        // can assert the walk drops it -- it would otherwise render once per row.
        {
          key: "predicates",
          props: { label: "Predicates" },
          fieldArray: () => ({
            props: { description: "row note" },
            fieldGroup: [{ key: "alias", props: { label: "Alias" } }],
          }),
        },
        // A scalar array: its row template is a leaf (no sub-fields), drives the leaf-item branch.
        {
          key: "tags",
          props: { label: "Tags" },
          fieldArray: { key: "item", props: { label: "Tag", description: "item note" } },
        },
        // A static object-array template (fieldArray is an object WITH sub-fields, not a builder):
        // drives the object-array-template branch, where the container's own items.description must
        // be dropped even though the container itself is not walked as a root.
        {
          key: "rules",
          props: { label: "Rules" },
          fieldArray: {
            props: { description: "rules note" },
            fieldGroup: [{ key: "field", props: { label: "Field", description: "field note" } }],
          },
        },
        // A scalar array whose row template is a BUILDER returning a leaf (no fieldGroup): drives
        // the leaf case inside the fieldArray-function wrapper.
        {
          key: "tagsFn",
          props: { label: "Tags (fn)" },
          fieldArray: () => ({ key: "item", props: { label: "Tag", description: "fn note" } }),
        },
      ];
      return { fieldGroup: opts?.map ? fields.map(opts.map) : fields };
    },
  };
  const dynamicSchemaService = { getDynamicSchema: () => ({ jsonSchema: {} }) };
  const workflowCompilingService = {
    getCompilationStateInfoChangedStream: () => compilationChanged.asObservable(),
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
  // (DOM-layout, jsdom has none) short-circuits. `contains` drives isTypingInTheForm; false by
  // default so a rebuild is never suppressed, and overridden by the tests that probe typing.
  const host = { nativeElement: { querySelector: () => null, contains: () => false } };
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
    formBindingService,
    executeWorkflowService,
    workflowResultService,
    notificationService,
    userService,
    formlyJsonschema,
    cdr,
    dynamicSchemaService,
    workflowCompilingService,
    computingUnitStatusService,
    workflowConsoleService,
    host,
    datePipe,
    config,
    workflowChangedStream,
    workflowMetaDataChangedStream,
    compilationChanged,
    hasOperatorIds,
    graphOperators,
    triggerCenterEvent,
  };
}

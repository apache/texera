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
  // Announces every form-config write (see formBindingChanged$ and the form-binding mock below).
  const formBindingChanged = new Subject<unknown>();
  // The root-level modification lock as other writers flip it (the execute service after a run, the
  // computing-unit selector); tests emit `true` to stand in for one of them unlocking the graph.
  const modificationEnabled = new Subject<boolean>();
  const workflowMetaDataChangedStream = new Subject<unknown>();
  // Compilation reports column names late; the form rebuilds its inputs off this stream.
  const compilationChanged = new Subject<unknown>();
  // Run-related streams the run tests drive: execution state, the engine's duration event, the
  // computing-unit connection status, the workflow validity, and the websocket connection.
  const executionStateStream = new Subject<any>();
  const durationEvents = new Subject<{ duration: number; isRunning: boolean }>();
  const statusStream = new Subject<any>();
  // The picked computing unit (with its accessPrivilege), separate from the connection status.
  const selectedUnitStream = new Subject<any>();
  const validationStream = new Subject<{ errors: Record<string, unknown>; workflowEmpty: boolean }>();
  const connectionStream = new Subject<boolean>();
  // A result changed; the form bumps versions and re-fits. Tests drive it directly.
  const resultUpdateStream = new Subject<Record<string, unknown>>();
  // Fires when the canvas's view-result set changes (a co-editor's eye toggle included).
  const viewResultChanged = new Subject<unknown>();
  // Fires for a change of the graph's shape (operator added/deleted/disabled, link added/deleted); one
  // subject stands in for all five streams, since the form treats them alike.
  const graphStructureChanged = new Subject<unknown>();
  // Fires when a step's display name changes; its own subject, so a test can tell it from the shape.
  const displayNameChanged = new Subject<unknown>();
  // The operators the graph holds: `hasOperatorIds` gates operatorSchemaFor, `graphOperators`
  // supplies each operator's type (which picks the custom widget). Tests add to them as needed.
  const hasOperatorIds = new Set<string>();
  const graphOperators: any[] = [];
  // Operators with view-result ("the eye") on in the canvas; the form shows these on top of the
  // always-shown terminal steps.
  const viewResultIds = new Set<string>();
  // Selecting a step on the embedded canvas drives the read-only inspect panel.
  const highlightStream = new Subject<readonly string[]>();
  const unhighlightStream = new Subject<readonly string[]>();
  const highlightedIds: string[] = [];
  const updateSharedModelAwareness = vi.fn();
  // The joint wrapper's unhighlight: drops the ids from the selection and nothing else. The two
  // streams stay under the tests' control, so this does not emit.
  const unhighlightOperators = vi.fn((...ops: string[]) => {
    for (const op of ops) {
      const at = highlightedIds.indexOf(op);
      if (at !== -1) {
        highlightedIds.splice(at, 1);
      }
    }
  });
  // The action service's unhighlight, which is the one the form has to use: it delegates to the
  // wrapper AND publishes the resulting selection on the shared awareness channel, so co-editors
  // stop seeing the highlight. Modelled as the real pair so a test can tell the two apart.
  const serviceUnhighlightOperators = vi.fn((...ops: string[]) => {
    unhighlightOperators(...ops);
    updateSharedModelAwareness("highlighted", [...highlightedIds]);
  });
  // Operators that produced a non-empty result -- drives hasNonEmptyResult in the result mock.
  const anyResultIds = new Set<string>();
  // Operators the engine treats as terminal (out-degree 0 on the ENABLED plan): the engine materializes
  // a terminal's result with no eye set, so the form shows it too. A test marks an id terminal by adding
  // it here (no downstream at all) or to disabledDownstream (its only downstream link is disabled) --
  // both mean "no enabled downstream link", so getAllEnabledLinks emits no link from that operator.
  const terminalIds = new Set<string>();
  const disabledDownstream = new Set<string>();
  // The preview centres the embedded graph once it is built; tests assert this fired.
  const triggerCenterEvent = vi.fn();

  const workflowActionService = {
    resetAsNewWorkflow: vi.fn(),
    setNewSharedModel: vi.fn(),
    reloadWorkflow: vi.fn(),
    enableWorkflowModification: vi.fn(),
    disableWorkflowModification: vi.fn(),
    getWorkflowModificationEnabledStream: () => modificationEnabled.asObservable(),
    clearWorkflow: vi.fn(),
    workflowChanged: () => workflowChangedStream.asObservable(),
    workflowMetaDataChanged: () => workflowMetaDataChangedStream.asObservable(),
    getWorkflow: vi.fn().mockReturnValue({ wid: 7, content: { operators: [], operatorPositions: {} } }),
    getWorkflowMetadata: () => ({ name: "scGPT", lastModifiedTime: 1767225600000 }),
    setWorkflowName: vi.fn(),
    setWorkflowMetadata: vi.fn(),
    setHighlightingEnabled: vi.fn(),
    unhighlightOperators: serviceUnhighlightOperators,
    getTexeraGraph: () => ({
      triggerCenterEvent,
      hasOperator: (id: string) => hasOperatorIds.has(id),
      getOperator: (id: string) => graphOperators.find(o => o.operatorID === id),
      getAllOperators: () => graphOperators,
      getOperatorsToViewResult: () => new Set(viewResultIds),
      getViewResultOperatorsChangedStream: () => viewResultChanged.asObservable(),
      getOperatorAddStream: () => graphStructureChanged.asObservable(),
      getOperatorDeleteStream: () => graphStructureChanged.asObservable(),
      getLinkAddStream: () => graphStructureChanged.asObservable(),
      getLinkDeleteStream: () => graphStructureChanged.asObservable(),
      getDisabledOperatorsChangedStream: () => graphStructureChanged.asObservable(),
      getOperatorDisplayNameChangedStream: () => displayNameChanged.asObservable(),
      // Enabled links only: a non-terminal operator emits one dummy outgoing link; a terminal operator
      // (in terminalIds, or whose downstream is disabled via disabledDownstream) emits none. Drives the
      // component's terminal detection (terminalOperatorIds).
      getAllEnabledLinks: () =>
        graphOperators
          .filter(op => !terminalIds.has(op.operatorID) && !disabledDownstream.has(op.operatorID))
          .map(op => ({
            linkID: `${op.operatorID}-out`,
            source: { operatorID: op.operatorID },
            target: { operatorID: "downstream" },
          })),
      updateSharedModelAwareness,
    }),
    getJointGraphWrapper: () => ({
      getJointOperatorHighlightStream: () => highlightStream.asObservable(),
      getJointOperatorUnhighlightStream: () => unhighlightStream.asObservable(),
      getCurrentHighlightedOperatorIDs: () => highlightedIds,
      unhighlightOperators,
    }),
    // Every config write announces on this stream (setFormBinding emits it); the form re-reads its
    // config on it unless the write is one of its own presentation edits. The form-binding mock's
    // writers below emit here, as the real service does, so that chain is under test.
    formBindingChanged$: formBindingChanged.asObservable(),
  };
  // Resolves the exposed inputs and reads/writes their values. Tests point `resolveFields` at the
  // inputs they want rendered; `readValue` seeds the write-back guard.
  const formBindingService = {
    // The presentation config: the instruction plus the fields. Tests override getConfig to give an
    // instruction; resolveFields drives which inputs render.
    getConfig: vi.fn().mockReturnValue({ instruction: undefined, fields: [] }),
    resolveFields: vi.fn().mockReturnValue([]),
    readValue: vi.fn().mockReturnValue(undefined),
    writeValue: vi.fn(),
    // A result card's friendly label; the mock returns the operator's display name or its id.
    operatorLabel: (op: any) => op?.customDisplayName ?? op?.operatorType ?? op?.operatorID,
    // Author-mode writes. Spied so a test can assert the edit was made without needing a real
    // binding store, and each announces on formBindingChanged$ as the real service does (every
    // write goes through setFormBinding, which emits), so the page's reaction to its own writes --
    // rebuild, or not, for a presentation edit -- is what the tests see.
    updateBinding: vi.fn(() => formBindingChanged.next(undefined)),
    setFieldOverride: vi.fn(() => formBindingChanged.next(undefined)),
    removeBinding: vi.fn(() => formBindingChanged.next(undefined)),
    reorder: vi.fn(() => formBindingChanged.next(undefined)),
    toggleShownResult: vi.fn(() => formBindingChanged.next(undefined)),
    updateConfig: vi.fn(() => formBindingChanged.next(undefined)),
    setFields: vi.fn(),
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
  const executeWorkflowService = {
    getExecutionStateStream: () => executionStateStream.asObservable(),
    executeWorkflow: vi.fn(),
    killWorkflow: vi.fn(),
    resetExecutionAndWorkers: vi.fn(),
  };
  // Results: `anyResultIds` marks which operators produced a non-empty result; `snapshotById`
  // lets a test give an operator a snapshot (drives vizHasContent). `hasPaginatedResult` is off
  // unless a test overrides it. `getResultUpdateStream` is the stream the form watches.
  const snapshotById = new Map<string, ReadonlyArray<object>>();
  const workflowResultService = {
    clearResults: vi.fn(),
    getResultUpdateStream: () => resultUpdateStream.asObservable(),
    hasNonEmptyResult: (id: string) => anyResultIds.has(id),
    hasPaginatedResult: (_id: string) => false,
    getResultService: (id: string) => ({ getCurrentResultSnapshot: () => snapshotById.get(id) }),
  };
  const panelResizeService = { changePanelSize: vi.fn() };
  const notificationService = { error: vi.fn() };
  // Not logged in by default so opening a workflow does not save; the save tests log in.
  const userService = { getCurrentUser: () => undefined, isLogin: vi.fn().mockReturnValue(false) };
  const markdownService = { parse: (s: string) => s };
  const cdr = { detectChanges: vi.fn(), markForCheck: vi.fn() };
  const computingUnitStatusService = {
    disconnect: vi.fn(),
    getSelectedComputingUnit: () => selectedUnitStream.asObservable(),
    getStatus: () => statusStream.asObservable(),
  };
  const workflowConsoleService = { clearConsoleMessages: vi.fn() };
  // The websocket the run clock and the "Connecting" state read. `isConnected` is a plain settable
  // flag so a test can put the page in the connecting window.
  const workflowWebsocketService = {
    subscribeToEvent: (_: string) => durationEvents.asObservable(),
    isConnected: true,
    getConnectionStatusStream: () => connectionStream.asObservable(),
  };
  const validationWorkflowService = {
    getWorkflowValidationErrorStream: () => validationStream.asObservable(),
  };
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
    markdownService,
    formlyJsonschema,
    cdr,
    dynamicSchemaService,
    workflowCompilingService,
    computingUnitStatusService,
    workflowConsoleService,
    workflowWebsocketService,
    panelResizeService,
    validationWorkflowService,
    host,
    datePipe,
    config,
    workflowChangedStream,
    formBindingChanged,
    workflowMetaDataChangedStream,
    compilationChanged,
    executionStateStream,
    modificationEnabled,
    durationEvents,
    statusStream,
    selectedUnitStream,
    validationStream,
    connectionStream,
    resultUpdateStream,
    viewResultChanged,
    graphStructureChanged,
    displayNameChanged,
    hasOperatorIds,
    graphOperators,
    viewResultIds,
    anyResultIds,
    terminalIds,
    disabledDownstream,
    snapshotById,
    triggerCenterEvent,
    highlightStream,
    unhighlightStream,
    highlightedIds,
    unhighlightOperators,
    serviceUnhighlightOperators,
    updateSharedModelAwareness,
  };
}

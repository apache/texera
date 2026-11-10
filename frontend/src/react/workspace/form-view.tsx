/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { useEffect, useMemo, useState, useSyncExternalStore, type ReactNode } from "react";
import { v4 as uuid } from "uuid";
import { WorkflowSession } from "./workflow-session";
import type { FormBindingConfig, FormFieldBinding, FormFieldOverride } from "../../core/common/type/workflow";
import type { OperatorMetadata } from "../../core/workspace/types/operator-schema.interface";
import type { OperatorPredicate } from "../../core/workspace/types/workflow-common.interface";
import { SchemaForm, type PropertyWidgets } from "../forms/schema-form";
import { useOperatorPropertyForms, type OperatorPropertyServices } from "./operator-properties";
import { asSchema, resolveSchemaReference, type OperatorPropertySchema } from "../forms/schema";
import type { OperatorFormContext } from "../forms/operator-context";
import { resolveOperatorWidget } from "../forms/widget-metadata";
import { MarkdownEditor, RichText } from "../common/rich-text";
import { ReadonlyWorkflowPreview } from "../hub/hub-pages";
import "./form-view.css";

export interface FormViewProps {
  session: WorkflowSession;
  metadata: OperatorMetadata;
  context?: OperatorFormContext;
  contextForOperator?: (operator: OperatorPredicate) => OperatorFormContext;
  schemaForOperator?: (operator: OperatorPredicate) => OperatorPropertySchema | undefined;
  widgetsForOperator?: (operator: OperatorPredicate) => PropertyWidgets;
  propertyServices?: Omit<OperatorPropertyServices, "session" | "metadata">;
  onCanvas(): void;
  renderResults?: (operatorIds: string[]) => ReactNode;
  renderPreview?: (workflow: ReturnType<WorkflowSession["getSnapshot"]>["workflow"]) => ReactNode;
  readOnly?: boolean;
  authoring?: boolean;
  onAuthoringChange?: (authoring: boolean) => void;
}
function bindingConfig(session: WorkflowSession): FormBindingConfig {
  return session.getSnapshot().workflow.content.formBinding ?? { fields: [] };
}
function operatorLabel(operator: OperatorPredicate, metadata: OperatorMetadata): string {
  return (
    operator.customDisplayName?.trim() ||
    metadata.operators.find(schema => schema.operatorType === operator.operatorType)?.additionalMetadata
      .userFriendlyName ||
    operator.operatorType
  );
}
export function formResultIds(
  session: WorkflowSession,
  wanted?: readonly string[]
): { terminals: string[]; available: Set<string>; shown: string[] } {
  const operators = session.graph.getAllOperators();
  const downstream = new Set(session.graph.getAllEnabledLinks().map(link => link.source.operatorID));
  const terminals = operators
    .filter(operator => !operator.isDisabled && !downstream.has(operator.operatorID))
    .map(operator => operator.operatorID);
  const viewed = session.graph.getOperatorsToViewResult();
  const available = new Set(
    operators
      .filter(
        operator => !operator.isDisabled && (viewed.has(operator.operatorID) || terminals.includes(operator.operatorID))
      )
      .map(operator => operator.operatorID)
  );
  return { terminals, available, shown: [...new Set(wanted ?? terminals)].filter(id => available.has(id)) };
}

/** Form and canvas share one WorkflowSession: filling a card edits the same operator values. */
export function FormView({
  session,
  metadata,
  context,
  contextForOperator,
  schemaForOperator,
  widgetsForOperator,
  propertyServices,
  onCanvas,
  renderResults,
  renderPreview,
  readOnly = false,
  authoring: controlledAuthoring,
  onAuthoringChange,
}: FormViewProps) {
  const propertyForms = useOperatorPropertyForms(
    propertyServices ? { ...propertyServices, session, metadata } : undefined
  );
  const { workflow, canEdit: sessionCanEdit } = useSyncExternalStore(
    session.subscribe,
    session.getSnapshot,
    session.getSnapshot
  );
  const config = workflow.content.formBinding ?? { fields: [] };
  const canEdit = !readOnly && sessionCanEdit;
  const [localAuthoring, setLocalAuthoring] = useState(false);
  const authoring = canEdit && (controlledAuthoring ?? localAuthoring);
  const [instructionOpen, setInstructionOpen] = useState(true);
  const [viewerResults, setViewerResults] = useState<string[]>();
  const [operatorId, setOperatorId] = useState(workflow.content.operators[0]?.operatorID ?? "");
  const [propertyKey, setPropertyKey] = useState("");
  const [inspecting, setInspecting] = useState<string>();
  const [error, setError] = useState<string>();
  const [switching, setSwitching] = useState(false);
  useEffect(() => {
    setViewerResults(undefined);
    setInspecting(undefined);
  }, [session]);

  function update(patch: Partial<FormBindingConfig>) {
    if (canEdit) session.setFormBinding({ ...bindingConfig(session), ...patch });
  }
  function patchBinding(id: string, patch: Partial<FormFieldBinding>) {
    update({
      fields: bindingConfig(session).fields.map(binding => (binding.id === id ? { ...binding, ...patch } : binding)),
    });
  }
  function patchOverride(binding: FormFieldBinding, path: string, patch: Partial<FormFieldOverride>) {
    const current = bindingConfig(session).fields.find(field => field.id === binding.id);
    if (!current) return;
    const overrides = { ...current.overrides },
      merged = { ...overrides[path], ...patch };
    if (!merged.displayName?.trim()) delete merged.displayName;
    if (merged.hidden !== true) delete merged.hidden;
    if (Object.keys(merged).length) overrides[path] = merged;
    else delete overrides[path];
    patchBinding(binding.id, { overrides: Object.keys(overrides).length ? overrides : undefined });
  }
  function schemaOf(operator: OperatorPredicate): OperatorPropertySchema | undefined {
    return (
      schemaForOperator?.(operator) ??
      propertyForms.schemaForOperator(operator) ??
      (metadata.operators.find(schema => schema.operatorType === operator.operatorType)?.jsonSchema as
        | OperatorPropertySchema
        | undefined)
    );
  }
  function toggleAuthoring() {
    const next = !authoring;
    setViewerResults(undefined);
    setLocalAuthoring(next);
    onAuthoringChange?.(next);
  }
  function reorder(from: number, to: number) {
    const fields = [...bindingConfig(session).fields];
    if (from === to || from < 0 || to < 0 || from >= fields.length || to >= fields.length) return;
    fields.splice(to, 0, fields.splice(from, 1)[0]);
    update({ fields });
  }
  function expose(operator: OperatorPredicate, key: string, exposed: boolean) {
    const current = bindingConfig(session).fields;
    const existing = current.find(binding => binding.operatorID === operator.operatorID && binding.propertyKey === key);
    if (!exposed) {
      if (existing) update({ fields: current.filter(binding => binding.id !== existing.id) });
      return;
    }
    if (existing) return;
    const property = asSchema(schemaOf(operator)?.properties?.[key]);
    if (resolveOperatorWidget(property, key, operator.operatorType) === "codearea") return;
    update({
      fields: [
        ...current,
        {
          id: `field-${uuid()}`,
          operatorID: operator.operatorID,
          propertyKey: key,
          displayName: property.title || key,
        },
      ],
    });
  }
  const selectedOperator = workflow.content.operators.find(operator => operator.operatorID === operatorId);
  useEffect(() => {
    if (!workflow.content.operators.some(operator => operator.operatorID === operatorId))
      setOperatorId(workflow.content.operators[0]?.operatorID ?? "");
  }, [workflow.content.operators, operatorId]);
  const selectedSchema = selectedOperator && schemaOf(selectedOperator);
  const availableProperties = useMemo(
    () =>
      selectedOperator
        ? Object.entries(selectedSchema?.properties ?? {}).filter(
            ([key, definition]) =>
              !config.fields.some(
                binding => binding.operatorID === selectedOperator.operatorID && binding.propertyKey === key
              ) && resolveOperatorWidget(asSchema(definition), key, selectedOperator.operatorType) !== "codearea"
          )
        : [],
    [selectedOperator, selectedSchema, config.fields]
  );
  useEffect(() => {
    if (!availableProperties.some(([key]) => key === propertyKey)) setPropertyKey(availableProperties[0]?.[0] ?? "");
  }, [availableProperties, propertyKey]);

  const results = formResultIds(session, viewerResults ?? config.shownResultIds);
  const listed = new Set(authoring ? config.shownResultIds ?? [] : []);
  const choices = workflow.content.operators.filter(
    operator => !operator.isDisabled && (results.available.has(operator.operatorID) || listed.has(operator.operatorID))
  );
  function toggleResult(id: string) {
    const shown = viewerResults ?? bindingConfig(session).shownResultIds ?? results.terminals;
    const next = shown.includes(id) ? shown.filter(value => value !== id) : [...shown, id];
    if (authoring) update({ shownResultIds: next });
    else setViewerResults(next);
  }
  function writeProperties(operator: OperatorPredicate, next: Record<string, unknown>) {
    if (!canEdit || !session.graph.hasOperator(operator.operatorID)) return;
    const live = session.graph.getOperator(operator.operatorID);
    const merged = { ...live.operatorProperties };
    for (const key of new Set([...Object.keys(operator.operatorProperties), ...Object.keys(next)])) {
      if (JSON.stringify(next[key]) === JSON.stringify(operator.operatorProperties[key])) continue;
      if (Object.hasOwn(next, key)) merged[key] = next[key];
      else delete merged[key];
    }
    session.setProperties(operator.operatorID, merged);
  }

  return (
    <main className="form-view">
      <header className="form-view-heading">
        <div>
          <h1>{workflow.name}</h1>
          <p>
            {authoring
              ? "Choose the inputs and results readers will see."
              : "Fill in the exposed inputs, then run the workflow."}
          </p>
        </div>
        <div>
          <button
            type="button"
            disabled={switching}
            onClick={() => {
              setSwitching(true);
              setError(undefined);
              void (async () => {
                try {
                  if (canEdit) await session.save();
                  onCanvas();
                } catch (reason) {
                  setError(reason instanceof Error ? reason.message : "The workflow could not be saved.");
                } finally {
                  setSwitching(false);
                }
              })();
            }}>
            Open canvas
          </button>
          {canEdit && (
            <button
              type="button"
              aria-pressed={authoring}
              onClick={toggleAuthoring}>
              {authoring ? "Done editing form" : "Edit form"}
            </button>
          )}
        </div>
      </header>
      {error && <p role="alert">{error}</p>}
      {(config.instruction?.body || authoring) && (
        <section className="form-view-instruction">
          {authoring ? (
            <div className="form-view-instruction-heading">
              <label>
                Instruction heading
                <input
                  value={config.instruction?.title ?? ""}
                  placeholder="How to use this"
                  onChange={event =>
                    update({
                      instruction: { body: bindingConfig(session).instruction?.body ?? "", title: event.target.value },
                    })
                  }
                />
              </label>
              <button
                type="button"
                aria-expanded={instructionOpen}
                onClick={() => setInstructionOpen(!instructionOpen)}>
                {instructionOpen ? "Collapse instruction" : "Expand instruction"}
              </button>
            </div>
          ) : (
            <button
              type="button"
              className="form-view-instruction-toggle"
              aria-expanded={instructionOpen}
              onClick={() => setInstructionOpen(!instructionOpen)}>
              {config.instruction?.title || "How to use this"}
            </button>
          )}
          {instructionOpen &&
            (authoring ? (
              <MarkdownEditor
                key="instruction-editor"
                value={config.instruction?.body ?? ""}
                label="Instruction body"
                initiallyEditing
                onChange={body => update({ instruction: { title: bindingConfig(session).instruction?.title, body } })}
                {...{
                  onDraftChange: (body: string) =>
                    update({ instruction: { title: bindingConfig(session).instruction?.title, body } }),
                }}
              />
            ) : (
              <RichText content={config.instruction?.body} />
            ))}
        </section>
      )}
      <h2>Inputs</h2>
      <section
        className="form-view-inputs"
        aria-label="Exposed workflow inputs">
        {config.fields.map((binding, index) => {
          const operator = workflow.content.operators.find(operator => operator.operatorID === binding.operatorID);
          const rootSchema = operator && schemaOf(operator);
          const definition = rootSchema?.properties?.[binding.propertyKey];
          const broken = !operator
            ? "the step it belonged to was removed from the workflow"
            : !definition
              ? `"${binding.propertyKey}" is not a setting of ${operatorLabel(operator, metadata)}`
              : undefined;
          if (broken && !authoring) return null;
          const source = rootSchema ? resolveSchemaReference(asSchema(definition), rootSchema) : asSchema(definition);
          const property = source;
          const schema: OperatorPropertySchema = {
            ...rootSchema,
            title: "",
            type: "object",
            properties: { [binding.propertyKey]: property },
            required: rootSchema?.required?.filter(key => key === binding.propertyKey),
            additionalProperties: true,
          };
          const overrides: Record<string, FormFieldOverride> = {
            [binding.propertyKey]: { displayName: binding.displayName },
          };
          for (const [path, override] of Object.entries(binding.overrides ?? {}))
            overrides[`${binding.propertyKey}.${path}`] = override;
          const presentation: OperatorFormContext["fieldPresentation"] = {
            authoring,
            overrides,
            suppressDescriptions: true,
            canHide: path => path !== binding.propertyKey,
            onRename: (path, displayName) =>
              path === binding.propertyKey
                ? patchBinding(binding.id, { displayName })
                : patchOverride(binding, path.slice(binding.propertyKey.length + 1), { displayName }),
            onHiddenChange: (path, hidden) => {
              if (path !== binding.propertyKey)
                patchOverride(binding, path.slice(binding.propertyKey.length + 1), { hidden });
            },
          };
          const label = binding.displayName || property.title || binding.propertyKey;
          return (
            <section
              className="form-view-input-card"
              key={binding.id}
              draggable={authoring}
              onDragStart={event => {
                if (authoring) event.dataTransfer.setData("text/plain", binding.id);
              }}
              onDragOver={event => {
                if (authoring) event.preventDefault();
              }}
              onDrop={event => {
                if (!authoring) return;
                event.preventDefault();
                const from = bindingConfig(session).fields.findIndex(
                  field => field.id === event.dataTransfer.getData("text/plain")
                );
                reorder(from, index);
              }}>
              {authoring && (
                <header className="form-view-card-controls">
                  <span>{operator ? `From ${operatorLabel(operator, metadata)}` : binding.operatorID}</span>
                  <div>
                    <button
                      type="button"
                      aria-label={`Move ${label} up`}
                      aria-disabled={index === 0}
                      onClick={() => reorder(index, index - 1)}>
                      Move up
                    </button>
                    <button
                      type="button"
                      aria-label={`Move ${label} down`}
                      aria-disabled={index === config.fields.length - 1}
                      onClick={() => reorder(index, index + 1)}>
                      Move down
                    </button>
                    <button
                      type="button"
                      aria-label={`Remove ${label}`}
                      onClick={() =>
                        update({ fields: bindingConfig(session).fields.filter(field => field.id !== binding.id) })
                      }>
                      Remove
                    </button>
                  </div>
                </header>
              )}
              {broken ? (
                <>
                  <h3>{label}</h3>
                  <p className="form-view-broken">{broken}</p>
                </>
              ) : (
                <SchemaForm
                  schema={schema}
                  referenceSchema={rootSchema}
                  value={operator!.operatorProperties}
                  disabled={!canEdit}
                  label={`Form input ${label}`}
                  widgets={widgetsForOperator?.(operator!) ?? propertyForms.widgetsForOperator(operator!)}
                  onChange={next => writeProperties(operator!, next)}
                  context={{
                    ...context,
                    ...propertyForms.contextForOperator(operator!),
                    ...contextForOperator?.(operator!),
                    operatorType: operator!.operatorType,
                    fieldPresentation: presentation,
                  }}
                />
              )}
              {authoring ? (
                <label className="form-view-help-editor">
                  Help for {label}
                  <textarea
                    value={binding.helpText ?? ""}
                    placeholder="Guidance for someone filling this in"
                    onChange={event => patchBinding(binding.id, { helpText: event.target.value || undefined })}
                  />
                </label>
              ) : (
                binding.helpText && <RichText content={binding.helpText} />
              )}
            </section>
          );
        })}
        {!config.fields.some(binding =>
          workflow.content.operators.some(
            operator =>
              operator.operatorID === binding.operatorID && schemaOf(operator)?.properties?.[binding.propertyKey]
          )
        ) &&
          !authoring && <p>This workflow has no inputs to fill in.</p>}
        {authoring && !config.fields.length && <p>No inputs yet. Choose a step and setting below.</p>}
      </section>
      {authoring && (
        <section className="form-view-expose">
          <h2>Add an input</h2>
          <label>
            Operator to expose
            <select
              value={operatorId}
              onChange={event => setOperatorId(event.target.value)}>
              {workflow.content.operators.map(operator => (
                <option
                  key={operator.operatorID}
                  value={operator.operatorID}>
                  {operatorLabel(operator, metadata)}
                </option>
              ))}
            </select>
          </label>
          <label>
            Property to expose
            <select
              value={propertyKey}
              onChange={event => setPropertyKey(event.target.value)}>
              {availableProperties.map(([key, definition]) => (
                <option
                  key={key}
                  value={key}>
                  {asSchema(definition).title || key}
                </option>
              ))}
              {!availableProperties.length && <option value="">No unexposed settings</option>}
            </select>
          </label>
          <button
            type="button"
            disabled={!selectedOperator || !propertyKey}
            onClick={() => {
              if (selectedOperator && propertyKey) expose(selectedOperator, propertyKey, true);
            }}>
            Add input
          </button>
        </section>
      )}
      <section className="form-view-results">
        <h2>Results</h2>
        <p>
          {authoring
            ? "Choose the results this form shows by default."
            : "Choose which available results to view on this page."}
        </p>
        <div className="form-view-result-choices">
          {choices.map(operator => {
            const shown = results.shown.includes(operator.operatorID),
              label = operatorLabel(operator, metadata);
            return (
              <button
                type="button"
                key={operator.operatorID}
                aria-pressed={shown}
                aria-label={`${shown ? "Hide" : "Show"} results from ${label}`}
                onClick={() => toggleResult(operator.operatorID)}>
                {label}
              </button>
            );
          })}
        </div>
        {renderResults?.(results.shown)}
      </section>
      <details className="form-view-preview">
        <summary>Workflow preview</summary>
        {renderPreview ? renderPreview(workflow) : <ReadonlyWorkflowPreview workflow={workflow} />}
        <nav aria-label="Inspect workflow steps">
          {workflow.content.operators.map(operator => (
            <button
              type="button"
              key={operator.operatorID}
              onClick={() => setInspecting(inspecting === operator.operatorID ? undefined : operator.operatorID)}>
              Inspect {operatorLabel(operator, metadata)}
            </button>
          ))}
        </nav>
        {inspecting &&
          (() => {
            const operator = workflow.content.operators.find(operator => operator.operatorID === inspecting);
            const schema = operator && schemaOf(operator);
            return operator && schema ? (
              <section className="form-view-inspector">
                <h3>{operatorLabel(operator, metadata)}</h3>
                <SchemaForm
                  schema={schema}
                  value={operator.operatorProperties}
                  disabled={!authoring}
                  onChange={next => writeProperties(operator, next)}
                  context={{
                    ...context,
                    ...contextForOperator?.(operator),
                    operatorType: operator.operatorType,
                    fieldPresentation: {
                      showExposure: authoring,
                      exposedPaths: config.fields
                        .filter(binding => binding.operatorID === operator.operatorID)
                        .map(binding => binding.propertyKey),
                      onExposedChange: (path, exposed) => expose(operator, path, exposed),
                    },
                  }}
                />
              </section>
            ) : null;
          })()}
      </details>
    </main>
  );
}

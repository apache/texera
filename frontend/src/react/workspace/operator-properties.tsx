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

import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import * as Y from "yjs";
import type { OperatorMetadata, OperatorSchema } from "../../core/workspace/types/operator-schema.interface";
import type { OperatorPredicate } from "../../core/workspace/types/workflow-common.interface";
import { getUiUdfLanguage } from "../../core/workspace/service/workflow-graph/model/workflow-graph";
import { loopVariablesInScope } from "../../core/workspace/util/loop-variable.util";
import type { ApiClient } from "../core/api-client";
import type { SessionUser } from "../core/auth";
import type { GuiConfig } from "../core/runtime-config";
import { SchemaForm, type PropertyWidgetProps, type PropertyWidgets } from "../forms/schema-form";
import type { OperatorPropertySchema } from "../forms/schema";
import type { OperatorFormContext } from "../forms/operator-context";
import { OperatorSupplements } from "../forms/operator-supplements";
import { createResourceCatalog } from "../forms/resource-catalog";
import { createHuggingFaceCatalog } from "../forms/hugging-face";
import { createPresetRepository } from "../forms/presets";
import { createUiUdfParameterSync } from "../forms/udf-parameters-sync";
import type { UiUdfParameterValue } from "../forms/ui-udf-parameters";
import {
  dropInvalidAttributeValues,
  inputSchemasForOperator,
  restoreOperatorInputAttrs,
  setOperatorInputAttrs,
} from "../forms/schema-propagation";
import { CodeareaField } from "../code-editor/code-editor";
import { createCodeEditorAi } from "../code-editor/ai-assistant";
import { createOperatorDebugSource } from "./operator-debug-source";
import type { WorkflowSession } from "./workflow-session";
import type { WorkflowRuntime } from "./workflow-runtime";

export interface OperatorPropertyServices {
  api: ApiClient;
  user: SessionUser;
  config: Readonly<GuiConfig>;
  getToken: () => string | null;
  session: WorkflowSession;
  runtime?: WorkflowRuntime;
  metadata: OperatorMetadata;
}
export interface OperatorPropertiesProps extends OperatorPropertyServices {
  operatorId: string;
}
interface PropertyFormOptions extends OperatorPropertyServices {
  reconfigurationOperatorId?: string;
}
function canReconfigure(options: OperatorPropertyServices, operatorId: string): boolean {
  if (options.session.getSnapshot().workflow.readonly || !options.session.graph.hasOperator(operatorId)) return false;
  if (!options.runtime?.canControl()) return false;
  const operator = options.session.graph.getOperator(operatorId);
  return (
    options.runtime?.getSnapshot().state === "Paused" &&
    options.runtime.getSnapshot().operators[operatorId]?.operatorState !== "Completed" &&
    operatorSchema(options.metadata, operator)?.additionalMetadata.supportReconfiguration === true
  );
}
function canWriteProperties(options: PropertyFormOptions, operatorId: string): boolean {
  return (
    options.session.getSnapshot().canEdit ||
    (options.reconfigurationOperatorId === operatorId && canReconfigure(options, operatorId))
  );
}
function writeProperties(options: PropertyFormOptions, operatorId: string, properties: Record<string, unknown>): void {
  if (!canWriteProperties(options, operatorId)) throw new Error("This operator cannot be edited.");
  if (options.session.getSnapshot().canEdit) options.session.setProperties(operatorId, properties);
  else options.session.shared.yDoc.transact(() => options.session.graph.setOperatorProperty(operatorId, properties));
}
const noopSubscribe = () => () => {};
const emptySnapshot = () => undefined;
function operatorSchema(metadata: OperatorMetadata, operator: OperatorPredicate): OperatorSchema | undefined {
  return (
    metadata.operators.find(
      schema => schema.operatorType === operator.operatorType && schema.operatorVersion === operator.operatorVersion
    ) ?? metadata.operators.find(schema => schema.operatorType === operator.operatorType)
  );
}
function sharedAtPath(session: WorkflowSession, operatorId: string, path: readonly (string | number)[]): unknown {
  let current: unknown = session.graph.getSharedOperatorPropertyType(operatorId);
  for (const key of path) {
    if (current instanceof Y.Map) current = current.get(String(key));
    else if (current instanceof Y.Array && typeof key === "number") current = current.get(key);
    else return undefined;
  }
  return current;
}

/** Shared by canvas properties and Form View; all writes target the same WorkflowSession and Y.Text. */
export function useOperatorPropertyForms(options?: PropertyFormOptions) {
  const latest = useRef(options);
  latest.current = options;
  const workflowState = useSyncExternalStore(
    options?.session.subscribe ?? noopSubscribe,
    options?.session.getSnapshot ?? emptySnapshot,
    options?.session.getSnapshot ?? emptySnapshot
  );
  const runtimeState = useSyncExternalStore(
    options?.runtime?.subscribe ?? noopSubscribe,
    options?.runtime?.getSnapshot ?? emptySnapshot,
    options?.runtime?.getSnapshot ?? emptySnapshot
  );
  const bindings = useRef(new Map<string, ReturnType<typeof createUiUdfParameterSync>>());
  const [parserErrors, setParserErrors] = useState<Record<string, string | undefined>>({});
  const session = options?.session,
    runtime = options?.runtime;
  const documentId = workflowState?.documentId;
  const api = options?.api,
    getToken = options?.getToken,
    userId = options?.user.uid;
  const ids =
    workflowState?.workflow.content.operators
      .map(operator => operator.operatorID)
      .sort()
      .join("\n") ?? "";
  const editableIds = JSON.stringify(
    (workflowState?.workflow.content.operators ?? [])
      .filter(operator => options && canWriteProperties(options, operator.operatorID))
      .map(operator => operator.operatorID)
      .sort()
  );
  const services = useMemo(() => {
    return api && getToken && userId !== undefined
      ? {
          resourceCatalog: createResourceCatalog(api),
          huggingFaceCatalog: createHuggingFaceCatalog({ getToken }),
          presetRepository: createPresetRepository(api),
          ai: createCodeEditorAi(api),
        }
      : undefined;
  }, [api, getToken, userId]);
  const debuggers = useMemo(() => {
    const result = new Map<string, ReturnType<typeof createOperatorDebugSource>>();
    if (session && runtime && ids && documentId)
      for (const operator of ids.split("\n").map(id => session.graph.getOperator(id)))
        if (getUiUdfLanguage(operator) === "python")
          result.set(operator.operatorID, createOperatorDebugSource(session, runtime, operator.operatorID));
    return result;
  }, [session, runtime, ids, documentId]);
  useEffect(() => {
    for (const debug of debuggers.values()) debug.start();
    return () => {
      for (const debug of debuggers.values()) debug.dispose();
    };
  }, [debuggers]);
  useEffect(() => {
    if (!session || !documentId) return;
    for (const operatorId of JSON.parse(editableIds) as string[]) {
      const operator = session.graph.getOperator(operatorId);
      const language = getUiUdfLanguage(operator);
      const code = session.graph.getSharedOperatorPropertyType(operator.operatorID).get("code");
      if (!language || !(code instanceof Y.Text)) continue;
      const getRows = () => {
        const rows = session.graph.getOperator(operator.operatorID).operatorProperties.uiParameters;
        return Array.isArray(rows) ? (rows as UiUdfParameterValue[]) : [];
      };
      const binding = createUiUdfParameterSync({
        code,
        language,
        getRows,
        onRows: rows => {
          const current = latest.current;
          if (
            !current ||
            !canWriteProperties(current, operator.operatorID) ||
            !session.graph.hasOperator(operator.operatorID)
          )
            return;
          writeProperties(current, operator.operatorID, {
            ...session.graph.getOperator(operator.operatorID).operatorProperties,
            uiParameters: rows,
          });
        },
        onError: message =>
          setParserErrors(previous =>
            previous[operator.operatorID] === message ? previous : { ...previous, [operator.operatorID]: message }
          ),
      });
      bindings.current.set(operator.operatorID, binding);
    }
    const current = bindings.current;
    return () => {
      for (const binding of current.values()) binding.dispose();
      current.clear();
    };
  }, [session, editableIds, documentId]);
  const derived = useMemo(() => {
    const result = new Map<
      string,
      { schema?: OperatorPropertySchema; context: OperatorFormContext; errors: string[]; inputKnown: boolean }
    >();
    if (!options || !services) return result;
    for (const operator of workflowState?.workflow.content.operators ?? []) {
      const base = operatorSchema(options.metadata, operator);
      const errors: string[] = [];
      let schema: OperatorPropertySchema | undefined,
        inputKnown = false;
      const input = inputSchemasForOperator({
        operatorId: operator.operatorID,
        graph: options.session.graph,
        dynamicSchema: base,
        outputSchemas: runtimeState?.compilation?.operatorOutputSchemas ?? {},
      });
      errors.push(...input.errors.map(error => `${error.message}: ${error.details}`));
      if (base) {
        try {
          const dynamic = input.inputSchemas
            ? setOperatorInputAttrs(base, input.inputSchemas)
            : base.additionalMetadata.inputPorts.length
              ? restoreOperatorInputAttrs(base)
              : base;
          schema = dynamic.jsonSchema as OperatorPropertySchema;
          inputKnown = !!input.inputSchemas;
        } catch (error) {
          errors.push(error instanceof Error ? error.message : "The operator schema is invalid.");
        }
      } else errors.push(`Operator metadata is unavailable for ${operator.operatorType}.`);
      const compileError = runtimeState?.compilation?.operatorErrors[operator.operatorID];
      if (compileError) errors.push(compileError.message);
      if (parserErrors[operator.operatorID]) errors.push(parserErrors[operator.operatorID]!);
      result.set(operator.operatorID, {
        schema,
        errors,
        inputKnown,
        context: {
          operatorType: operator.operatorType,
          loopVariableNames: loopVariablesInScope(options.session.graph, operator.operatorID),
          inputAttributes: input.inputSchemas,
          ...services,
          selectingFilesFromDatasetsEnabled: options.config.selectingFilesFromDatasetsEnabled,
          userPresetEnabled: options.config.userPresetEnabled,
          addUdfParameter: (name, type) => {
            if (!canWriteProperties(options, operator.operatorID)) throw new Error("This workflow cannot be edited.");
            const binding = bindings.current.get(operator.operatorID);
            if (!binding) throw new Error("The UDF code buffer is unavailable.");
            binding.addParameter(name, type);
          },
        },
      });
    }
    return result;
  }, [options, services, workflowState, runtimeState, parserErrors]);
  useEffect(() => {
    if (!session || !options) return;
    for (const [operatorId, { schema, inputKnown }] of derived) {
      if (!schema || !inputKnown || !canWriteProperties(options, operatorId) || !session.graph.hasOperator(operatorId))
        continue;
      const current = session.graph.getOperator(operatorId).operatorProperties;
      const cleaned = dropInvalidAttributeValues(schema, current);
      if (cleaned.changed) writeProperties(options, operatorId, cleaned.value as Record<string, unknown>);
    }
  }, [session, derived, options]);
  const widgetEnvironment = useMemo(
    () => ({ session, services, debuggers, documentId, cache: new Map<string, PropertyWidgets>() }),
    [session, services, debuggers, documentId]
  );
  const widgetCache = widgetEnvironment.cache;
  function widgetsForOperator(operator: OperatorPredicate): PropertyWidgets {
    const cached = widgetCache.get(operator.operatorID);
    if (cached) return cached;
    const CodeWidget = (props: PropertyWidgetProps) => {
      const current = latest.current;
      if (!current) return null;
      const live = current.session.graph.hasOperator(operator.operatorID)
        ? current.session.graph.getOperator(operator.operatorID)
        : undefined;
      if (!live) return <span role="alert">The operator was removed.</span>;
      const code = sharedAtPath(current.session, operator.operatorID, props.path);
      if (!(code instanceof Y.Text)) return <span role="alert">The shared code buffer is unavailable.</span>;
      return (
        <CodeareaField
          {...props}
          session={{
            operatorId: operator.operatorID,
            code,
            language: getUiUdfLanguage(live) ?? "java",
            title: live.customDisplayName,
            readOnly: !canWriteProperties(current, operator.operatorID),
            awareness: current.session.shared.awareness,
            pythonLanguageServerPort: current.config.pythonLanguageServerPort,
            debug: debuggers.get(operator.operatorID),
            ai: services?.ai,
            onFocus: () =>
              current.session.shared.awareness.setLocalStateField("highlightedOperatorIDs", [operator.operatorID]),
          }}
        />
      );
    };
    const widgets = { codearea: CodeWidget };
    widgetCache.set(operator.operatorID, widgets);
    return widgets;
  }
  return {
    contextForOperator: (operator: OperatorPredicate) => derived.get(operator.operatorID)?.context ?? {},
    schemaForOperator: (operator: OperatorPredicate) => derived.get(operator.operatorID)?.schema,
    errorsForOperator: (operatorId: string) => derived.get(operatorId)?.errors ?? [],
    widgetsForOperator,
  };
}

export function OperatorProperties(props: OperatorPropertiesProps) {
  const [unlockedOperatorId, setUnlockedOperatorId] = useState<string>();
  const [reconfigurationError, setReconfigurationError] = useState<string>();
  const options = { ...props, reconfigurationOperatorId: unlockedOperatorId };
  const forms = useOperatorPropertyForms(options);
  const snapshot = useSyncExternalStore(props.session.subscribe, props.session.getSnapshot, props.session.getSnapshot);
  const execution = useSyncExternalStore(
    props.runtime?.subscribe ?? noopSubscribe,
    props.runtime?.getSnapshot ?? emptySnapshot,
    props.runtime?.getSnapshot ?? emptySnapshot
  );
  useEffect(() => {
    if (unlockedOperatorId && (unlockedOperatorId !== props.operatorId || !canReconfigure(props, unlockedOperatorId)))
      setUnlockedOperatorId(undefined);
  }, [props, unlockedOperatorId, execution]);
  const operator = snapshot.workflow.content.operators.find(operator => operator.operatorID === props.operatorId);
  if (!operator) return <p role="status">Select a step to edit its properties.</p>;
  const schema = forms.schemaForOperator(operator),
    errors = forms.errorsForOperator(operator.operatorID);
  const editable = canWriteProperties(options, operator.operatorID);
  return (
    <section
      className="react-operator-properties"
      aria-label="Step properties">
      <h2>
        {operator.customDisplayName ||
          props.metadata.operators.find(schema => schema.operatorType === operator.operatorType)?.additionalMetadata
            .userFriendlyName ||
          operator.operatorType}
      </h2>
      <OperatorSupplements
        operatorType={operator.operatorType}
        value={operator.operatorProperties}
        inputAttributes={forms.contextForOperator(operator).inputAttributes}
      />
      {reconfigurationError && <p role="alert">{reconfigurationError}</p>}
      {execution?.state === "Paused" &&
        !snapshot.workflow.readonly &&
        !snapshot.canEdit &&
        (editable ? (
          <button
            type="button"
            onClick={() => {
              try {
                if (!canReconfigure(props, operator.operatorID))
                  throw new Error("This operator cannot be reconfigured.");
                const live = props.session.graph.getOperator(operator.operatorID);
                props.runtime!.socket.send("ModifyLogicRequest", {
                  operator: {
                    ...live.operatorProperties,
                    operatorID: live.operatorID,
                    operatorType: live.operatorType,
                  },
                });
                setUnlockedOperatorId(undefined);
                setReconfigurationError(undefined);
              } catch (reason) {
                setReconfigurationError(reason instanceof Error ? reason.message : "Reconfiguration failed.");
              }
            }}>
            Confirm Logic Change
          </button>
        ) : (
          <button
            type="button"
            disabled={!canReconfigure(props, operator.operatorID)}
            onClick={() => {
              setUnlockedOperatorId(operator.operatorID);
              setReconfigurationError(undefined);
            }}>
            Unlock for Logic Change
          </button>
        ))}
      {errors.map((message, index) => (
        <p
          role="alert"
          key={`${index}:${message}`}>
          {message}
        </p>
      ))}
      {schema && (
        <SchemaForm
          schema={schema}
          value={operator.operatorProperties}
          disabled={!editable}
          context={forms.contextForOperator(operator)}
          widgets={forms.widgetsForOperator(operator)}
          onChange={properties => {
            writeProperties(options, operator.operatorID, properties);
          }}
        />
      )}
    </section>
  );
}

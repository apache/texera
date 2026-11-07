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

import { useMemo, useState, useSyncExternalStore } from "react";
import * as Y from "yjs";
import type { OperatorMetadata } from "../../core/workspace/types/operator-schema.interface";
import type {
  PartitionInfo,
  PortProperty,
  PortDescription,
} from "../../core/workspace/types/workflow-common.interface";
import { isSink } from "../../core/workspace/service/workflow-graph/model/workflow-graph";
import type { WorkflowSession } from "./workflow-session";

export interface OperatorControlsProps {
  session: WorkflowSession;
  metadata: OperatorMetadata;
  operatorId: string;
}
function validPartition(value: PartitionInfo): boolean {
  if (!value || typeof value !== "object") return false;
  const attributes = (value: unknown) => Array.isArray(value) && value.every(name => typeof name === "string");
  switch (value.type) {
    case "none":
    case "single":
    case "broadcast":
      return true;
    case "hash":
      return attributes(value.hashAttributeNames);
    case "range":
      return (
        attributes(value.rangeAttributeNames) &&
        Number.isSafeInteger(value.rangeMin) &&
        Number.isSafeInteger(value.rangeMax)
      );
    default:
      return false;
  }
}

/** Port and cache actions use the existing graph's shared structures and one undo transaction per action. */
export function createOperatorControls(session: WorkflowSession, metadata: OperatorMetadata, operatorId: string) {
  const operator = () => session.graph.getOperator(operatorId);
  const schema = () => metadata.operators.find(schema => schema.operatorType === operator().operatorType);
  const mutate = (action: () => void) => {
    if (!session.getSnapshot().canEdit) throw new Error("This workflow cannot be edited.");
    session.graph.assertOperatorExists(operatorId);
    session.shared.yDoc.transact(action);
  };
  const dynamic = (input: boolean) =>
    input
      ? operator().dynamicInputPorts ?? schema()?.additionalMetadata.dynamicInputPorts
      : operator().dynamicOutputPorts ?? schema()?.additionalMetadata.dynamicOutputPorts;
  return {
    rename(name: string) {
      mutate(() => session.setDisplayName(operatorId, name));
    },
    advanced(show: boolean) {
      mutate(() => session.graph.getSharedOperatorType(operatorId).set("showAdvanced", show));
    },
    reuse(enabled: boolean) {
      mutate(() => {
        if (enabled) session.graph.markReuseResult(operatorId);
        else session.graph.removeMarkReuseResult(operatorId);
      });
    },
    addPort(input: boolean, disallowMultiInputs?: boolean) {
      mutate(() => {
        if (!dynamic(input))
          throw new Error(`operator ${operatorId} does not have dynamic ${input ? "input" : "output"} ports`);
        if (!input && disallowMultiInputs !== undefined)
          throw new Error("disallowMultiInputs cannot be specified on an output port");
        const ports = input ? operator().inputPorts : operator().outputPorts;
        const prefix = input ? "input-" : "output-";
        let suffix = ports.length;
        while (ports.some(port => port.portID === `${prefix}${suffix}`)) suffix++;
        session.graph.addPort(
          operatorId,
          {
            portID: `${prefix}${suffix}`,
            displayName: "",
            ...(input ? { disallowMultiInputs } : {}),
            isDynamicPort: true,
            dependencies: [],
          },
          input
        );
      });
    },
    removeLastPort(input: boolean) {
      mutate(() => {
        const ports = input ? operator().inputPorts : operator().outputPorts,
          last = ports.at(-1);
        if (!dynamic(input) || !last?.isDynamicPort) throw new Error("Only the last dynamic port can be removed.");
        // Removing a JointJS port removes its attached links; keep the shared graph in the same state atomically.
        for (const link of session.graph.getAllLinks()) {
          const endpoint = input ? link.target : link.source;
          if (endpoint.operatorID === operatorId && endpoint.portID === last.portID)
            session.graph.deleteLinkWithID(link.linkID);
        }
        session.graph.removePort(operatorId, input);
      });
    },
    renamePort(portId: string, name: string) {
      mutate(() => {
        const shared = session.graph.getSharedPortDescriptionType({ operatorID: operatorId, portID: portId });
        if (!shared) throw new Error("The operator port does not exist.");
        const text = shared.get("displayName");
        if (text instanceof Y.Text) {
          text.delete(0, text.length);
          text.insert(0, name);
        } else shared.set("displayName", new Y.Text(name));
      });
    },
    setPort(portId: string, property: PortProperty) {
      mutate(() => {
        if (
          !schema()?.additionalMetadata.allowPortCustomization ||
          !operator().inputPorts.some(port => port.portID === portId)
        )
          throw new Error("This input port cannot be customized.");
        if (!validPartition(property.partitionInfo)) throw new Error("Invalid partition settings.");
        if (
          !Array.isArray(property.dependencies) ||
          property.dependencies.some(
            item => !item || !Number.isSafeInteger(item.id) || item.id < 0 || typeof item.internal !== "boolean"
          )
        )
          throw new Error("Invalid dependency settings.");
        session.graph.setPortProperty({ operatorID: operatorId, portID: portId }, property);
      });
    },
  };
}

export function OperatorControls({ session, metadata, operatorId }: OperatorControlsProps) {
  const snapshot = useSyncExternalStore(session.subscribe, session.getSnapshot, session.getSnapshot);
  const actions = useMemo(() => createOperatorControls(session, metadata, operatorId), [session, metadata, operatorId]);
  const [error, setError] = useState<string>();
  const operator = snapshot.workflow.content.operators.find(operator => operator.operatorID === operatorId);
  if (!operator) return null;
  const schema = metadata.operators.find(schema => schema.operatorType === operator.operatorType);
  const disabled = !snapshot.canEdit;
  const act = (action: () => void) => {
    try {
      action();
      setError(undefined);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "The operator could not be updated.");
    }
  };
  const renderPorts = (ports: PortDescription[], input: boolean) => {
    const dynamic = input
      ? operator.dynamicInputPorts ?? schema?.additionalMetadata.dynamicInputPorts
      : operator.dynamicOutputPorts ?? schema?.additionalMetadata.dynamicOutputPorts;
    return (
      <section aria-label={input ? "Input ports" : "Output ports"}>
        <h3>{input ? "Input ports" : "Output ports"}</h3>
        {dynamic && (
          <div>
            <button
              type="button"
              disabled={disabled}
              onClick={() => act(() => actions.addPort(input))}>
              Add {input ? "input" : "output"} port
            </button>
            <button
              type="button"
              disabled={disabled || !ports.at(-1)?.isDynamicPort}
              onClick={() => act(() => actions.removeLastPort(input))}>
              Remove last {input ? "input" : "output"} port
            </button>
          </div>
        )}
        {ports.map((port, index) => {
          const label = `${input ? "Input" : "Output"} ${index + 1}`;
          const partition = port.partitionRequirement ?? { type: "none" as const };
          const dependencies = port.dependencies ?? [];
          const change = (patch: Partial<PortProperty>) =>
            act(() => actions.setPort(port.portID, { partitionInfo: partition, dependencies, ...patch }));
          return (
            <fieldset
              key={port.portID}
              disabled={disabled}>
              <legend>{label}</legend>
              <label>
                {label} name
                <input
                  type="text"
                  value={port.displayName ?? ""}
                  onChange={event => act(() => actions.renamePort(port.portID, event.target.value))}
                />
              </label>
              {input && schema?.additionalMetadata.allowPortCustomization && (
                <>
                  <label>
                    {label} partition type
                    <select
                      value={partition.type}
                      onChange={event => {
                        const type = event.target.value as PartitionInfo["type"];
                        change({
                          partitionInfo:
                            type === "hash"
                              ? { type, hashAttributeNames: [] }
                              : type === "range"
                                ? { type, rangeAttributeNames: [], rangeMin: 0, rangeMax: 0 }
                                : { type },
                        });
                      }}>
                      {["none", "hash", "range", "single", "broadcast"].map(type => (
                        <option key={type}>{type}</option>
                      ))}
                    </select>
                  </label>
                  {(partition.type === "hash" || partition.type === "range") && (
                    <label>
                      {label} {partition.type} attributes
                      <input
                        type="text"
                        value={(partition.type === "hash"
                          ? partition.hashAttributeNames
                          : partition.rangeAttributeNames
                        ).join(",")}
                        onChange={event => {
                          const names = event.target.value.split(",").map(name => name.trim());
                          change({
                            partitionInfo:
                              partition.type === "hash"
                                ? { ...partition, hashAttributeNames: names }
                                : { ...partition, rangeAttributeNames: names },
                          });
                        }}
                      />
                    </label>
                  )}
                  {partition.type === "range" &&
                    (["rangeMin", "rangeMax"] as const).map(key => (
                      <label key={key}>
                        {label} {key === "rangeMin" ? "range minimum" : "range maximum"}
                        <input
                          type="number"
                          step="1"
                          value={partition[key]}
                          onChange={event =>
                            change({
                              partitionInfo: {
                                ...partition,
                                [key]: event.currentTarget.value === "" ? NaN : Number(event.currentTarget.value),
                              },
                            })
                          }
                        />
                      </label>
                    ))}
                  <div>
                    {dependencies.map((dependency, row) => (
                      <div key={row}>
                        <label>
                          {label} dependency {row + 1} id
                          <input
                            type="number"
                            min="0"
                            step="1"
                            value={dependency.id}
                            onChange={event =>
                              change({
                                dependencies: dependencies.map((value, index) =>
                                  index === row
                                    ? {
                                        ...value,
                                        id: event.currentTarget.value === "" ? NaN : Number(event.currentTarget.value),
                                      }
                                    : value
                                ),
                              })
                            }
                          />
                        </label>
                        <label>
                          <input
                            type="checkbox"
                            checked={dependency.internal}
                            onChange={event =>
                              change({
                                dependencies: dependencies.map((value, index) =>
                                  index === row ? { ...value, internal: event.target.checked } : value
                                ),
                              })
                            }
                          />
                          {label} dependency {row + 1} internal
                        </label>
                        <button
                          type="button"
                          onClick={() => change({ dependencies: dependencies.filter((_, index) => index !== row) })}>
                          Remove {label} dependency {row + 1}
                        </button>
                      </div>
                    ))}
                    <button
                      type="button"
                      onClick={() => change({ dependencies: [...dependencies, { id: 0, internal: false }] })}>
                      Add {label} dependency
                    </button>
                  </div>
                </>
              )}
            </fieldset>
          );
        })}
      </section>
    );
  };
  return (
    <details className="react-operator-controls">
      <summary>Operator and port settings</summary>
      {error && <p role="alert">{error}</p>}
      <label>
        Operator name
        <input
          type="text"
          disabled={disabled}
          value={operator.customDisplayName ?? ""}
          onChange={event => act(() => actions.rename(event.target.value))}
        />
      </label>
      <label>
        <input
          type="checkbox"
          disabled={disabled || isSink(operator)}
          checked={operator.markedForReuse ?? false}
          onChange={event => act(() => actions.reuse(event.target.checked))}
        />
        Reuse cached result
      </label>
      <label>
        <input
          type="checkbox"
          disabled={disabled}
          checked={operator.showAdvanced}
          onChange={event => act(() => actions.advanced(event.target.checked))}
        />
        Show advanced settings
      </label>
      {renderPorts(operator.inputPorts, true)}
      {renderPorts(operator.outputPorts, false)}
    </details>
  );
}

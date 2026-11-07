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

import { afterEach, describe, expect, it } from "vitest";
import { copySelection, pasteSelection } from "./workflow-clipboard";
import { WorkflowSession } from "./workflow-session";
import { SharedModel } from "../../core/workspace/service/workflow-graph/model/shared-model";
import { ExecutionMode } from "../../core/common/type/workflow";
const all: WorkflowSession[] = [];
function setup(readonly = false) {
  const session = new WorkflowSession(
    {
      wid: 1,
      name: "Test",
      description: "",
      isPublished: 0,
      readonly,
      creationTime: 1,
      lastModifiedTime: 1,
      content: {
        operators: ["a", "b"].map(operatorID => ({
          operatorID,
          operatorType: "Test",
          operatorVersion: "1",
          operatorProperties: { code: "日本語" },
          inputPorts: [{ portID: "input-0" }],
          outputPorts: [{ portID: "output-0" }],
          showAdvanced: false,
        })),
        operatorPositions: { a: { x: 0, y: 0 }, b: { x: 100, y: 0 } },
        links: [
          {
            linkID: "ab",
            source: { operatorID: "a", portID: "output-0" },
            target: { operatorID: "b", portID: "input-0" },
          },
        ],
        commentBoxes: [],
        settings: { dataTransferBatchSize: 400, executionMode: ExecutionMode.PIPELINED },
      },
    },
    { shared: new SharedModel(1, undefined, undefined, { connect: false }), save: async value => value, autosaveMs: 0 }
  );
  all.push(session);
  return session;
}
afterEach(() => all.splice(0).forEach(item => item.dispose()));
describe("workflow clipboard", () => {
  it("round trips the existing clipboard JSON, remaps identities/links and uses non-overlapping offsets", () => {
    const session = setup();
    let id = 0;
    const text = copySelection(session, ["a", "b", "ab"]);
    const ids = pasteSelection(session, text, () => String(++id));
    expect(ids).toHaveLength(2);
    const copied = session.graph.getOperator(ids[0]);
    expect(copied.operatorProperties).toEqual({ code: "日本語" });
    expect(session.getSnapshot().workflow.content.operatorPositions[ids[0]]).toEqual({ x: 20, y: 20 });
    expect(session.graph.getAllLinks().at(-1)).toMatchObject({
      source: { operatorID: ids[0], portID: "output-0" },
      target: { operatorID: ids[1], portID: "input-0" },
    });
    const next = pasteSelection(session, text, () => String(++id));
    expect(session.getSnapshot().workflow.content.operatorPositions[next[0]]).toEqual({ x: 40, y: 40 });
    session.undo();
    expect(session.graph.getAllOperators()).toHaveLength(4);
  });
  it("drops dangling selected links and preserves the original clipboard payload", () => {
    const session = setup();
    const text = copySelection(session, ["a", "ab"]);
    pasteSelection(session, text);
    expect(session.graph.getAllLinks()).toHaveLength(1);
    expect(JSON.parse(text).operators[0].operatorID).toBe("a");
  });
  it("rejects malformed positions/ports and read-only edits before adding any elements", () => {
    const session = setup();
    const data = JSON.parse(copySelection(session, ["a"]));
    data.operatorPositions.a.x = null;
    expect(() => pasteSelection(session, JSON.stringify(data))).toThrow(/position/i);
    data.operatorPositions.a.x = 0;
    data.operators[0].outputPorts = [{}];
    expect(() => pasteSelection(session, JSON.stringify(data))).toThrow(/port/i);
    expect(() => pasteSelection(session, "null")).toThrow(/clipboard/i);
    expect(session.graph.getAllOperators()).toHaveLength(2);
    const readonly = setup(true);
    expect(() => pasteSelection(readonly, copySelection(session, ["a"]))).toThrow(/read.only/i);
    expect(readonly.graph.getAllOperators()).toHaveLength(2);
  });
});

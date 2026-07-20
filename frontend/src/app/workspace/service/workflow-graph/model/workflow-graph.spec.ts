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

import {
  mockCommentBox,
  mockMultiInputOutputPredicate,
  mockResultPredicate,
  mockScanPredicate,
  mockScanResultLink,
  mockScanSentimentLink,
  mockSentimentPredicate,
  mockSentimentResultLink,
} from "./mock-workflow-data";
import { WorkflowGraph } from "./workflow-graph";
import { Observable } from "rxjs";
import { Comment, OperatorLink, PortDescription, PortProperty } from "../../../types/workflow-common.interface";

describe("WorkflowGraph", () => {
  let workflowGraph: WorkflowGraph;

  beforeEach(() => {
    workflowGraph = new WorkflowGraph();
  });

  afterEach(() => {
    // Tear down the underlying shared model (awareness / websocket provider) to avoid leaks across tests.
    // No test leaves the per-test workflowGraph destroyed (loadNewYModel installs a fresh model; the one
    // destroyYModel() call is on a separate local graph), so let any teardown failure surface here.
    workflowGraph.destroyYModel();
  });

  it("should have an empty graph from the beginning", () => {
    expect(workflowGraph.getAllOperators().length).toEqual(0);
    expect(workflowGraph.getAllLinks().length).toEqual(0);
  });

  it("should load an existing graph properly", () => {
    workflowGraph = new WorkflowGraph(
      [mockScanPredicate, mockSentimentPredicate, mockResultPredicate],
      [mockScanSentimentLink, mockSentimentResultLink]
    );
    expect(workflowGraph.getAllOperators().length).toEqual(3);
    expect(workflowGraph.getAllLinks().length).toEqual(2);
  });

  it("should add an operator and get it properly", () => {
    workflowGraph.addOperator(mockScanPredicate);
    expect(workflowGraph.getOperator(mockScanPredicate.operatorID)).toBeTruthy();
    expect(workflowGraph.getAllOperators().length).toEqual(1);
    expect(workflowGraph.getAllOperators()[0]).toEqual(mockScanPredicate);
  });

  it("should return undefined when get an operator with a nonexist operator ID", () => {
    expect(() => {
      workflowGraph.getOperator("nonexist");
    }).toThrowError(new RegExp("does not exist"));
  });

  it("should throw an error when trying to add an operator with an existing operator ID", () => {
    expect(() => {
      workflowGraph.addOperator(mockScanPredicate);
      workflowGraph.addOperator(mockScanPredicate);
    }).toThrowError(new RegExp("already exists"));
  });

  it("should delete an operator properly", () => {
    workflowGraph.addOperator(mockScanPredicate);
    workflowGraph.deleteOperator(mockScanPredicate.operatorID);
    expect(workflowGraph.getAllOperators().length).toBe(0);
  });

  it("should throw an error when tring to delete an operator that doesn't exist", () => {
    expect(() => {
      workflowGraph.deleteOperator("nonexist");
    }).toThrowError(new RegExp("does not exist"));
  });

  it("should add and get a link properly", () => {
    workflowGraph.addOperator(mockScanPredicate);
    workflowGraph.addOperator(mockResultPredicate);
    workflowGraph.addLink(mockScanResultLink);

    expect(workflowGraph.getLinkWithID(mockScanResultLink.linkID)).toEqual(mockScanResultLink);
    expect(workflowGraph.getLink(mockScanResultLink.source, mockScanResultLink.target)).toEqual(mockScanResultLink);
    expect(workflowGraph.getAllLinks().length).toEqual(1);
  });

  it("should throw an error when try to add a link with an existingID", () => {
    workflowGraph.addOperator(mockScanPredicate);
    workflowGraph.addOperator(mockResultPredicate);
    workflowGraph.addOperator(mockSentimentPredicate);
    workflowGraph.addLink(mockScanResultLink);

    // create a mock link with modified target
    const mockLink = {
      ...mockScanResultLink,
      target: {
        operatorID: mockSentimentPredicate.operatorID,
        portID: mockSentimentPredicate.inputPorts[0].portID,
      },
    };

    expect(() => {
      workflowGraph.addLink(mockLink);
    }).toThrowError(new RegExp("already exists"));
  });

  it("should throw an error when try to add a link with exising source and target but different ID", () => {
    workflowGraph.addOperator(mockScanPredicate);
    workflowGraph.addOperator(mockResultPredicate);
    workflowGraph.addOperator(mockSentimentPredicate);
    workflowGraph.addLink(mockScanResultLink);

    // create a mock link with modified ID
    const mockLink = {
      ...mockScanResultLink,
      linkID: "new-link-id",
    };

    expect(() => {
      workflowGraph.addLink(mockLink);
    }).toThrowError(new RegExp("already exists"));
  });

  it("should return undefined when tring to get a nonexist link by link ID", () => {
    expect(() => {
      workflowGraph.getLinkWithID("nonexist");
    }).toThrowError(new RegExp("does not exist"));
  });

  it("should throw an error when tring to get a nonexist link by link source and target", () => {
    expect(() => {
      workflowGraph.getLink(
        { operatorID: "source", portID: "source port" },
        { operatorID: "target", portID: "taret port" }
      );
    }).toThrowError(new RegExp("does not exist"));
  });

  it("should delete a link by ID properly", () => {
    workflowGraph.addOperator(mockScanPredicate);
    workflowGraph.addOperator(mockResultPredicate);
    workflowGraph.addLink(mockScanResultLink);
    workflowGraph.deleteLinkWithID(mockScanResultLink.linkID);

    expect(workflowGraph.getAllLinks().length).toEqual(0);
  });

  it("should delete a link by source and target properly", () => {
    workflowGraph.addOperator(mockScanPredicate);
    workflowGraph.addOperator(mockResultPredicate);
    workflowGraph.addLink(mockScanResultLink);
    workflowGraph.deleteLink(mockScanResultLink.source, mockScanResultLink.target);

    expect(workflowGraph.getAllLinks().length).toEqual(0);
  });

  it("should throw an error when trying to delete a link (by ID) that doesn't exist", () => {
    expect(() => {
      workflowGraph.deleteLinkWithID(mockScanResultLink.linkID);
    }).toThrowError(new RegExp("does not exist"));
  });

  it("should throw an error when trying to delete a link (by source and target) that doesn't exist", () => {
    expect(() => {
      workflowGraph.deleteLink(
        { operatorID: "source", portID: "source port" },
        { operatorID: "target", portID: "taret port" }
      );
    }).toThrowError(new RegExp("does not exist"));
  });

  it("should set the operator property(attributes) properly", () => {
    workflowGraph.addOperator(mockScanPredicate);

    const testProperty = { tableName: "testTable" };
    workflowGraph.setOperatorProperty(mockScanPredicate.operatorID, testProperty);

    const operator = workflowGraph.getOperator(mockScanPredicate.operatorID);
    if (!operator) {
      throw new Error("test fails: operator is undefined");
    }
    expect(operator.operatorProperties).toEqual(testProperty);
  });

  it("should throw an error when trying to set the property of an nonexist operator", () => {
    expect(() => {
      const testProperty = { tableName: "testTable" };
      workflowGraph.setOperatorProperty(mockScanPredicate.operatorID, testProperty);
    }).toThrowError(new RegExp("doesn't exist"));
  });

  it("it should get input links of the certain operator correctly", () => {
    workflowGraph.addOperator(mockScanPredicate);
    workflowGraph.addOperator(mockResultPredicate);
    workflowGraph.addLink(mockScanResultLink);
    expect(workflowGraph.getInputLinksByOperatorId("3").length).toEqual(1);
  });

  it("it should get output links of the certain operator correctly", () => {
    workflowGraph.addOperator(mockScanPredicate);
    workflowGraph.addOperator(mockResultPredicate);
    workflowGraph.addLink(mockScanResultLink);
    expect(workflowGraph.getOutputLinksByOperatorId("1").length).toEqual(1);
  });

  it("should disable and enable an operator", () => {
    workflowGraph.addOperator(mockScanPredicate);
    workflowGraph.addOperator(mockResultPredicate);
    workflowGraph.disableOperator(mockScanPredicate.operatorID);

    expect(workflowGraph.isOperatorDisabled(mockScanPredicate.operatorID)).toBe(true);
    expect(workflowGraph.isOperatorDisabled(mockResultPredicate.operatorID)).toBe(false);
    expect(workflowGraph.getDisabledOperators().size).toEqual(1);

    workflowGraph.enableOperator(mockScanPredicate.operatorID);
    expect(workflowGraph.isOperatorDisabled(mockScanPredicate.operatorID)).toBe(false);
    expect(workflowGraph.getDisabledOperators().size).toEqual(0);
  });

  it("should calculate if link is disabled based on the disabled operator", () => {
    workflowGraph.addOperator(mockScanPredicate);
    workflowGraph.addOperator(mockResultPredicate);
    workflowGraph.addLink(mockScanResultLink);
    workflowGraph.disableOperator(mockScanPredicate.operatorID);

    expect(workflowGraph.isLinkEnabled(mockScanResultLink.linkID)).toBe(false);
    expect(workflowGraph.getAllEnabledLinks().length).toEqual(0);
  });

  it("should set and un-set viewing result status of an operator", () => {
    workflowGraph.addOperator(mockScanPredicate);
    workflowGraph.addOperator(mockResultPredicate);
    workflowGraph.setViewOperatorResult(mockScanPredicate.operatorID);

    expect(workflowGraph.isViewingResult(mockScanPredicate.operatorID)).toBe(true);
    expect(workflowGraph.isViewingResult(mockResultPredicate.operatorID)).toBe(false);
    expect(workflowGraph.getOperatorsToViewResult().size).toEqual(1);

    workflowGraph.unsetViewOperatorResult(mockScanPredicate.operatorID);
    expect(workflowGraph.isViewingResult(mockScanPredicate.operatorID)).toBe(false);
    expect(workflowGraph.getDisabledOperators().size).toEqual(0);
  });

  it("should ignore set the view result of the sink operator", () => {
    workflowGraph.addOperator(mockScanPredicate);
    workflowGraph.addOperator(mockResultPredicate);
    workflowGraph.setViewOperatorResult(mockResultPredicate.operatorID);

    expect(workflowGraph.isViewingResult(mockScanPredicate.operatorID)).toBe(false);
    expect(workflowGraph.isViewingResult(mockResultPredicate.operatorID)).toBe(false);
    expect(workflowGraph.getOperatorsToViewResult().size).toEqual(0);

    workflowGraph.unsetViewOperatorResult(mockResultPredicate.operatorID);
    expect(workflowGraph.isViewingResult(mockResultPredicate.operatorID)).toBe(false);
    expect(workflowGraph.getOperatorsToViewResult().size).toEqual(0);
  });

  describe("sync flags and shared model lifecycle", () => {
    it("should default to syncing both graphs and toggle the sync flags", () => {
      expect(workflowGraph.getSyncTexeraGraph()).toBe(true);
      expect(workflowGraph.getSyncJointGraph()).toBe(true);

      workflowGraph.setSyncTexeraGraph(false);
      workflowGraph.setSyncJointGraph(false);

      expect(workflowGraph.getSyncTexeraGraph()).toBe(false);
      expect(workflowGraph.getSyncJointGraph()).toBe(false);
    });

    it("should expose the shared awareness and forward awareness updates without a user", () => {
      expect(workflowGraph.getSharedModelAwareness()).toBe(workflowGraph.sharedModel.awareness);
      // no user configured, so this is a no-op but must not throw
      expect(() => workflowGraph.updateSharedModelAwareness("isActive", true)).not.toThrow();
    });

    it("should load a fresh Y model, discarding previous operators and emitting the loaded event", () => {
      workflowGraph.addOperator(mockScanPredicate);
      expect(workflowGraph.getAllOperators().length).toEqual(1);

      const oldSharedModel = workflowGraph.sharedModel;
      let loaded = false;
      const sub = workflowGraph.newYDocLoadedSubject.subscribe(() => (loaded = true));

      workflowGraph.loadNewYModel();

      expect(loaded).toBe(true);
      expect(workflowGraph.sharedModel).not.toBe(oldSharedModel);
      expect(workflowGraph.getAllOperators().length).toEqual(0);
      sub.unsubscribe();
    });

    it("should run a bundled action as an atomic transaction that mutates the graph", () => {
      workflowGraph.bundleActions(() => {
        workflowGraph.addOperator(mockScanPredicate);
        workflowGraph.addOperator(mockResultPredicate);
      });
      expect(workflowGraph.getAllOperators().length).toEqual(2);
    });
  });

  describe("shared-editing accessors", () => {
    it("should return the shared operator property type", () => {
      workflowGraph.addOperator(mockScanPredicate);
      const propType = workflowGraph.getSharedOperatorPropertyType(mockScanPredicate.operatorID);
      expect(propType).toBeDefined();
      expect(propType.toJSON()).toEqual(mockScanPredicate.operatorProperties);
    });

    it("should return the shared port description type for input and output ports", () => {
      workflowGraph.addOperator(mockSentimentPredicate);
      const inputType = workflowGraph.getSharedPortDescriptionType({ operatorID: "2", portID: "input-0" });
      const outputType = workflowGraph.getSharedPortDescriptionType({ operatorID: "2", portID: "output-0" });
      expect(inputType).toBeDefined();
      expect(outputType).toBeDefined();
      expect((inputType as any).toJSON().portID).toEqual("input-0");
      expect((outputType as any).toJSON().portID).toEqual("output-0");
    });

    it("should return undefined for a shared port description of a nonexistent port", () => {
      workflowGraph.addOperator(mockSentimentPredicate);
      expect(workflowGraph.getSharedPortDescriptionType({ operatorID: "2", portID: "input-99" })).toBeUndefined();
    });

    it("should throw when getting the shared operator type of a nonexistent operator", () => {
      expect(() => workflowGraph.getSharedOperatorType("nonexist")).toThrowError(new RegExp("doesn't exist"));
    });
  });

  describe("operator version and reuse-cache state", () => {
    it("should change the operator version and emit the change", () => {
      workflowGraph.addOperator(mockScanPredicate);
      let emitted: { operatorID: string; newOperatorVersion: string } | undefined;
      const sub = workflowGraph.getOperatorVersionChangedStream().subscribe(v => (emitted = v));

      workflowGraph.changeOperatorVersion(mockScanPredicate.operatorID, "scan-v2");

      expect(workflowGraph.getOperator(mockScanPredicate.operatorID).operatorVersion).toEqual("scan-v2");
      expect(emitted).toEqual({ operatorID: mockScanPredicate.operatorID, newOperatorVersion: "scan-v2" });
      sub.unsubscribe();
    });

    it("should not emit or change when the operator version is unchanged", () => {
      workflowGraph.addOperator(mockScanPredicate);
      let emitted = false;
      const sub = workflowGraph.getOperatorVersionChangedStream().subscribe(() => (emitted = true));

      workflowGraph.changeOperatorVersion(mockScanPredicate.operatorID, mockScanPredicate.operatorVersion);

      expect(emitted).toBe(false);
      sub.unsubscribe();
    });

    it("should mark and un-mark an operator for reuse-cache result", () => {
      workflowGraph.addOperator(mockScanPredicate);
      expect(workflowGraph.isMarkedForReuseResult(mockScanPredicate.operatorID)).toBe(false);

      workflowGraph.markReuseResult(mockScanPredicate.operatorID);
      expect(workflowGraph.isMarkedForReuseResult(mockScanPredicate.operatorID)).toBe(true);
      expect(workflowGraph.getOperatorsMarkedForReuseResult().size).toEqual(1);

      // marking again is a no-op
      workflowGraph.markReuseResult(mockScanPredicate.operatorID);
      expect(workflowGraph.getOperatorsMarkedForReuseResult().size).toEqual(1);

      workflowGraph.removeMarkReuseResult(mockScanPredicate.operatorID);
      expect(workflowGraph.isMarkedForReuseResult(mockScanPredicate.operatorID)).toBe(false);
      expect(workflowGraph.getOperatorsMarkedForReuseResult().size).toEqual(0);

      // removing again is a no-op
      workflowGraph.removeMarkReuseResult(mockScanPredicate.operatorID);
      expect(workflowGraph.getOperatorsMarkedForReuseResult().size).toEqual(0);
    });

    it("should ignore reuse-cache marking of a sink operator", () => {
      workflowGraph.addOperator(mockResultPredicate);
      workflowGraph.markReuseResult(mockResultPredicate.operatorID);
      expect(workflowGraph.isMarkedForReuseResult(mockResultPredicate.operatorID)).toBe(false);
      expect(workflowGraph.getOperatorsMarkedForReuseResult().size).toEqual(0);
    });

    it("should throw on reuse-cache queries against a nonexistent operator", () => {
      // these guards resolve the operator via getOperator, which throws "does not exist"
      expect(() => workflowGraph.markReuseResult("nonexist")).toThrowError(new RegExp("does not exist"));
      expect(() => workflowGraph.removeMarkReuseResult("nonexist")).toThrowError(new RegExp("does not exist"));
      expect(() => workflowGraph.isMarkedForReuseResult("nonexist")).toThrowError(new RegExp("does not exist"));
    });
  });

  describe("disabled / view-result guard branches", () => {
    it("should be a no-op when disabling an already-disabled operator and enabling an already-enabled one", () => {
      workflowGraph.addOperator(mockScanPredicate);

      workflowGraph.disableOperator(mockScanPredicate.operatorID);
      workflowGraph.disableOperator(mockScanPredicate.operatorID); // already disabled -> early return
      expect(workflowGraph.isOperatorDisabled(mockScanPredicate.operatorID)).toBe(true);

      // freshly-added operator is already enabled -> enableOperator early-returns
      workflowGraph.addOperator(mockResultPredicate);
      workflowGraph.enableOperator(mockResultPredicate.operatorID);
      expect(workflowGraph.isOperatorDisabled(mockResultPredicate.operatorID)).toBe(false);
    });

    it("should be a no-op when re-setting an already-set view result and un-setting an already-unset one", () => {
      workflowGraph.addOperator(mockScanPredicate);

      workflowGraph.setViewOperatorResult(mockScanPredicate.operatorID);
      workflowGraph.setViewOperatorResult(mockScanPredicate.operatorID); // already set -> early return
      expect(workflowGraph.isViewingResult(mockScanPredicate.operatorID)).toBe(true);

      workflowGraph.unsetViewOperatorResult(mockScanPredicate.operatorID);
      workflowGraph.unsetViewOperatorResult(mockScanPredicate.operatorID); // already unset -> early return
      expect(workflowGraph.isViewingResult(mockScanPredicate.operatorID)).toBe(false);
    });

    it("should throw on disabled / view-result queries against nonexistent operators", () => {
      // these guards resolve the operator via getOperator, which throws "does not exist"
      expect(() => workflowGraph.disableOperator("nonexist")).toThrowError(new RegExp("does not exist"));
      expect(() => workflowGraph.enableOperator("nonexist")).toThrowError(new RegExp("does not exist"));
      expect(() => workflowGraph.isOperatorDisabled("nonexist")).toThrowError(new RegExp("does not exist"));
      expect(() => workflowGraph.setViewOperatorResult("nonexist")).toThrowError(new RegExp("does not exist"));
      expect(() => workflowGraph.unsetViewOperatorResult("nonexist")).toThrowError(new RegExp("does not exist"));
      expect(() => workflowGraph.isViewingResult("nonexist")).toThrowError(new RegExp("does not exist"));
    });

    it("should report only enabled operators from getAllEnabledOperators", () => {
      workflowGraph.addOperator(mockScanPredicate);
      workflowGraph.addOperator(mockResultPredicate);
      workflowGraph.disableOperator(mockScanPredicate.operatorID);

      const enabled = workflowGraph.getAllEnabledOperators();
      expect(enabled.length).toEqual(1);
      expect(enabled[0].operatorID).toEqual(mockResultPredicate.operatorID);
    });
  });

  describe("ports", () => {
    it("should add and remove input and output ports", () => {
      workflowGraph.addOperator(mockScanPredicate); // no input ports, one output port

      const newInputPort: PortDescription = { portID: "input-0" };
      const newOutputPort: PortDescription = { portID: "output-1" };

      workflowGraph.addPort(mockScanPredicate.operatorID, newInputPort, true);
      workflowGraph.addPort(mockScanPredicate.operatorID, newOutputPort, false);

      let operator = workflowGraph.getOperator(mockScanPredicate.operatorID);
      expect(operator.inputPorts.map(p => p.portID)).toEqual(["input-0"]);
      expect(operator.outputPorts.map(p => p.portID)).toEqual(["output-0", "output-1"]);

      workflowGraph.removePort(mockScanPredicate.operatorID, true);
      workflowGraph.removePort(mockScanPredicate.operatorID, false);

      operator = workflowGraph.getOperator(mockScanPredicate.operatorID);
      expect(operator.inputPorts.length).toEqual(0);
      expect(operator.outputPorts.map(p => p.portID)).toEqual(["output-0"]);
    });

    it("should throw when adding or removing a port on a nonexistent operator", () => {
      expect(() => workflowGraph.addPort("nonexist", { portID: "input-0" }, true)).toThrowError(
        new RegExp("doesn't exist")
      );
      expect(() => workflowGraph.removePort("nonexist", false)).toThrowError(new RegExp("doesn't exist"));
    });

    it("should report port existence via hasPort", () => {
      workflowGraph.addOperator(mockSentimentPredicate);

      expect(workflowGraph.hasPort({ operatorID: "2", portID: "input-0" })).toBe(true);
      expect(workflowGraph.hasPort({ operatorID: "2", portID: "output-0" })).toBe(true);
      expect(workflowGraph.hasPort({ operatorID: "2", portID: "input-99" })).toBe(false);
      expect(workflowGraph.hasPort({ operatorID: "2", portID: "output-99" })).toBe(false);
      // portID that is neither input nor output
      expect(workflowGraph.hasPort({ operatorID: "2", portID: "weird" })).toBe(false);
      // nonexistent operator
      expect(workflowGraph.hasPort({ operatorID: "nonexist", portID: "input-0" })).toBe(false);
    });

    it("should return the port description for an existing port and throw for a missing one", () => {
      workflowGraph.addOperator(mockSentimentPredicate);

      expect(workflowGraph.getPortDescription({ operatorID: "2", portID: "input-0" })?.portID).toEqual("input-0");
      expect(workflowGraph.getPortDescription({ operatorID: "2", portID: "output-0" })?.portID).toEqual("output-0");
      expect(() => workflowGraph.getPortDescription({ operatorID: "2", portID: "input-99" })).toThrowError(
        new RegExp("does not exist")
      );
    });

    it("should set the partition and dependency properties of a port", () => {
      workflowGraph.addOperator(mockSentimentPredicate);
      const newProperty: PortProperty = {
        partitionInfo: { type: "hash", hashAttributeNames: ["col"] },
        dependencies: [{ id: 0, internal: false }],
      };

      workflowGraph.setPortProperty({ operatorID: "2", portID: "input-0" }, newProperty);

      const portDescription = workflowGraph.getPortDescription({ operatorID: "2", portID: "input-0" });
      expect(portDescription?.partitionRequirement).toEqual(newProperty.partitionInfo);
      expect(portDescription?.dependencies).toEqual(newProperty.dependencies);
    });

    it("should throw when setting a property on a nonexistent port", () => {
      workflowGraph.addOperator(mockSentimentPredicate);
      const newProperty: PortProperty = { partitionInfo: { type: "none" }, dependencies: [] };
      expect(() => workflowGraph.setPortProperty({ operatorID: "2", portID: "input-99" }, newProperty)).toThrowError(
        new RegExp("does not exist")
      );
    });
  });

  describe("comment boxes", () => {
    it("should load comment boxes provided to the constructor", () => {
      const graph = new WorkflowGraph([mockScanPredicate], [], [mockCommentBox]);
      expect(graph.getAllCommentBoxes().length).toEqual(1);
      expect(graph.getAllCommentBoxes()[0].commentBoxID).toEqual(mockCommentBox.commentBoxID);
      graph.destroyYModel();
    });

    it("should add, query, and delete a comment box", () => {
      workflowGraph.addCommentBox(mockCommentBox);
      expect(workflowGraph.hasCommentBox(mockCommentBox.commentBoxID)).toBe(true);
      expect(workflowGraph.hasElementWithID(mockCommentBox.commentBoxID)).toBe(true);
      expect(workflowGraph.getCommentBox(mockCommentBox.commentBoxID).commentBoxID).toEqual(
        mockCommentBox.commentBoxID
      );
      expect(workflowGraph.getSharedCommentBoxType(mockCommentBox.commentBoxID)).toBeDefined();

      workflowGraph.deleteCommentBox(mockCommentBox.commentBoxID);
      expect(workflowGraph.hasCommentBox(mockCommentBox.commentBoxID)).toBe(false);
    });

    it("should throw when adding a duplicate comment box", () => {
      workflowGraph.addCommentBox(mockCommentBox);
      expect(() => workflowGraph.addCommentBox(mockCommentBox)).toThrowError(new RegExp("already exists"));
    });

    it("should throw when operating on a nonexistent comment box", () => {
      expect(() => workflowGraph.getCommentBox("nonexist")).toThrowError(new RegExp("does not exist"));
      expect(() => workflowGraph.deleteCommentBox("nonexist")).toThrowError(new RegExp("does not exist"));
      expect(() => workflowGraph.getSharedCommentBoxType("nonexist")).toThrowError(new RegExp("does not exist"));
      expect(() => workflowGraph.assertCommentBoxExists("nonexist")).toThrowError(new RegExp("does not exist"));
    });

    it("should throw from assertCommentBoxNotExists when the comment box already exists", () => {
      workflowGraph.addCommentBox(mockCommentBox);
      expect(() => workflowGraph.assertCommentBoxNotExists(mockCommentBox.commentBoxID)).toThrowError(
        new RegExp("already exists")
      );
    });

    it("should add, edit, and delete a comment within a comment box", () => {
      workflowGraph.addCommentBox(mockCommentBox);
      const comment: Comment = { content: "hello", creationTime: "t1", creatorName: "alice", creatorID: 1 };

      workflowGraph.addCommentToCommentBox(comment, mockCommentBox.commentBoxID);
      expect(workflowGraph.getCommentBox(mockCommentBox.commentBoxID).comments.length).toEqual(1);
      expect(workflowGraph.getCommentBox(mockCommentBox.commentBoxID).comments[0].content).toEqual("hello");

      workflowGraph.editCommentInCommentBox(1, "t1", mockCommentBox.commentBoxID, "edited");
      expect(workflowGraph.getCommentBox(mockCommentBox.commentBoxID).comments[0].content).toEqual("edited");
      expect(workflowGraph.getCommentBox(mockCommentBox.commentBoxID).comments[0].creatorName).toEqual("alice");

      workflowGraph.deleteCommentFromCommentBox(1, "t1", mockCommentBox.commentBoxID);
      expect(workflowGraph.getCommentBox(mockCommentBox.commentBoxID).comments.length).toEqual(0);
    });

    it("should throw when adding a comment to a nonexistent comment box", () => {
      const comment: Comment = { content: "x", creationTime: "t", creatorName: "n", creatorID: 1 };
      expect(() => workflowGraph.addCommentToCommentBox(comment, "nonexist")).toThrowError(
        new RegExp("does not exist")
      );
    });
  });

  describe("debug state", () => {
    it("should create, get, and idempotently re-create an operator debug state", () => {
      workflowGraph.addOperator(mockScanPredicate);

      workflowGraph.createOperatorDebugState(mockScanPredicate.operatorID);
      const debugState = workflowGraph.getOperatorDebugState(mockScanPredicate.operatorID);
      expect(debugState).toBeDefined();

      // creating again is a no-op that keeps the same map
      workflowGraph.createOperatorDebugState(mockScanPredicate.operatorID);
      expect(workflowGraph.getOperatorDebugState(mockScanPredicate.operatorID)).toBe(debugState);
    });

    it("should throw when getting a debug state that was never created", () => {
      expect(() => workflowGraph.getOperatorDebugState("nonexist")).toThrowError(
        new RegExp("does not have a debug state")
      );
    });
  });

  describe("link assertions and guards", () => {
    it("should throw the duplicate-link error when two links share source and target", () => {
      workflowGraph.addOperator(mockScanPredicate);
      workflowGraph.addOperator(mockResultPredicate);
      workflowGraph.addLink(mockScanResultLink);
      // inject a second link with the same source/target but a different ID directly into the model
      workflowGraph.sharedModel.operatorLinkMap.set("dup-link", { ...mockScanResultLink, linkID: "dup-link" });

      expect(() => workflowGraph.getLink(mockScanResultLink.source, mockScanResultLink.target)).toThrowError(
        new RegExp("duplicate links")
      );
      expect(() => workflowGraph.assertLinkNotDuplicated(mockScanResultLink)).toThrowError(
        new RegExp("duplicate link")
      );
    });

    it("should not throw from assertLinkNotDuplicated for a unique link", () => {
      workflowGraph.addOperator(mockScanPredicate);
      workflowGraph.addOperator(mockResultPredicate);
      workflowGraph.addLink(mockScanResultLink);
      expect(() => workflowGraph.assertLinkNotDuplicated(mockScanResultLink)).not.toThrow();
    });

    it("should throw from assertLinkWithIDExists and assertLinkExists for missing links", () => {
      expect(() => workflowGraph.assertLinkWithIDExists("nonexist")).toThrowError(new RegExp("doesn't exist"));
      expect(() =>
        workflowGraph.assertLinkExists({ operatorID: "1", portID: "output-0" }, { operatorID: "3", portID: "input-0" })
      ).toThrow();
    });

    it("should validate a link's source port existence", () => {
      workflowGraph.addOperator(mockResultPredicate); // operator 3, has no output ports
      workflowGraph.addOperator(mockSentimentPredicate); // operator 2
      const badSourcePortLink: OperatorLink = {
        linkID: "bad-source-port",
        source: { operatorID: "3", portID: "output-0" },
        target: { operatorID: "2", portID: "input-0" },
      };
      expect(() => workflowGraph.assertLinkIsValid(badSourcePortLink)).toThrowError(new RegExp("source port"));
    });

    it("should validate a link's target port existence", () => {
      workflowGraph.addOperator(mockSentimentPredicate); // operator 2
      workflowGraph.addOperator(mockScanPredicate); // operator 1, has no input ports
      const badTargetPortLink: OperatorLink = {
        linkID: "bad-target-port",
        source: { operatorID: "2", portID: "output-0" },
        target: { operatorID: "1", portID: "input-0" },
      };
      expect(() => workflowGraph.assertLinkIsValid(badTargetPortLink)).toThrowError(new RegExp("target port"));
    });

    it("should throw when validating a link whose source or target operator is missing", () => {
      workflowGraph.addOperator(mockResultPredicate);
      const missingSource: OperatorLink = {
        linkID: "missing-source",
        source: { operatorID: "nonexist", portID: "output-0" },
        target: { operatorID: "3", portID: "input-0" },
      };
      expect(() => workflowGraph.assertLinkIsValid(missingSource)).toThrowError(new RegExp("does not exist"));
    });

    it("should throw the correct errors when deleting nonexistent links (both overloads)", () => {
      // deleteLinkWithID goes through getLinkWithID which throws "does not exist"
      expect(() => workflowGraph.deleteLinkWithID("nope")).toThrowError(new RegExp("does not exist"));
    });
  });

  describe("getSubDAG", () => {
    // scan(1) -> sentiment(2), scan(1) -> multi(4), sentiment(2) -> multi(4) forms a diamond ending at multi(4)
    const scanMultiLink: OperatorLink = {
      linkID: "link-scan-multi",
      source: { operatorID: "1", portID: "output-0" },
      target: { operatorID: "4", portID: "input-0" },
    };
    const sentimentMultiLink: OperatorLink = {
      linkID: "link-sentiment-multi",
      source: { operatorID: "2", portID: "output-0" },
      target: { operatorID: "4", portID: "input-1" },
    };

    beforeEach(() => {
      workflowGraph.addOperator(mockScanPredicate);
      workflowGraph.addOperator(mockSentimentPredicate);
      workflowGraph.addOperator(mockMultiInputOutputPredicate);
      workflowGraph.addLink(mockScanSentimentLink);
      workflowGraph.addLink(scanMultiLink);
      workflowGraph.addLink(sentimentMultiLink);
    });

    it("should build the whole subDAG from terminal operators when no target is given", () => {
      const subDag = workflowGraph.getSubDAG();
      expect(new Set(subDag.operators.map(op => op.operatorID))).toEqual(new Set(["1", "2", "4"]));
      expect(new Set(subDag.links.map(link => link.linkID))).toEqual(
        new Set(["link-2", "link-scan-multi", "link-sentiment-multi"])
      );
    });

    it("should build a subDAG rooted at a specific target operator", () => {
      const subDag = workflowGraph.getSubDAG("2");
      expect(new Set(subDag.operators.map(op => op.operatorID))).toEqual(new Set(["1", "2"]));
      expect(subDag.links.map(link => link.linkID)).toEqual(["link-2"]);
    });

    it("should exclude disabled operators and their links from the subDAG", () => {
      workflowGraph.disableOperator("1");
      const subDag = workflowGraph.getSubDAG();
      expect(subDag.operators.map(op => op.operatorID)).not.toContain("1");
      expect(subDag.links.map(link => link.linkID)).not.toContain("link-scan-multi");
      expect(subDag.links.map(link => link.linkID)).not.toContain("link-2");
    });
  });

  describe("event stream getters", () => {
    it("should expose observable streams that forward emissions from their subjects", () => {
      const cases: Array<[Observable<any>, { next: (v: any) => void }, any]> = [
        [workflowGraph.getOperatorAddStream(), workflowGraph.operatorAddSubject, mockScanPredicate],
        [workflowGraph.getOperatorDeleteStream(), workflowGraph.operatorDeleteSubject, { deletedOperatorID: "1" }],
        [
          workflowGraph.getDisabledOperatorsChangedStream(),
          workflowGraph.disabledOperatorChangedSubject,
          { newDisabled: ["1"], newEnabled: [] },
        ],
        [workflowGraph.getCommentBoxAddStream(), workflowGraph.commentBoxAddSubject, mockCommentBox],
        [
          workflowGraph.getCommentBoxDeleteStream(),
          workflowGraph.commentBoxDeleteSubject,
          { deletedCommentBox: mockCommentBox },
        ],
        [
          workflowGraph.getCommentBoxAddCommentStream(),
          workflowGraph.commentBoxAddCommentSubject,
          { addedComment: {} as Comment, commentBox: mockCommentBox },
        ],
        [
          workflowGraph.getCommentBoxDeleteCommentStream(),
          workflowGraph.commentBoxDeleteCommentSubject,
          { commentBox: mockCommentBox },
        ],
        [
          workflowGraph.getCommentBoxEditCommentStream(),
          workflowGraph.commentBoxEditCommentSubject,
          { commentBox: mockCommentBox },
        ],
        [
          workflowGraph.getViewResultOperatorsChangedStream(),
          workflowGraph.viewResultOperatorChangedSubject,
          { newViewResultOps: ["1"], newUnviewResultOps: [] },
        ],
        [
          workflowGraph.getReuseCacheOperatorsChangedStream(),
          workflowGraph.reuseOperatorChangedSubject,
          { newReuseCacheOps: ["1"], newUnreuseCacheOps: [] },
        ],
        [
          workflowGraph.getOperatorDisplayNameChangedStream(),
          workflowGraph.operatorDisplayNameChangedSubject,
          { operatorID: "1", newDisplayName: "n" },
        ],
        [workflowGraph.getLinkAddStream(), workflowGraph.linkAddSubject, mockScanResultLink],
        [workflowGraph.getLinkDeleteStream(), workflowGraph.linkDeleteSubject, { deletedLink: mockScanResultLink }],
        [
          workflowGraph.getOperatorPropertyChangeStream(),
          workflowGraph.operatorPropertyChangeSubject,
          { operator: mockScanPredicate },
        ],
        [
          workflowGraph.getBreakpointChangeStream(),
          workflowGraph.breakpointChangeStream,
          { oldBreakpoint: undefined, linkID: "link-1" },
        ],
        [
          workflowGraph.getPortAddedOrDeletedStream(),
          workflowGraph.portAddedOrDeletedSubject,
          { newOperator: mockScanPredicate },
        ],
        [
          workflowGraph.getPortDisplayNameChangedSubject(),
          workflowGraph.portDisplayNameChangedSubject,
          { operatorID: "1", portID: "input-0", newDisplayName: "n" },
        ],
        [
          workflowGraph.getPortPropertyChangedStream(),
          workflowGraph.portPropertyChangedSubject,
          { operatorPortID: { operatorID: "1", portID: "input-0" }, newProperty: {} as PortProperty },
        ],
      ];

      for (const [stream, subject, value] of cases) {
        expect(stream).toBeInstanceOf(Observable);
        let received: any;
        const sub = stream.subscribe(v => (received = v));
        subject.next(value);
        expect(received).toBe(value);
        sub.unsubscribe();
      }
    });

    it("should emit on the center-event stream when triggerCenterEvent is called", () => {
      let fired = false;
      const sub = workflowGraph.getCenterEventStream().subscribe(() => (fired = true));
      workflowGraph.triggerCenterEvent();
      expect(fired).toBe(true);
      sub.unsubscribe();
    });
  });
});

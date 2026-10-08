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

import { inject, TestBed } from "@angular/core/testing";
import { Validation, ValidationError, ValidationWorkflowService } from "./validation-workflow.service";
import {
  mockLoopEndPredicate,
  mockLoopStartPredicate,
  mockLoopStartScalaExecutorLink,
  mockPoint,
  mockResultPredicate,
  mockScalaExecutorLoopEndLink,
  mockScalaExecutorPredicate,
  mockScanPredicate,
  mockScanResultLink,
  mockScanSentimentLink,
  mockSentimentPredicate,
} from "../workflow-graph/model/mock-workflow-data";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { UndoRedoService } from "../undo-redo/undo-redo.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
import { JointUIService } from "../joint-ui/joint-ui.service";
import { marbles } from "rxjs-marbles";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { map } from "rxjs/operators";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { LoopBlockService } from "../compile-workflow/loop-block.service";

describe("ValidationWorkflowService", () => {
  let validationWorkflowService: ValidationWorkflowService;
  let workflowActionservice: WorkflowActionService;
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        WorkflowActionService,
        WorkflowUtilService,
        UndoRedoService,
        ValidationWorkflowService,
        JointUIService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        ...commonTestProviders,
      ],
    });

    validationWorkflowService = TestBed.inject(ValidationWorkflowService);
    workflowActionservice = TestBed.inject(WorkflowActionService);
  });

  it("should be created", inject([ValidationWorkflowService], (service: ValidationWorkflowService) => {
    expect(service).toBeTruthy();
  }));

  it("should receive true from validateOperator when operator box is connected and required properties are complete ", () => {
    workflowActionservice.addOperator(mockScanPredicate, mockPoint);
    workflowActionservice.addOperator(mockResultPredicate, mockPoint);
    workflowActionservice.addLink(mockScanResultLink);
    const newProperty = { tableName: "test-table" };
    workflowActionservice.setOperatorProperty(mockScanPredicate.operatorID, newProperty);
    expect(validationWorkflowService.validateOperator(mockResultPredicate.operatorID).isValid).toBeTruthy();
    expect(validationWorkflowService.validateOperator(mockScanPredicate.operatorID).isValid).toBeTruthy();
  });

  it(
    "should subscribe the changes of validateOperatorStream when operator box is connected and required properties are complete ",
    marbles(m => {
      const testEvents = m.hot("-a-b-c----d-", {
        a: () => workflowActionservice.addOperator(mockScanPredicate, mockPoint),
        b: () => workflowActionservice.addOperator(mockResultPredicate, mockPoint),
        c: () => workflowActionservice.addLink(mockScanResultLink),
        d: () => workflowActionservice.setOperatorProperty(mockScanPredicate.operatorID, { tableName: "test-table" }),
      });

      testEvents.subscribe(action => action());

      const expected = m.hot("-u-v-(yz)-m-", {
        u: { operatorID: "1", isValid: false },
        v: { operatorID: "3", isValid: false },
        y: { operatorID: "1", isValid: false },
        z: { operatorID: "3", isValid: true },
        m: { operatorID: "1", isValid: true },
      });

      m.expect(
        validationWorkflowService.getOperatorValidationStream().pipe(
          map(value => ({
            operatorID: value.operatorID,
            isValid: value.validation.isValid,
          }))
        )
      ).toBeObservable(expected);
    })
  );

  it("should receive false from validateOperator when operator box is not connected or required properties are not complete ", () => {
    workflowActionservice.addOperator(mockScanPredicate, mockPoint);
    workflowActionservice.addOperator(mockResultPredicate, mockPoint);
    workflowActionservice.addLink(mockScanResultLink);
    expect(validationWorkflowService.validateOperator(mockResultPredicate.operatorID).isValid).toBeTruthy();
    expect(validationWorkflowService.validateOperator(mockScanPredicate.operatorID).isValid).toBeFalsy();
  });

  // TODO: this test is incompatible with shared editing.
  // it(
  //   "should subscribe the changes of validateOperatorStream when one operator box is deleted after valid status ",
  //   marbles(m => {
  //     const testEvents = m.hot("-a-b-c----d-e-----", {
  //       a: () => workflowActionservice.addOperator(mockScanPredicate, mockPoint),
  //       b: () => workflowActionservice.addOperator(mockResultPredicate, mockPoint),
  //       c: () => workflowActionservice.addLink(mockScanResultLink),
  //       d: () => workflowActionservice.setOperatorProperty(mockScanPredicate.operatorID, { tableName: "test-table" }),
  //       e: () => workflowActionservice.deleteOperator(mockResultPredicate.operatorID),
  //     });
  //
  //     testEvents.subscribe(action => action());
  //
  //     const expected = m.hot("-t-u-(vw)-x-(yz)-)", {
  //       t: { operatorID: "1", isValid: false },
  //       u: { operatorID: "3", isValid: false },
  //       v: { operatorID: "1", isValid: false },
  //       w: { operatorID: "3", isValid: true },
  //       x: { operatorID: "1", isValid: true },
  //       y: { operatorID: "1", isValid: false }, // If one of the oprator is deleted, the other one is invaild since it is isolated
  //       z: { operatorID: "3", isValid: false },
  //     });
  //
  //     m.expect(
  //       validationWorkflowService.getOperatorValidationStream().pipe(
  //         map(value => ({
  //           operatorID: value.operatorID,
  //           isValid: value.validation.isValid,
  //         }))
  //       )
  //     ).toBeObservable(expected);
  //   })
  // );

  it(
    "should subscribe the changes of validateOperatorStream when operator link is deleted after valid status ",
    marbles(m => {
      const testEvents = m.hot("-a-b-c----d-e-f--", {
        a: () => workflowActionservice.addOperator(mockScanPredicate, mockPoint),
        b: () => workflowActionservice.addOperator(mockSentimentPredicate, mockPoint),
        c: () => workflowActionservice.addLink(mockScanSentimentLink),
        d: () => workflowActionservice.setOperatorProperty(mockScanPredicate.operatorID, { tableName: "test-table" }),
        e: () =>
          workflowActionservice.setOperatorProperty(mockSentimentPredicate.operatorID, {
            attribute: "test-attribute",
            resultAttribute: "result-attribtue",
          }),
        f: () => workflowActionservice.deleteLinkWithID(mockScanSentimentLink.linkID),
      });

      testEvents.subscribe(action => action());

      const expected = m.hot("-s-t-(uv)-w-x-(yz)-", {
        s: { operatorID: "1", isValid: false },
        t: { operatorID: "2", isValid: false },
        u: { operatorID: "1", isValid: false },
        v: { operatorID: "2", isValid: false },
        w: { operatorID: "1", isValid: true },
        x: { operatorID: "2", isValid: true },
        y: { operatorID: "1", isValid: true },
        z: { operatorID: "2", isValid: false }, // If the link is deleted, the one missing input link is invalid
      });

      m.expect(
        validationWorkflowService.getOperatorValidationStream().pipe(
          map(value => ({
            operatorID: value.operatorID,
            isValid: value.validation.isValid,
          }))
        )
      ).toBeObservable(expected);
    })
  );

  it("should consider disabled operators when validating workflow", () => {
    workflowActionservice.addOperator(mockScanPredicate, mockPoint);
    workflowActionservice.addOperator(mockResultPredicate, mockPoint);
    workflowActionservice.addLink(mockScanResultLink);
    workflowActionservice.setOperatorProperty(mockScanPredicate.operatorID, {
      tableName: "test-table",
    });
    expect(Object.entries(validationWorkflowService.getCurrentWorkflowValidationError().errors).length).toEqual(0);

    const mockScanPredicate2 = {
      ...mockScanPredicate,
      operatorID: "mockScan2",
    };
    const mockResultPredicate2 = {
      ...mockResultPredicate,
      operatorID: "mockResult2",
    };
    const mockScanResultLink2 = {
      linkID: "mock-scan-result-link-2",
      source: {
        operatorID: mockScanPredicate2.operatorID,
        portID: mockScanPredicate2.outputPorts[0].portID,
      },
      target: {
        operatorID: mockResultPredicate2.operatorID,
        portID: mockResultPredicate2.inputPorts[0].portID,
      },
    };

    workflowActionservice.addOperator(mockScanPredicate2, mockPoint);
    workflowActionservice.addOperator(mockResultPredicate2, mockPoint);
    workflowActionservice.addLink(mockScanResultLink2);
    console.log(validationWorkflowService.getCurrentWorkflowValidationError().errors);
    expect(Object.entries(validationWorkflowService.getCurrentWorkflowValidationError().errors).length).toEqual(1);

    workflowActionservice.getTexeraGraph().disableOperator(mockScanPredicate2.operatorID);
    workflowActionservice.getTexeraGraph().disableOperator(mockResultPredicate2.operatorID);
    expect(Object.entries(validationWorkflowService.getCurrentWorkflowValidationError().errors).length).toEqual(0);
  });

  it("should report an operator invalid when a disallowMultiInputs port has two enabled links", () => {
    // The guard behind `disallowMultiLinks` on a loop operator's input port
    // (LoopOpDesc): a port declared single-input must have exactly one inbound
    // link, so fanning two producers into it is invalid rather than silently
    // accepted and failing at StartWorkflow.
    const singleInputSink = {
      ...mockResultPredicate,
      operatorID: "single-input-sink",
      inputPorts: [{ portID: "input-0", disallowMultiInputs: true }],
    };
    const secondSource = { ...mockScanPredicate, operatorID: "scan-2" };
    const linkFromFirst = {
      linkID: "link-single-input-1",
      source: { operatorID: mockScanPredicate.operatorID, portID: "output-0" },
      target: { operatorID: singleInputSink.operatorID, portID: "input-0" },
    };
    const linkFromSecond = {
      linkID: "link-single-input-2",
      source: { operatorID: secondSource.operatorID, portID: "output-0" },
      target: { operatorID: singleInputSink.operatorID, portID: "input-0" },
    };

    workflowActionservice.addOperator(mockScanPredicate, mockPoint);
    workflowActionservice.addOperator(secondSource, mockPoint);
    workflowActionservice.addOperator(singleInputSink, mockPoint);

    workflowActionservice.addLink(linkFromFirst);
    expect(validationWorkflowService.validateOperator(singleInputSink.operatorID).isValid).toBeTruthy();

    workflowActionservice.addLink(linkFromSecond);
    const validation = validationWorkflowService.validateOperator(singleInputSink.operatorID);
    expect(validation.isValid).toBeFalsy();
    if (!validation.isValid) {
      expect(validation.messages["inputs"]).toContain("requires 1 input, has 2");
    }
  });

  // A stale operator id surfaces as an explicit error rather than a downstream undefined
  // dereference. The graph's own lookup rejects it before the service's guards are reached,
  // so that is the message asserted here.
  it("should throw for an operator id that is not in the graph", () => {
    expect(() => validationWorkflowService.validateOperator("no-such-operator")).toThrowError(
      "operator no-such-operator does not exist"
    );
  });

  // The service's own `operatorSchema === undefined` guards are not reachable through the public
  // API: the graph rejects an unknown operator type at insertion time, so a schema-less operator
  // never makes it in.
  it("should reject an operator whose type has no schema at insertion time", () => {
    const unknownTypeOperator = {
      ...mockScanPredicate,
      operatorID: "unknown-type-operator",
      operatorType: "NoSuchOperatorType",
    };

    expect(() => workflowActionservice.addOperator(unknownTypeOperator, mockPoint)).toThrowError(
      "operator type NoSuchOperatorType is invalid"
    );
  });

  it("should expose the workflow validation error stream", () => {
    const emissions: unknown[] = [];
    const subscription = validationWorkflowService
      .getWorkflowValidationErrorStream()
      .subscribe(value => emissions.push(value));

    workflowActionservice.addOperator(mockScanPredicate, mockPoint);

    expect(emissions.length).toBeGreaterThan(0);
    subscription.unsubscribe();
  });

  describe("loop-variable references ($name) in operator properties", () => {
    // Loop Start (K = 2) -> body -> Loop End, where the body is an operator backed by a Scala executor,
    // the kind on which the backend binds a reference. Which operators sit inside a block comes from the
    // compile result, which these tests stand in for through LoopBlockService.
    const body = mockScalaExecutorPredicate;
    const link = (linkID: string, from: string, to: string) => ({
      linkID,
      source: { operatorID: from, portID: "output-0" },
      target: { operatorID: to, portID: "input-0" },
    });
    /** A second plain operator, for a body of two. */
    const other = { ...mockScalaExecutorPredicate, operatorID: "13", operatorProperties: { limit: 1 } };

    let loopBlockService: LoopBlockService;
    beforeEach(() => {
      loopBlockService = TestBed.inject(LoopBlockService);
    });

    /** A compile result that puts the operators given inside the Loop Start's block, and nothing else. */
    const compileWithInside = (...operatorIDs: string[]) =>
      loopBlockService.setOperatorLoopStarts(
        Object.fromEntries(operatorIDs.map(operatorID => [operatorID, [mockLoopStartPredicate.operatorID]]))
      );
    /** The block around the body, and the compile result that follows those edits. */
    const addBlockAround = (bodyProperties: Record<string, unknown>) => {
      workflowActionservice.addOperator(mockLoopStartPredicate, mockPoint);
      workflowActionservice.addOperator({ ...body, operatorProperties: bodyProperties }, mockPoint);
      workflowActionservice.addOperator(mockLoopEndPredicate, mockPoint);
      workflowActionservice.addLink(mockLoopStartScalaExecutorLink);
      workflowActionservice.addLink(mockScalaExecutorLoopEndLink);
      compileWithInside(body.operatorID);
    };
    /** What the canvas holds for an operator: its error, or undefined while it is valid. */
    const currentError = (operatorID: string): ValidationError | undefined =>
      validationWorkflowService.getCurrentWorkflowValidationError().errors[operatorID];

    it("accepts a reference to a declared loop variable where the schema wants an integer or a number", () => {
      addBlockAround({ limit: "$K", fraction: "$K" });
      expect(validationWorkflowService.validateOperator(body.operatorID)).toEqual({ isValid: true });
    });

    it("accepts a reference to a declared loop variable on a string or boolean property", () => {
      addBlockAround({ limit: 1, prefix: "$K", caseSensitive: "$K" });
      expect(validationWorkflowService.validateOperator(body.operatorID)).toEqual({ isValid: true });
    });

    it("rejects a reference to a variable no enclosing block declares, naming the first one", () => {
      addBlockAround({ limit: "$foo", prefix: "$bar" });
      const validation = validationWorkflowService.validateOperator(body.operatorID);
      expect(validation.isValid).toBe(false);
      expect((validation as ValidationError).messages["loopVariable"]).toBe(
        "$foo is not a variable of an enclosing block"
      );
      // the schema type error at the reference is not reported twice
      expect((validation as ValidationError).messages["type"]).toBeUndefined();
    });

    it("still reports other schema errors next to a reference", () => {
      // a wrong-typed plain value alongside a valid reference: only the plain value is an error
      addBlockAround({ limit: "$K", fraction: "warm" });
      const validation = validationWorkflowService.validateOperator(body.operatorID);
      expect(validation.isValid).toBe(false);
      expect((validation as ValidationError).messages["type"]).toBe("must be number");
    });

    it("does not waive the type check for a reference in an array property, which no reference can fill", () => {
      addBlockAround({ limit: 1, columns: "$K" });
      const validation = validationWorkflowService.validateOperator(body.operatorID);
      expect(validation.isValid).toBe(false);
      expect((validation as ValidationError).messages["type"]).toBe("must be array");
    });

    it("keeps rejecting a reference in an integer property outside every block (today's behavior)", () => {
      workflowActionservice.addOperator(mockScanPredicate, mockPoint);
      workflowActionservice.addOperator({ ...body, operatorProperties: { limit: "$K" } }, mockPoint);
      workflowActionservice.addLink(link("scan-to-body", mockScanPredicate.operatorID, body.operatorID));
      compileWithInside();
      const validation = validationWorkflowService.validateOperator(body.operatorID);
      expect(validation.isValid).toBe(false);
      expect((validation as ValidationError).messages["type"]).toBe("must be integer");
      expect((validation as ValidationError).messages["loopVariable"]).toBeUndefined();
    });

    it("counts no operator inside a block until a compile result says so, whatever the graph looks like", () => {
      workflowActionservice.addOperator(mockLoopStartPredicate, mockPoint);
      workflowActionservice.addOperator({ ...body, operatorProperties: { limit: "$K" } }, mockPoint);
      workflowActionservice.addOperator(mockLoopEndPredicate, mockPoint);
      workflowActionservice.addLink(mockLoopStartScalaExecutorLink);
      workflowActionservice.addLink(mockScalaExecutorLoopEndLink);
      expect(validationWorkflowService.validateOperator(body.operatorID)).toEqual({
        isValid: false,
        messages: { type: "must be integer" },
      });

      compileWithInside(body.operatorID);
      expect(validationWorkflowService.validateOperator(body.operatorID)).toEqual({ isValid: true });
    });

    // The canvas state, which the Run button reads, follows each compile result.
    describe("as compile results change the block around an operator holding a reference", () => {
      it("flags the reference once the Loop Start's variable is renamed, and clears it once it is back", () => {
        addBlockAround({ limit: "$K" });
        expect(currentError(body.operatorID)).toBeUndefined();

        workflowActionservice.setOperatorProperty(mockLoopStartPredicate.operatorID, {
          initialization: "i = 0",
          output: "table.iloc[i]",
        });
        // the edit itself is not what makes it look again: the compile result that follows it is
        expect(currentError(body.operatorID)).toBeUndefined();
        compileWithInside(body.operatorID);
        expect(currentError(body.operatorID)?.messages["loopVariable"]).toBe(
          "$K is not a variable of an enclosing block"
        );

        workflowActionservice.setOperatorProperty(mockLoopStartPredicate.operatorID, {
          initialization: "if True:\n    K = 3",
          output: "table.iloc[K]",
        });
        compileWithInside(body.operatorID);
        expect(currentError(body.operatorID)).toBeUndefined();
      });

      it("reports the type error once a compile result takes the operator out of the block", () => {
        addBlockAround({ limit: "$K" });
        workflowActionservice.deleteLinkWithID(mockScalaExecutorLoopEndLink.linkID);
        compileWithInside();
        expect(currentError(body.operatorID)?.messages["type"]).toBe("must be integer");
      });

      it("clears the type error once a compile result puts the operator inside a block", () => {
        workflowActionservice.addOperator(mockLoopStartPredicate, mockPoint);
        workflowActionservice.addOperator({ ...body, operatorProperties: { limit: "$K" } }, mockPoint);
        workflowActionservice.addLink(mockLoopStartScalaExecutorLink);
        compileWithInside();
        expect(currentError(body.operatorID)?.messages["type"]).toBe("must be integer");

        compileWithInside(body.operatorID);
        expect(currentError(body.operatorID)).toBeUndefined();
      });

      it("looks again only at the operators holding a reference", () => {
        addBlockAround({ limit: "$K" });
        workflowActionservice.addOperator(other, mockPoint);
        workflowActionservice.addLink(link("body-other", body.operatorID, other.operatorID));
        const looked: string[] = [];
        const subscription = validationWorkflowService
          .getOperatorValidationStream()
          .subscribe(({ operatorID }) => looked.push(operatorID));

        compileWithInside(body.operatorID, other.operatorID);
        expect(looked).toEqual([body.operatorID]);
        subscription.unsubscribe();
      });
    });

    // The compile request carries the operators the frontend judges valid, and the compiler is what tells
    // which of them sit inside a block, so an operator whose only errors are at references must reach it.
    describe("the graph sent to the compiler", () => {
      it("keeps an operator whose only errors are at references, inside a block or not, known or not", () => {
        workflowActionservice.addOperator(mockLoopStartPredicate, mockPoint);
        workflowActionservice.addOperator({ ...body, operatorProperties: { limit: "$K" } }, mockPoint);
        workflowActionservice.addOperator(mockLoopEndPredicate, mockPoint);
        workflowActionservice.addLink(mockLoopStartScalaExecutorLink);
        workflowActionservice.addLink(mockScalaExecutorLoopEndLink);
        // no compile result has placed it yet: the Run button sees a type error, the compiler the operator
        expect(validationWorkflowService.validateOperator(body.operatorID).isValid).toBe(false);
        const validGraph = validationWorkflowService.getValidTexeraGraph();
        expect(validGraph.hasOperator(body.operatorID)).toBe(true);
        expect(validGraph.getOperator(body.operatorID).operatorProperties).toEqual({ limit: "$K" });

        // a name no enclosing Loop Start declares is the compiler's to judge as well
        compileWithInside(body.operatorID);
        workflowActionservice.setOperatorProperty(body.operatorID, { limit: "$foo" });
        expect(validationWorkflowService.validateOperator(body.operatorID).isValid).toBe(false);
        expect(validationWorkflowService.getValidTexeraGraph().hasOperator(body.operatorID)).toBe(true);
      });

      it("leaves out an operator with any other error, next to a reference or not", () => {
        addBlockAround({ limit: "$K", fraction: "warm" });
        expect(validationWorkflowService.getValidTexeraGraph().hasOperator(body.operatorID)).toBe(false);
        // a reference in an array property is an error everywhere
        workflowActionservice.setOperatorProperty(body.operatorID, { limit: 1, columns: "$K" });
        expect(validationWorkflowService.getValidTexeraGraph().hasOperator(body.operatorID)).toBe(false);
        // and a valid operator stays in
        workflowActionservice.setOperatorProperty(body.operatorID, { limit: 1 });
        expect(validationWorkflowService.getValidTexeraGraph().hasOperator(body.operatorID)).toBe(true);
      });
    });
  });
});

describe("ValidationWorkflowService.combineValidation", () => {
  const invalid = (messages: Record<string, string>): Validation => ({ isValid: false, messages });

  it("should be valid when given no validations at all", () => {
    const combined = ValidationWorkflowService.combineValidation();

    expect(combined.isValid).toBe(true);
    // The valid branch returns { isValid } only, so consumers reading `messages`
    // off a valid result get undefined rather than an empty object.
    expect((combined as ValidationError).messages).toBeUndefined();
  });

  it("should be valid, with no messages, when every validation is valid", () => {
    const combined = ValidationWorkflowService.combineValidation({ isValid: true }, { isValid: true });

    expect(combined).toEqual({ isValid: true });
  });

  it("should be invalid and carry the messages when a single validation is invalid", () => {
    const combined = ValidationWorkflowService.combineValidation(
      { isValid: true },
      invalid({ jsonSchema: "property 'x' is required" })
    );

    expect(combined).toEqual({ isValid: false, messages: { jsonSchema: "property 'x' is required" } });
  });

  it("should stay invalid regardless of where the invalid validation sits", () => {
    const failure = invalid({ connection: "operator has no input" });

    expect(ValidationWorkflowService.combineValidation(failure, { isValid: true }).isValid).toBe(false);
    expect(ValidationWorkflowService.combineValidation({ isValid: true }, failure).isValid).toBe(false);
  });

  it("should merge the messages of several invalid validations", () => {
    const combined = ValidationWorkflowService.combineValidation(
      invalid({ jsonSchema: "property 'x' is required" }),
      { isValid: true },
      invalid({ connection: "operator has no input" })
    );

    expect(combined).toEqual({
      isValid: false,
      messages: {
        jsonSchema: "property 'x' is required",
        connection: "operator has no input",
      },
    });
  });

  it("should let a later message win when two invalid validations share a key", () => {
    const combined = ValidationWorkflowService.combineValidation(
      invalid({ connection: "first" }),
      invalid({ connection: "second" })
    );

    expect(combined).toEqual({ isValid: false, messages: { connection: "second" } });
  });

  it("should ignore messages attached to a validation that reports itself valid", () => {
    // The Validation union gives the valid arm no `messages`, but the merge is
    // guarded on isValid rather than on the key being absent, so a stray field
    // is dropped instead of leaking into the combined result.
    const validWithStrayMessages = { isValid: true, messages: { ignored: "not a real error" } } as Validation;

    const combined = ValidationWorkflowService.combineValidation(
      validWithStrayMessages,
      invalid({ connection: "operator has no input" })
    );

    expect(combined).toEqual({ isValid: false, messages: { connection: "operator has no input" } });
  });

  it("should report invalid with an empty message map when the failing validation has none", () => {
    const combined = ValidationWorkflowService.combineValidation(invalid({}));

    expect(combined).toEqual({ isValid: false, messages: {} });
  });

  it("should not mutate the validations it was given", () => {
    const first = invalid({ jsonSchema: "property 'x' is required" });
    const second = invalid({ connection: "operator has no input" });

    ValidationWorkflowService.combineValidation(first, second);

    expect(first).toEqual({ isValid: false, messages: { jsonSchema: "property 'x' is required" } });
    expect(second).toEqual({ isValid: false, messages: { connection: "operator has no input" } });
  });
});

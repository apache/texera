import {
  mockResultPredicate,
  mockScanPredicate,
  mockScanResultLink,
  mockScanSentimentLink,
  mockSentimentPredicate,
  mockSentimentResultLink,
} from "./mock-workflow-data";
import { WorkflowGraph } from "./workflow-graph";
import { environment } from "../../../../../environments/environment";

describe("WorkflowGraph", () => {
  let workflowGraph: WorkflowGraph;

  beforeEach(() => {
    workflowGraph = new WorkflowGraph();
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

    expect(workflowGraph.isOperatorDisabled(mockScanPredicate.operatorID)).toBeTrue();
    expect(workflowGraph.isOperatorDisabled(mockResultPredicate.operatorID)).toBeFalse();
    expect(workflowGraph.getDisabledOperators().size).toEqual(1);

    workflowGraph.enableOperator(mockScanPredicate.operatorID);
    expect(workflowGraph.isOperatorDisabled(mockScanPredicate.operatorID)).toBeFalse();
    expect(workflowGraph.getDisabledOperators().size).toEqual(0);
  });

  it("should calculate if link is disabled based on the disabled operator", () => {
    workflowGraph.addOperator(mockScanPredicate);
    workflowGraph.addOperator(mockResultPredicate);
    workflowGraph.addLink(mockScanResultLink);
    workflowGraph.disableOperator(mockScanPredicate.operatorID);

    expect(workflowGraph.isLinkEnabled(mockScanResultLink.linkID)).toBeFalse();
    expect(workflowGraph.getAllEnabledLinks().length).toEqual(0);
  });

  it("should cache and un-cache an operator", () => {
    workflowGraph.addOperator(mockScanPredicate);
    workflowGraph.addOperator(mockResultPredicate);
    workflowGraph.cacheOperator(mockScanPredicate.operatorID);

    expect(workflowGraph.isOperatorCached(mockScanPredicate.operatorID)).toBeTrue();
    expect(workflowGraph.isOperatorCached(mockResultPredicate.operatorID)).toBeFalse();
    expect(workflowGraph.getCachedOperators().size).toEqual(1);

    workflowGraph.unCacheOperator(mockScanPredicate.operatorID);
    expect(workflowGraph.isOperatorCached(mockScanPredicate.operatorID)).toBeFalse();
    expect(workflowGraph.getDisabledOperators().size).toEqual(0);
  });

  it("should ignore cache the view result operator", () => {
    workflowGraph.addOperator(mockScanPredicate);
    workflowGraph.addOperator(mockResultPredicate);
    workflowGraph.cacheOperator(mockResultPredicate.operatorID);

    expect(workflowGraph.isOperatorCached(mockScanPredicate.operatorID)).toBeFalse();
    expect(workflowGraph.isOperatorCached(mockResultPredicate.operatorID)).toBeFalse();
    expect(workflowGraph.getCachedOperators().size).toEqual(0);

    workflowGraph.unCacheOperator(mockResultPredicate.operatorID);
    expect(workflowGraph.isOperatorCached(mockResultPredicate.operatorID)).toBeFalse();
    expect(workflowGraph.getCachedOperators().size).toEqual(0);
  });

  describe("when linkBreakpoint is enabled", () => {
    beforeAll(() => {
      environment.linkBreakpointEnabled = true;
    });

    afterAll(() => {
      environment.linkBreakpointEnabled = false;
    });

    it("should set/remove link breakpoint correctly", () => {
      workflowGraph.addOperator(mockScanPredicate);
      workflowGraph.addOperator(mockResultPredicate);
      workflowGraph.addLink(mockScanResultLink);
      const mockBreakpoint = { count: 100 };
      workflowGraph.setLinkBreakpoint(mockScanResultLink.linkID, mockBreakpoint);
      expect(workflowGraph.getLinkBreakpoint(mockScanResultLink.linkID)).toEqual(mockBreakpoint);
    });
  });
});

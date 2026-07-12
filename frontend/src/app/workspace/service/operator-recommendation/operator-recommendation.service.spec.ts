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

import { TestBed } from "@angular/core/testing";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { OperatorRecommendationService } from "./operator-recommendation.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { MockGuiConfigService } from "../../../common/service/gui-config.service.mock";
import { OperatorLink, OperatorPredicate, Point } from "../../types/workflow-common.interface";

function makeOperator(overrides: Partial<OperatorPredicate> = {}): OperatorPredicate {
  return {
    operatorID: "op-source",
    operatorType: "CSVFileScan",
    operatorVersion: "1",
    operatorProperties: {},
    inputPorts: [],
    outputPorts: [{ portID: "output-0" }],
    showAdvanced: false,
    ...overrides,
  } as OperatorPredicate;
}

describe("OperatorRecommendationService", () => {
  let service: OperatorRecommendationService;
  let httpTestingController: HttpTestingController;
  let config: MockGuiConfigService;
  let addOperatorsAndLinksSpy: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    addOperatorsAndLinksSpy = vi.fn();

    const texeraGraphStub = {
      getAllOperators: () => [makeOperator()],
    };
    const jointGraphWrapperStub = {
      getElementPosition: (_id: string): Point => ({ x: 100, y: 200 }),
    };
    const workflowActionServiceStub = {
      getTexeraGraph: () => texeraGraphStub,
      getJointGraphWrapper: () => jointGraphWrapperStub,
      addOperatorsAndLinks: addOperatorsAndLinksSpy,
    };

    let linkCounter = 0;
    const workflowUtilServiceStub = {
      getNewOperatorPredicate: (operatorType: string): OperatorPredicate =>
        makeOperator({
          operatorID: `op-${operatorType}`,
          operatorType,
          inputPorts: [{ portID: "input-0" }],
          outputPorts: [{ portID: "output-0" }],
        }),
      getLinkRandomUUID: () => `link-${++linkCounter}`,
    };

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        OperatorRecommendationService,
        { provide: GuiConfigService, useClass: MockGuiConfigService },
        { provide: WorkflowActionService, useValue: workflowActionServiceStub },
        { provide: WorkflowUtilService, useValue: workflowUtilServiceStub },
      ],
    });

    service = TestBed.inject(OperatorRecommendationService);
    httpTestingController = TestBed.inject(HttpTestingController);
    config = TestBed.inject(GuiConfigService) as unknown as MockGuiConfigService;
    // Default the opt-in flag on for the majority of tests.
    (config.env as any).operatorRecommendationEnabled = true;
  });

  afterEach(() => {
    httpTestingController.verify();
  });

  it("reports enabled state from the GUI config flag", () => {
    (config.env as any).operatorRecommendationEnabled = false;
    expect(service.isEnabled()).toBe(false);
    (config.env as any).operatorRecommendationEnabled = true;
    expect(service.isEnabled()).toBe(true);
  });

  it("returns no recommendations and makes no request when disabled", () => {
    (config.env as any).operatorRecommendationEnabled = false;
    let result: unknown;
    service.getRecommendations(makeOperator()).subscribe(r => (result = r));
    expect(result).toEqual([]);
    httpTestingController.expectNone("/api/recommend");
  });

  it("returns no recommendations and makes no request for an operator with no output port", () => {
    let result: unknown;
    service.getRecommendations(makeOperator({ outputPorts: [] })).subscribe(r => (result = r));
    expect(result).toEqual([]);
    httpTestingController.expectNone("/api/recommend");
  });

  it("posts the operator context and returns the ranked recommendations", () => {
    const recommendations = [{ operatorType: "Filter", score: 0.9, reason: "Keep rows" }];
    let result: unknown;
    service.getRecommendations(makeOperator()).subscribe(r => (result = r));

    const req = httpTestingController.expectOne("/api/recommend");
    expect(req.request.method).toBe("POST");
    expect(req.request.body.operatorType).toBe("CSVFileScan");
    expect(req.request.body.existingOperatorTypes).toEqual(["CSVFileScan"]);
    req.flush({ recommendations, strategy: "hardcoded" });

    expect(result).toEqual(recommendations);
  });

  it("swallows backend errors and yields an empty list", () => {
    let result: unknown = "unset";
    service.getRecommendations(makeOperator()).subscribe(r => (result = r));
    httpTestingController.expectOne("/api/recommend").flush("boom", { status: 500, statusText: "Server Error" });
    expect(result).toEqual([]);
  });

  it("materializes a suggestion to the right of the source, linked port-to-port", () => {
    const source = makeOperator();
    const newId = service.materialize(source, "output-0", "Filter");

    expect(newId).toBe("op-Filter");
    expect(addOperatorsAndLinksSpy).toHaveBeenCalledTimes(1);

    const [operatorsAndPositions, links] = addOperatorsAndLinksSpy.mock.lastCall as [
      { op: OperatorPredicate; pos: Point }[],
      OperatorLink[],
    ];
    // New operator placed to the right of the source at (100, 200).
    expect(operatorsAndPositions[0].pos.x).toBeGreaterThan(100);
    expect(operatorsAndPositions[0].pos.y).toBe(200);
    // Link wires the clicked output port to the new operator's first input port.
    expect(links.length).toBe(1);
    expect(links[0].source).toEqual({ operatorID: "op-source", portID: "output-0" });
    expect(links[0].target).toEqual({ operatorID: "op-Filter", portID: "input-0" });
  });

  it("materializes without a link when the new operator has no input port", () => {
    vi.spyOn(TestBed.inject(WorkflowUtilService), "getNewOperatorPredicate").mockReturnValue(
      makeOperator({ operatorID: "op-sink", operatorType: "BarChart", inputPorts: [], outputPorts: [] })
    );
    service.materialize(makeOperator(), "output-0", "BarChart");

    const [, links] = addOperatorsAndLinksSpy.mock.lastCall as [unknown, OperatorLink[]];
    expect(links.length).toBe(0);
  });
});

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

import { StubOperatorMetadataService } from "./../../operator-metadata/stub-operator-metadata.service";
import { OperatorMetadataService } from "./../../operator-metadata/operator-metadata.service";
import { TestBed, inject } from "@angular/core/testing";

import { WorkflowUtilService } from "./workflow-util.service";
import { mockScanSourceSchema } from "../../operator-metadata/mock-operator-metadata.data";

describe("WorkflowUtilService", () => {
  let workflowUtilService: WorkflowUtilService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        WorkflowUtilService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
      ],
    });
    workflowUtilService = TestBed.get(WorkflowUtilService);
  });

  it("should be created", inject([WorkflowUtilService], (service: WorkflowUtilService) => {
    expect(service).toBeTruthy();
  }));

  it("should be able to generate an operator predicate properly given a valid operator type", () => {
    const operatorSchema = mockScanSourceSchema;
    const operatorPredicate = workflowUtilService.getNewOperatorPredicate(operatorSchema.operatorType);

    // assert predicate itself and operator type are correct
    expect(operatorPredicate).toBeTruthy();
    expect(operatorPredicate.operatorType).toEqual(operatorSchema.operatorType);
    // assert num of input ports and output ports are correct
    expect(operatorPredicate.inputPorts.length).toEqual(operatorSchema.additionalMetadata.inputPorts.length);
    expect(operatorPredicate.outputPorts.length).toEqual(operatorSchema.additionalMetadata.outputPorts.length);
    // asssert that the portID of input and output ports are all distinct
    expect(new Set(operatorPredicate.inputPorts).size).toEqual(operatorPredicate.inputPorts.length);
    expect(new Set(operatorPredicate.outputPorts).size).toEqual(operatorPredicate.outputPorts.length);

    // assert that it creates the operator property to be an empty object
    expect(operatorPredicate.operatorProperties).toEqual({});
  });

  it("should throw an error when trying to generate an operator predicate with non exist operator type", () => {
    expect(() => {
      workflowUtilService.getNewOperatorPredicate("non-exist-operator-type");
    }).toThrowError(new RegExp("doesn't exist"));
  });

  it("should be able to generate different operator IDs", () => {
    const idSet = new Set<string>();
    const repeat = 100;
    for (let i = 0; i < repeat; i++) {
      idSet.add(workflowUtilService.getOperatorRandomUUID());
    }
    // assert all IDs are distinct
    expect(idSet.size).toEqual(repeat);
  });

  it("should be able to generate different link IDs", () => {
    const idSet = new Set<string>();
    const repeat = 100;
    for (let i = 0; i < repeat; i++) {
      idSet.add(workflowUtilService.getLinkRandomUUID());
    }
    // assert all IDs are distinct
    expect(idSet.size).toEqual(repeat);
  });

  it("should be able to assign different operator IDs to newly generated operators", () => {
    const operatorSchema = mockScanSourceSchema;
    const idSet = new Set<string>();
    const repeat = 100;

    for (let i = 0; i < repeat; i++) {
      idSet.add(workflowUtilService.getNewOperatorPredicate(operatorSchema.operatorType).operatorID);
    }
    // assert all IDs are distinct
    expect(idSet.size).toEqual(repeat);
  });
});

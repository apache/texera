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
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { WorkflowCompilingService } from "../compile-workflow/workflow-compiling.service";
import {
  createListOperatorsInCurrentWorkflowTool,
  createListLinksInCurrentWorkflowTool,
  createListAllOperatorTypesTool,
  createGetOperatorInCurrentWorkflowTool,
  createGetOperatorPropertiesSchemaTool,
  createGetOperatorPortsInfoTool,
  createGetOperatorMetadataTool,
  toolWithTimeout,
} from "./workflow-tools";

describe("Workflow Tools", () => {
  let mockWorkflowActionService: jasmine.SpyObj<WorkflowActionService>;
  let mockWorkflowUtilService: jasmine.SpyObj<WorkflowUtilService>;
  let mockOperatorMetadataService: jasmine.SpyObj<OperatorMetadataService>;
  let mockWorkflowCompilingService: jasmine.SpyObj<WorkflowCompilingService>;

  beforeEach(() => {
    mockWorkflowActionService = jasmine.createSpyObj("WorkflowActionService", ["getTexeraGraph"]);
    mockWorkflowUtilService = jasmine.createSpyObj("WorkflowUtilService", ["getOperatorTypeList"]);
    mockOperatorMetadataService = jasmine.createSpyObj("OperatorMetadataService", ["getOperatorSchema"]);
    mockWorkflowCompilingService = jasmine.createSpyObj("WorkflowCompilingService", [
      "getOperatorInputSchemaMap",
      "getOperatorOutputSchemaMap",
    ]);
  });

  describe("listOperatorsInCurrentWorkflowTool", () => {
    it("should return list of operators with IDs, types, and custom names", async () => {
      const mockOperators = [
        { operatorID: "op1", operatorType: "ScanSource", customDisplayName: "Scan 1" },
        { operatorID: "op2", operatorType: "Filter", customDisplayName: "Filter 1" },
      ];

      const mockGraph = {
        getAllOperators: () => mockOperators,
      };
      mockWorkflowActionService.getTexeraGraph.and.returnValue(mockGraph as any);

      const tool = createListOperatorsInCurrentWorkflowTool(mockWorkflowActionService);
      const result = await (tool as any).execute({}, {});

      expect((result as any).success).toBe(true);
      expect((result as any).count).toBe(2);
      expect((result as any).operators).toEqual([
        { operatorId: "op1", operatorType: "ScanSource", customDisplayName: "Scan 1" },
        { operatorId: "op2", operatorType: "Filter", customDisplayName: "Filter 1" },
      ]);
    });

    it("should handle errors gracefully", async () => {
      mockWorkflowActionService.getTexeraGraph.and.throwError("Graph error");

      const tool = createListOperatorsInCurrentWorkflowTool(mockWorkflowActionService);
      const result = await (tool as any).execute({}, {});

      expect((result as any).success).toBe(false);
      expect((result as any).error).toContain("Graph error");
    });
  });

  describe("listLinksInCurrentWorkflowTool", () => {
    it("should return list of links", async () => {
      const mockLinks = [
        { linkID: "link1", source: { operatorID: "op1" }, target: { operatorID: "op2" } },
        { linkID: "link2", source: { operatorID: "op2" }, target: { operatorID: "op3" } },
      ];

      const mockGraph = {
        getAllLinks: () => mockLinks,
      };
      mockWorkflowActionService.getTexeraGraph.and.returnValue(mockGraph as any);

      const tool = createListLinksInCurrentWorkflowTool(mockWorkflowActionService);
      const result = await (tool as any).execute({}, {});

      expect((result as any).success).toBe(true);
      expect((result as any).count).toBe(2);
      expect((result as any).links).toEqual(mockLinks);
    });
  });

  describe("listAllOperatorTypesTool", () => {
    it("should return list of all operator types", async () => {
      const mockTypes = ["ScanSource", "Filter", "Join", "Aggregate"];
      mockWorkflowUtilService.getOperatorTypeList.and.returnValue(mockTypes);

      const tool = createListAllOperatorTypesTool(mockWorkflowUtilService);
      const result = await (tool as any).execute({}, {});

      expect((result as any).success).toBe(true);
      expect((result as any).count).toBe(4);
      expect((result as any).operatorTypes).toEqual(mockTypes);
    });
  });

  describe("getOperatorInCurrentWorkflowTool", () => {
    it("should return operator details with input and output schemas", async () => {
      const mockOperator = { operatorID: "op1", operatorType: "ScanSource" };
      const mockInputSchema = {
        port1: [{ attributeName: "field1", attributeType: "string" }],
      };
      const mockOutputSchema = {
        port1: [{ attributeName: "field1", attributeType: "string" }],
      };

      const mockGraph = {
        getOperator: (id: string) => mockOperator,
      };
      mockWorkflowActionService.getTexeraGraph.and.returnValue(mockGraph as any);
      mockWorkflowCompilingService.getOperatorInputSchemaMap.and.returnValue(mockInputSchema as any);
      mockWorkflowCompilingService.getOperatorOutputSchemaMap.and.returnValue(mockOutputSchema as any);

      const tool = createGetOperatorInCurrentWorkflowTool(mockWorkflowActionService, mockWorkflowCompilingService);
      const result = await (tool as any).execute({ operatorId: "op1" }, {});

      expect((result as any).success).toBe(true);
      expect((result as any).operator).toEqual(mockOperator);
      expect((result as any).inputSchema).toEqual(mockInputSchema);
      expect((result as any).outputSchema).toEqual(mockOutputSchema);
    });

    it("should return empty schemas when not available", async () => {
      const mockOperator = { operatorID: "op1", operatorType: "ScanSource" };

      const mockGraph = {
        getOperator: (id: string) => mockOperator,
      };
      mockWorkflowActionService.getTexeraGraph.and.returnValue(mockGraph as any);
      mockWorkflowCompilingService.getOperatorInputSchemaMap.and.returnValue(undefined);
      mockWorkflowCompilingService.getOperatorOutputSchemaMap.and.returnValue(undefined);

      const tool = createGetOperatorInCurrentWorkflowTool(mockWorkflowActionService, mockWorkflowCompilingService);
      const result = await (tool as any).execute({ operatorId: "op1" }, {});

      expect((result as any).success).toBe(true);
      expect((result as any).inputSchema).toEqual({});
      expect((result as any).outputSchema).toEqual({});
    });
  });

  describe("getOperatorPropertiesSchemaTool", () => {
    it("should return properties schema for operator type", async () => {
      const mockSchema = {
        jsonSchema: {
          properties: { prop1: { type: "string" } },
          required: ["prop1"],
          definitions: {},
        },
      };
      mockOperatorMetadataService.getOperatorSchema.and.returnValue(mockSchema as any);

      const tool = createGetOperatorPropertiesSchemaTool(mockOperatorMetadataService);
      const result = await (tool as any).execute({ operatorType: "ScanSource" }, {});

      expect((result as any).success).toBe(true);
      expect((result as any).operatorType).toBe("ScanSource");
      expect((result as any).propertiesSchema).toEqual({
        properties: { prop1: { type: "string" } },
        required: ["prop1"],
        definitions: {},
      });
    });
  });

  describe("getOperatorPortsInfoTool", () => {
    it("should return port information for operator type", async () => {
      const mockSchema = {
        additionalMetadata: {
          inputPorts: [{ portID: "input-0" }],
          outputPorts: [{ portID: "output-0" }],
          dynamicInputPorts: false,
          dynamicOutputPorts: false,
        },
      };
      mockOperatorMetadataService.getOperatorSchema.and.returnValue(mockSchema as any);

      const tool = createGetOperatorPortsInfoTool(mockOperatorMetadataService);
      const result = await (tool as any).execute({ operatorType: "Filter" }, {});

      expect((result as any).success).toBe(true);
      expect((result as any).portsInfo).toEqual(mockSchema.additionalMetadata);
    });
  });

  describe("getOperatorMetadataTool", () => {
    it("should return metadata for operator type", async () => {
      const mockSchema = {
        additionalMetadata: {
          userFriendlyName: "Scan Source",
          operatorDescription: "Reads data from a source",
          operatorGroupName: "Source",
        },
        operatorVersion: "1.0",
      };
      mockOperatorMetadataService.getOperatorSchema.and.returnValue(mockSchema as any);

      const tool = createGetOperatorMetadataTool(mockOperatorMetadataService);
      const result = await (tool as any).execute({ operatorType: "ScanSource" }, {});

      expect((result as any).success).toBe(true);
      expect((result as any).metadata).toEqual(mockSchema.additionalMetadata);
      expect((result as any).operatorVersion).toBe("1.0");
    });
  });

  describe("toolWithTimeout", () => {
    it("should execute tool successfully within timeout", async () => {
      const mockTool = {
        execute: jasmine.createSpy("execute").and.returnValue(Promise.resolve({ success: true, data: "result" })),
      };

      const wrappedTool = toolWithTimeout(mockTool);
      const result = await (wrappedTool as any).execute({ param: "value" });

      expect((result as any).success).toBe(true);
      expect((result as any).data).toBe("result");
      expect(mockTool.execute).toHaveBeenCalled();
    });

    it("should propagate non-timeout errors", async () => {
      const mockTool = {
        execute: jasmine.createSpy("execute").and.returnValue(Promise.reject(new Error("Custom error"))),
      };

      const wrappedTool = toolWithTimeout(mockTool);

      try {
        await (wrappedTool as any).execute({});
        fail("Should have thrown an error");
      } catch (error: any) {
        expect(error.message).toBe("Custom error");
      }
    });
  });
});

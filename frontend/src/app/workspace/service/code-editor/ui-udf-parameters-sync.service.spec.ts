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

import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { PYTHON_UDF_V2_OP_TYPE } from "../workflow-graph/model/workflow-graph";
import { UiUdfParametersParseError, UiUdfParametersParserService } from "./ui-udf-parameters-parser.service";
import type { UiUdfParameter } from "./ui-udf-parameters-parser.service";
import { UiUdfParametersSyncService } from "./ui-udf-parameters-sync.service";
import type { Mock } from "vitest";
import { vi as vitest } from "vitest";
import * as Yjs from "yjs";

describe("UiUdfParametersSyncService", () => {
  const operatorId = "operator-1";
  const code = "self.UiParameter(...)";

  let service: UiUdfParametersSyncService;
  let parserServiceMock: { parse: Mock };
  let graphMock: { getOperator: Mock; getSharedOperatorType: Mock };
  let operator: { operatorType: string; operatorProperties: { uiParameters: UiUdfParameter[] } };

  beforeEach(() => {
    operator = { operatorType: PYTHON_UDF_V2_OP_TYPE, operatorProperties: { uiParameters: [] } };
    graphMock = {
      getOperator: vitest
        .fn()
        .mockImplementation((requestedOperatorId: string) =>
          requestedOperatorId === operatorId ? operator : undefined
        ),
      getSharedOperatorType: vitest.fn(),
    };
    parserServiceMock = { parse: vitest.fn() };
    service = new UiUdfParametersSyncService(
      { getTexeraGraph: vitest.fn().mockReturnValue(graphMock) } as unknown as WorkflowActionService,
      parserServiceMock as unknown as UiUdfParametersParserService
    );
  });

  [
    {
      description: "preserve values from current parameter names",
      existingParameters: [parameter("count", "integer", "42")],
      parsedParameters: [parameter("count", "integer"), parameter("name", "string")],
      expectedParameters: [parameter("count", "integer", "42"), parameter("name", "string", "")],
    },
    {
      description: "remove stale parameters while preserving retained values",
      existingParameters: [parameter("count", "integer", "42"), parameter("removed", "string", "stale")],
      parsedParameters: [parameter("count", "integer"), parameter("name", "string")],
      expectedParameters: [parameter("count", "integer", "42"), parameter("name", "string", "")],
    },
  ].forEach(({ description, existingParameters, parsedParameters, expectedParameters }) => {
    it(`should ${description}`, () => {
      operator.operatorProperties.uiParameters = existingParameters;
      parserServiceMock.parse.mockReturnValue(parsedParameters);

      const parametersChangedObserver = observeParameterChanges();

      service.syncStructureFromCode(operatorId, code);

      expect(parametersChangedObserver).toHaveBeenCalledWith({ operatorId, parameters: expectedParameters });
      expect(parametersChangedObserver).toHaveBeenCalledOnce();
    });
  });

  it("should not emit when the merged parameters are unchanged", () => {
    operator.operatorProperties.uiParameters = [parameter("count", "integer", "42")];
    parserServiceMock.parse.mockReturnValue([parameter("count", "integer")]);

    const parametersChangedObserver = observeParameterChanges();

    service.syncStructureFromCode(operatorId, code);

    expect(parametersChangedObserver).not.toHaveBeenCalled();
  });

  it("should emit parser errors without replacing the current parameters", () => {
    operator.operatorProperties.uiParameters = [parameter("count", "integer", "42")];
    parserServiceMock.parse.mockImplementation(() => {
      throw new UiUdfParametersParseError("Only one Python UDF class can declare UiParameter values.");
    });

    const parametersChangedObserver = observeParameterChanges();
    const parseErrorObserver = vitest.fn();
    service.uiParametersParseError$.subscribe(parseErrorObserver);

    service.syncStructureFromCode(operatorId, code);

    expect(parametersChangedObserver).not.toHaveBeenCalled();
    expect(parseErrorObserver).toHaveBeenCalledWith({
      operatorId,
      message: "Only one Python UDF class can declare UiParameter values.",
    });
  });

  it("should not parse code for non-Python UDF operators", () => {
    operator.operatorType = "Projection";

    const parametersChangedObserver = observeParameterChanges();

    service.syncStructureFromCode(operatorId, code);

    expect(parserServiceMock.parse).not.toHaveBeenCalled();
    expect(parametersChangedObserver).not.toHaveBeenCalled();
  });

  it("should read code from the shared operator property when editor code is omitted", () => {
    const sharedCode = 'self.UiParameter("count", AttributeType.INT)';
    graphMock.getSharedOperatorType.mockReturnValue(sharedOperatorType(sharedCode));
    parserServiceMock.parse.mockReturnValue([parameter("count", "integer")]);

    const parametersChangedObserver = observeParameterChanges();

    service.syncStructureFromCode(operatorId);

    expect(parserServiceMock.parse).toHaveBeenCalledWith(sharedCode);
    expect(parametersChangedObserver).toHaveBeenCalledWith({
      operatorId,
      parameters: [parameter("count", "integer")],
    });
  });

  it("should warn and skip sync when shared code cannot be read", () => {
    const sharedCodeError = new Error("missing shared operator");
    const consoleWarnSpy = vitest.spyOn(console, "warn").mockImplementation(() => undefined);
    graphMock.getSharedOperatorType.mockImplementation(() => {
      throw sharedCodeError;
    });

    try {
      service.syncStructureFromCode(operatorId);

      expect(parserServiceMock.parse).not.toHaveBeenCalled();
      expect(consoleWarnSpy).toHaveBeenCalledWith(
        "Unable to read Python UDF code from shared operator properties.",
        sharedCodeError
      );
    } finally {
      consoleWarnSpy.mockRestore();
    }
  });

  it("should debounce YText changes and clean up the observer", () => {
    vitest.useFakeTimers();
    try {
      const sharedCodeText = sharedText('self.UiParameter("count", AttributeType.INT)');
      parserServiceMock.parse.mockReturnValue([parameter("count", "integer")]);

      const parametersChangedObserver = observeParameterChanges();
      const cleanup = service.attachToYCode(operatorId, sharedCodeText);

      expect(parserServiceMock.parse).toHaveBeenCalledOnce();
      expect(parametersChangedObserver).toHaveBeenCalledOnce();

      sharedCodeText.insert(sharedCodeText.length, "\n# changed");
      vitest.advanceTimersByTime(199);
      expect(parserServiceMock.parse).toHaveBeenCalledOnce();

      vitest.advanceTimersByTime(1);
      expect(parserServiceMock.parse).toHaveBeenCalledTimes(2);
      expect(parametersChangedObserver).toHaveBeenCalledTimes(2);

      cleanup();
      sharedCodeText.insert(sharedCodeText.length, "\n# after cleanup");
      vitest.advanceTimersByTime(200);
      expect(parserServiceMock.parse).toHaveBeenCalledTimes(2);
      expect(parametersChangedObserver).toHaveBeenCalledTimes(2);
    } finally {
      vitest.useRealTimers();
    }
  });

  function observeParameterChanges(): Mock {
    const parametersChangedObserver = vitest.fn();
    service.uiParametersChanged$.subscribe(parametersChangedObserver);
    return parametersChangedObserver;
  }
});

function parameter(
  attributeName: string,
  attributeType: UiUdfParameter["attribute"]["attributeType"],
  value = ""
): UiUdfParameter {
  return { attribute: { attributeName, attributeType }, value };
}

function sharedOperatorType(code: string): Yjs.Map<unknown> {
  const yjsDocument = new Yjs.Doc();
  const sharedOperator = yjsDocument.getMap<unknown>("operator");
  const operatorProperties = new Yjs.Map<unknown>();

  operatorProperties.set("code", sharedText(code));
  sharedOperator.set("operatorProperties", operatorProperties);
  return sharedOperator;
}

function sharedText(text: string): Yjs.Text {
  const yjsDocument = new Yjs.Doc();
  const sharedRootMap = yjsDocument.getMap<unknown>("root");
  const codeText = new Yjs.Text();

  sharedRootMap.set("code", codeText);
  codeText.insert(0, text);
  return codeText;
}

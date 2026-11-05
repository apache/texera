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

import fixture from "../../../../../../common/workflow-operator/src/test/resources/udf/ui-parameter-contract.json";
import { UiUdfParametersParserService } from "./ui-udf-parameters-parser.service";

const parser = new UiUdfParametersParserService();
for (const language of ["python", "r"] as const) {
  describe(`${language} declaration contract`, () => {
    it("validates new names and types before parsing existing declarations", () => {
      const code =
        language === "python"
          ? 'class ProcessTupleOperator(UDFOperatorV2):\n    def open(self):\n        self.UiParameter("x", AttributeType.INT)\n        self.UiParameter("x", AttributeType.INT)'
          : 'UiParameter("x", AttributeType$INT); UiParameter("x", AttributeType$INT)';
      expect(() => parser.computeParameterInsertion(code, "", "string", language)).toThrow("name is required");
      expect(() => parser.computeParameterInsertion(code, "name", "binary", language)).toThrow("not supported");
    });
    fixture.declarations.forEach((example, index) => {
      it(`recognizes shared declaration ${index}: ${example.token}`, () => {
        const separator = language === "python" ? "." : "$";
        const resource = example.resource ? `, value=Resource${separator}${example.resource}` : "";
        const call = `${language === "python" ? "self." : ""}UiParameter(${JSON.stringify(example.name)}, AttributeType${separator}${example.token}${resource})`;
        const code =
          language === "python"
            ? `class ProcessTupleOperator(UDFOperatorV2):\n    def open(self):\n        ${call}\n`
            : `function() { ${call} }`;
        const expected = example.attributeType
          ? [
              {
                attribute: { attributeName: example.name, attributeType: example.attributeType },
                value: "",
                ...(example.inputType ? { inputType: example.inputType } : {}),
              },
            ]
          : [];
        expect(parser.parse(code, language)).toEqual(expected);
      });
    });
    it("rejects duplicate names through the shared validation contract", () => {
      const code =
        language === "python"
          ? 'class ProcessTupleOperator(UDFOperatorV2):\n    def open(self):\n        self.UiParameter("x", AttributeType.INT)\n        self.UiParameter("x", AttributeType.INT)'
          : 'UiParameter("x", AttributeType$INT); UiParameter("x", AttributeType$INT)';
      expect(() => parser.parse(code, language)).toThrow("UiParameter name 'x' is declared more than once.");
    });
  });
}

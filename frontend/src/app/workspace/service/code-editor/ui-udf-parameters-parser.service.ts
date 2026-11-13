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

import { Injectable } from "@angular/core";
import type { AttributeType } from "../../types/workflow-compiling.interface";
import { PythonUiUdfParametersParser } from "./python-ui-udf-parameters-parser";
import { parseRUiParameters, computeRParameterInsertion } from "./r-ui-udf-parameters-parser";
import type { UiUdfLanguage, UiUdfParameter } from "./ui-udf-parameter";
import { assertUniqueParameterNames, UI_PARAMETER_TOKENS, UiUdfParametersEditError } from "./ui-udf-parameter";
export {
  UiUdfParameter,
  UiUdfParametersParseError,
  UiUdfParametersEditError,
  MODEL_INPUT_TYPE,
  DATASET_INPUT_TYPE,
} from "./ui-udf-parameter";

/** Selects a language parser; metadata policy lives in the shared parameter contract. */
@Injectable({ providedIn: "root" })
export class UiUdfParametersParserService {
  private readonly python = new PythonUiUdfParametersParser();

  parse(code: string, language: UiUdfLanguage): UiUdfParameter[] {
    return assertUniqueParameterNames(language === "r" ? parseRUiParameters(code) : this.python.parse(code));
  }

  computeParameterInsertion(
    code: string,
    name: string,
    type: AttributeType,
    language: UiUdfLanguage
  ): Readonly<{ offset: number; text: string }> {
    const attributeName = name.trim();
    const token = UI_PARAMETER_TOKENS[type];
    if (!attributeName) throw new UiUdfParametersEditError("UiParameter name is required.");
    if (!Object.hasOwn(UI_PARAMETER_TOKENS, type))
      throw new UiUdfParametersEditError(`UiParameter type '${type}' is not supported.`);
    if (this.parse(code, language).some(parameter => parameter.attribute.attributeName === attributeName))
      throw new UiUdfParametersEditError(`UiParameter name '${attributeName}' is declared already.`);
    return language === "r"
      ? computeRParameterInsertion(code, attributeName, token!)
      : this.python.computeParameterInsertion(code, attributeName, token!);
  }
}

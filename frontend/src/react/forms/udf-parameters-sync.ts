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

import type { Text as YText } from "yjs";
import { PythonUiUdfParametersParser } from "../../core/workspace/service/code-editor/python-ui-udf-parameters-parser";
import {
  parseRUiParameters,
  computeRParameterInsertion,
} from "../../core/workspace/service/code-editor/r-ui-udf-parameters-parser";
import {
  assertUniqueParameterNames,
  UI_PARAMETER_TOKENS,
  UiUdfParametersEditError,
  UiUdfParametersParseError,
} from "../../core/workspace/service/code-editor/ui-udf-parameter";
import type { AttributeType } from "../../core/workspace/types/workflow-compiling.interface";
import type { UiUdfParameterValue } from "./ui-udf-parameters";

export type UiUdfLanguage = "python" | "r";

/** The declaration grammars are retained verbatim; this facade carries no Angular service lifecycle. */
export class UiUdfParametersParser {
  private readonly python = new PythonUiUdfParametersParser();

  parse(code: string, language: UiUdfLanguage): UiUdfParameterValue[] {
    return assertUniqueParameterNames(language === "r" ? parseRUiParameters(code) : this.python.parse(code));
  }

  computeInsertion(
    code: string,
    name: string,
    type: string,
    language: UiUdfLanguage
  ): Readonly<{ offset: number; text: string }> {
    const candidate = name.trim();
    if (!candidate) throw new UiUdfParametersEditError("UiParameter name is required.");
    if (!Object.hasOwn(UI_PARAMETER_TOKENS, type))
      throw new UiUdfParametersEditError(`UiParameter type '${type}' is not supported.`);
    if (this.parse(code, language).some(parameter => parameter.attribute.attributeName === candidate))
      throw new UiUdfParametersEditError(`UiParameter name '${candidate}' is declared already.`);
    const token = UI_PARAMETER_TOKENS[type as AttributeType]!;
    return language === "r"
      ? computeRParameterInsertion(code, candidate, token)
      : this.python.computeParameterInsertion(code, candidate, token);
  }
}

export interface UiUdfParameterSyncOptions {
  code: YText;
  language: UiUdfLanguage;
  getRows: () => readonly UiUdfParameterValue[];
  onRows: (rows: UiUdfParameterValue[]) => void;
  onError?: (message?: string) => void;
}

/** One independent binding per shared code buffer; dispose it with the editor/operator view. */
export function createUiUdfParameterSync({ code, language, getRows, onRows, onError }: UiUdfParameterSyncOptions) {
  const parser = new UiUdfParametersParser();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let disposed = false;
  const sync = () => {
    if (disposed) return;
    clearTimeout(timer);
    timer = undefined;
    let parsed: UiUdfParameterValue[];
    try {
      parsed = parser.parse(code.toString(), language);
    } catch (error) {
      if (error instanceof UiUdfParametersParseError) {
        onError?.(error.message);
        return;
      }
      throw error;
    }
    onError?.(undefined);
    const current = getRows();
    const key = (row: UiUdfParameterValue) => `${row.attribute.attributeName}:${row.inputType ?? ""}`;
    const previousValues = new Map(current.map(row => [key(row), row.value]));
    const rows = parsed.map(row => ({ ...row, value: previousValues.get(key(row)) ?? "" }));
    if (JSON.stringify(current) !== JSON.stringify(rows)) onRows(rows);
  };
  const onCode = () => {
    clearTimeout(timer);
    timer = setTimeout(sync, 200);
  };
  code.observe(onCode);
  sync();
  return {
    sync,
    addParameter(name: string, attributeType: string) {
      if (disposed) throw new UiUdfParametersEditError("UDF parameter binding is disposed.");
      const edit = parser.computeInsertion(code.toString(), name, attributeType, language);
      code.insert(edit.offset, edit.text);
      sync();
    },
    dispose() {
      if (disposed) return;
      disposed = true;
      clearTimeout(timer);
      timer = undefined;
      code.unobserve(onCode);
    },
  };
}

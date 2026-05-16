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

import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { OperatorMetadata, OperatorSchema } from "../../types/operator-schema.interface";
import { map, shareReplay } from "rxjs/operators";

export const OPERATOR_METADATA_ENDPOINT = "resources/operator-metadata";

const addDictionaryAPIAddress = "/api/resources/dictionary/";
const getDictionaryAPIAddress = "/api/upload/dictionary/";

// interface only containing public methods
export type IOperatorMetadataService = Pick<OperatorMetadataService, keyof OperatorMetadataService>;

/**
 * OperatorMetadataService talks to the backend to fetch the operator metadata, which contains a list of operator schemas.
 * Each operator schema contains all the information related to an operator, for example, operatorType, userFriendlyName,
 *  and the jsonSchema of its properties.
 *
 * Components and Services should call getOperatorMetadata() and subscribe to the Observable to get the metadata,
 *  after the metadata is fetched from the backend, it will be broadcast through the observable.
 *
 * The mock operator metadata is also available in mock-operator-metadata.ts for testing.
 * It contains the schemas for 3 operators.
 * @author Zuozhi Wang
 *
 */
@Injectable({
  providedIn: "root",
})
export class OperatorMetadataService {
  // holds the current version of operator metadata
  private currentOperatorMetadata: OperatorMetadata | undefined;

  private operatorMetadataObservable = this.httpClient
    .get<OperatorMetadata>(`${AppSettings.getApiEndpoint()}/${OPERATOR_METADATA_ENDPOINT}`)
    .pipe(
      map(metadata => OperatorMetadataService.sanitizeMetadata(metadata)),
      shareReplay(1)
    );

  /**
   * The backend's reflective JSON-schema generator emits `{"nullable": true}`
   * for `Option[...]` fields whose inner type it can't enumerate
   * (e.g. `Option[MacroBody]` on `MacroOpDesc`). Ajv strict-mode rejects
   * `nullable` without a sibling `type`, which throws everywhere the schema
   * gets compiled — validation, property editor, dynamic schema, the YJS
   * shared-model handler, etc. Strip those orphan `nullable` flags as the
   * metadata comes off the wire so downstream code never sees them.
   *
   * The proper long-term fix is to teach the generator to emit a real type
   * (see project memory `project_macroopdesc_schema_ajv_bug.md`); this
   * sanitizer is defense-in-depth.
   */
  private static sanitizeMetadata(metadata: OperatorMetadata): OperatorMetadata {
    metadata.operators.forEach(op => OperatorMetadataService.sanitizeSchemaNode(op.jsonSchema));
    return metadata;
  }

  private static sanitizeSchemaNode(node: unknown): void {
    if (node === null || typeof node !== "object") return;
    if (Array.isArray(node)) {
      node.forEach(child => OperatorMetadataService.sanitizeSchemaNode(child));
      return;
    }
    const obj = node as Record<string, unknown>;
    if (obj["nullable"] === true && obj["type"] === undefined) {
      if (obj["$ref"] !== undefined) {
        // "nullable: true, $ref: X" — Ajv ignores $ref siblings under Draft-07 strict
        // rules. Convert to anyOf so that null AND the referenced type are both valid.
        // This preserves round-trip properties that serialize Option[T] as null.
        const ref = obj["$ref"];
        delete obj["nullable"];
        delete obj["$ref"];
        obj["anyOf"] = [{ type: "null" }, { $ref: ref }];
      } else {
        delete obj["nullable"];
      }
    }
    for (const key of ["properties", "definitions", "patternProperties"]) {
      const dict = obj[key];
      if (dict && typeof dict === "object" && !Array.isArray(dict)) {
        for (const childKey of Object.keys(dict as Record<string, unknown>)) {
          OperatorMetadataService.sanitizeSchemaNode((dict as Record<string, unknown>)[childKey]);
        }
      }
    }
    for (const key of ["items", "additionalProperties", "not"]) {
      if (obj[key]) OperatorMetadataService.sanitizeSchemaNode(obj[key]);
    }
    for (const key of ["oneOf", "anyOf", "allOf"]) {
      const arr = obj[key];
      if (Array.isArray(arr)) {
        arr.forEach(child => OperatorMetadataService.sanitizeSchemaNode(child));
      }
    }
  }

  constructor(private httpClient: HttpClient) {
    this.getOperatorMetadata().subscribe(data => {
      this.currentOperatorMetadata = data;
    });
  }

  /**
   * Gets an Observable for operator metadata.
   * This observable will emit OperatorMetadataValue after the data is fetched from the backend.
   *
   * // TODO: refactor this to 2 functions: getOperatorMetadataStream() and getOperatorMetadata()
   */
  public getOperatorMetadata(): Observable<OperatorMetadata> {
    return this.operatorMetadataObservable;
  }

  public getOperatorSchema(operatorType: string): OperatorSchema {
    if (!this.currentOperatorMetadata) {
      throw new Error("operator metadata is undefined");
    }
    const operatorSchema = this.currentOperatorMetadata.operators.find(schema => schema.operatorType === operatorType);
    if (!operatorSchema) {
      throw new Error(`can\'t find operator schema of type ${operatorType}`);
    }
    return operatorSchema;
  }

  /**
   * Returns true if the operator type exists *in the current operator metadata*.
   * For example, if the first HTTP request to the backend hasn't returned yet,
   *  the current operator metadata is empty, and no operator type exists.
   *
   * @param operatorType - Operator name string that we are checking for existence *in the current operator metadata*
   * @param userFriendlyNameFilter - If true, checks if operatorType matches an operator's user friendly or type name
   * @param caseInsensitive - If true, operatorType checking becomes case insensitive
   */
  public operatorTypeExists(
    operatorType: string,
    userFriendlyNameFilter: boolean = false,
    caseInsensitive: boolean = false
  ): boolean {
    if (!this.currentOperatorMetadata) {
      return false;
    }
    const operator = this.currentOperatorMetadata.operators.filter(op => {
      let operatorTypeInMetadata = op.operatorType;
      let operatorNameInMetadata = op.additionalMetadata.userFriendlyName;
      if (caseInsensitive) {
        operatorTypeInMetadata = operatorTypeInMetadata.toLowerCase();
        operatorNameInMetadata = operatorNameInMetadata.toLowerCase();
        operatorType = operatorType.toLowerCase();
      }
      if (userFriendlyNameFilter) {
        return operatorTypeInMetadata === operatorType || operatorNameInMetadata === operatorType;
      } else {
        return operatorTypeInMetadata === operatorType;
      }
    });
    if (operator.length === 0) {
      return false;
    }
    return true;
  }
}

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

import { describe, expect, test } from "bun:test";
import { readEvalConfig } from "./config";

describe("readEvalConfig", () => {
  test("needs only a model, and fills in local-dev defaults", () => {
    expect(readEvalConfig({ EVAL_MODEL: "claude-haiku-5-5" })).toEqual({
      model: "claude-haiku-5-5",
      username: "texera",
      password: "texera",
      fileServiceEndpoint: "http://localhost:9092",
      computingUnitId: 1,
      warehouseId: undefined,
      repeat: 3,
      caseIds: undefined,
      out: undefined,
    });
  });

  test("reads every setting, trims the case list and the endpoint's trailing slash", () => {
    const config = readEvalConfig({
      EVAL_MODEL: "m",
      EVAL_USERNAME: "u",
      EVAL_PASSWORD: "p",
      EVAL_FILE_SERVICE_ENDPOINT: "http://gateway:8080/",
      EVAL_COMPUTING_UNIT_ID: "4",
      EVAL_WAREHOUSE_ID: "2",
      EVAL_REPEAT: "5",
      EVAL_CASES: " build-workflow, ,docs-question ",
      EVAL_OUT: "/tmp/eval.json",
    });
    expect(config).toEqual({
      model: "m",
      username: "u",
      password: "p",
      fileServiceEndpoint: "http://gateway:8080",
      computingUnitId: 4,
      warehouseId: 2,
      repeat: 5,
      caseIds: ["build-workflow", "docs-question"],
      out: "/tmp/eval.json",
    });
  });

  test.each([
    [{}],
    [{ EVAL_MODEL: "" }],
    [{ EVAL_MODEL: "m", EVAL_REPEAT: "0" }],
    [{ EVAL_MODEL: "m", EVAL_REPEAT: "two" }],
    [{ EVAL_MODEL: "m", EVAL_COMPUTING_UNIT_ID: "-1" }],
    [{ EVAL_MODEL: "m", EVAL_FILE_SERVICE_ENDPOINT: "not a url" }],
  ])("refuses %p", source => {
    expect(() => readEvalConfig(source)).toThrow();
  });
});

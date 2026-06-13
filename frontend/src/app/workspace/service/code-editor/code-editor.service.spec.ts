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

import { CodeEditorService } from "./code-editor.service";

describe("CodeEditorService", () => {
  let service: CodeEditorService;

  beforeEach(() => {
    service = new CodeEditorService();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("should emit true after setEditorState is called with true", () => {
    let value: boolean | undefined;
    service.getEditorState("op1").subscribe(v => (value = v));
    service.setEditorState("op1", true);
    expect(value).toBe(true);
  });

  it("should emit false after setEditorState is called with false", () => {
    let value: boolean | undefined;
    service.getEditorState("op1").subscribe(v => (value = v));
    service.setEditorState("op1", false);
    expect(value).toBe(false);
  });

  it("should track state independently for different operator IDs", () => {
    let valueA: boolean | undefined;
    let valueB: boolean | undefined;
    service.getEditorState("opA").subscribe(v => (valueA = v));
    service.getEditorState("opB").subscribe(v => (valueB = v));
    service.setEditorState("opA", true);
    expect(valueA).toBe(true);
    expect(valueB).toBe(false);
  });
});

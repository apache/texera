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

import { ViewContainerRef } from "@angular/core";
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

  it("seeds the state when setEditorState runs before any getEditorState", () => {
    // Nothing has asked for "fresh" yet, so setEditorState has no subject to push
    // onto and must create one already carrying the requested value. Every other
    // test in this file calls getEditorState first, which pre-creates the subject
    // seeded to false, so this is the only path that exercises the seeding.
    service.setEditorState("fresh", true);

    let value: boolean | undefined;
    service.getEditorState("fresh").subscribe(v => (value = v));
    expect(value).toBe(true);

    // The seeded subject is the same one getEditorState handed out -- a later
    // write reaches the existing subscriber rather than a replacement.
    service.setEditorState("fresh", false);
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

  // The outlet the editor opens into, attached by whichever view is in front (#8760).
  describe("the editor host", () => {
    const outlet = (name: string) => ({ name }) as unknown as ViewContainerRef;

    it("has no host until a view attaches one", () => {
      expect(service.host).toBeUndefined();

      const canvas = outlet("canvas");
      service.attachHost(canvas);

      expect(service.host).toBe(canvas);
    });

    it("lets go of the host that detaches, and keeps one that has replaced it", () => {
      const canvas = outlet("canvas");
      const form = outlet("form");
      service.attachHost(canvas);
      service.attachHost(form);

      // The canvas's late destroy must not take the form's host with it.
      service.detachHost(canvas);
      expect(service.host).toBe(form);

      service.detachHost(form);
      expect(service.host).toBeUndefined();
    });
  });
});

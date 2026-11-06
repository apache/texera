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

import * as Y from "yjs";
import {
  applyMinimalTextEdit,
  clampDescription,
  descriptionText,
  MAX_OPERATOR_DESCRIPTION_LENGTH,
} from "./operator-description.util";

describe("operator-description.util", () => {
  describe("descriptionText", () => {
    it("reads strings and Y.Text as text", () => {
      const doc = new Y.Doc();
      const text = doc.getText("t");
      text.insert(0, "from ytext");
      expect(descriptionText("plain")).toEqual("plain");
      expect(descriptionText(text)).toEqual("from ytext");
    });

    it("reads anything else as no description", () => {
      for (const bad of [undefined, null, 5, true, { a: 1 }, ["x"], new Y.Map()]) {
        expect(descriptionText(bad)).toEqual("");
      }
    });
  });

  describe("clampDescription", () => {
    it("leaves text at or under the limit untouched", () => {
      const atLimit = "a".repeat(MAX_OPERATOR_DESCRIPTION_LENGTH);
      expect(clampDescription(atLimit)).toBe(atLimit);
      expect(clampDescription("")).toBe("");
    });

    it("cuts text over the limit to the limit", () => {
      expect(clampDescription("a".repeat(MAX_OPERATOR_DESCRIPTION_LENGTH + 5)).length).toBe(
        MAX_OPERATOR_DESCRIPTION_LENGTH
      );
    });

    it("never leaves half of a surrogate pair", () => {
      expect(clampDescription("ab🚀", 3)).toBe("ab");
      expect(clampDescription("a🚀b", 3)).toBe("a🚀");
    });
  });

  describe("applyMinimalTextEdit", () => {
    function textOf(initial: string): Y.Text {
      const doc = new Y.Doc();
      const text = doc.getText("t");
      text.insert(0, initial);
      return text;
    }

    it.each([
      ["", "new"],
      ["old", ""],
      ["hello world", "hello brave world"],
      ["hello brave world", "hello world"],
      ["abc", "xyz"],
      ["aaaa", "aa"],
      ["aa", "aaaa"],
      ["same", "same"],
      ["x🚀y", "x🎉y"],
      ["🚀", "🚀🚀"],
      ["line1\r\nline2", "line1\nline2"],
    ])("turns %j into %j", (from, to) => {
      const text = textOf(from);
      applyMinimalTextEdit(text, to);
      expect(text.toString()).toBe(to);
    });

    it("touches only the changed span", () => {
      const text = textOf("keep this, change that, keep this");
      let inserted = "";
      let deleted = 0;
      text.observe(e =>
        e.delta.forEach(op => {
          if (typeof op.insert === "string") inserted += op.insert;
          if (op.delete) deleted += op.delete;
        })
      );
      applyMinimalTextEdit(text, "keep this, edit that, keep this");
      expect(text.toString()).toBe("keep this, edit that, keep this");
      expect(inserted.length + deleted).toBeLessThanOrEqual("change".length + "edit".length);
    });

    it("merges two concurrent edits to different parts without duplicating the text", () => {
      const docA = new Y.Doc();
      docA.getText("t").insert(0, "The red fox jumps over the lazy dog");
      const docB = new Y.Doc();
      Y.applyUpdate(docB, Y.encodeStateAsUpdate(docA));

      docA.transact(() => applyMinimalTextEdit(docA.getText("t"), "The quick red fox jumps over the lazy dog"));
      docB.transact(() => applyMinimalTextEdit(docB.getText("t"), "The red fox jumps over the sleepy dog"));
      Y.applyUpdate(docA, Y.encodeStateAsUpdate(docB));
      Y.applyUpdate(docB, Y.encodeStateAsUpdate(docA));

      expect(docA.getText("t").toString()).toBe("The quick red fox jumps over the sleepy dog");
      expect(docB.getText("t").toString()).toBe(docA.getText("t").toString());
    });
  });
});

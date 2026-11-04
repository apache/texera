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

import { UiUdfParametersParserService } from "./ui-udf-parameters-parser.service";

describe("R UDF UI parameter declarations", () => {
  const parser = new UiUdfParametersParserService();
  const parse = (code: string) => parser.parse(code, "r");

  it("infers positional, named, multiline, aliased, and resource parameters", () => {
    expect(
      parse(`
      count <- UiParameter("count", AttributeType$INT)$value
      label <- UiParameter(type = AttributeType$STRING, name = "label")$value
      function(table, port) {
        UiParameter(
          "enabled", attr_type = AttributeType$BOOLEAN
        )$value
        UiParameter("model", AttributeType$STRING, value = Resource$MODEL)$value
        UiParameter("dataset", AttributeType$STRING, value = Resource$DATASET)$value
      }
    `)
    ).toEqual([
      { attribute: { attributeName: "count", attributeType: "integer" }, value: "" },
      { attribute: { attributeName: "label", attributeType: "string" }, value: "" },
      { attribute: { attributeName: "enabled", attributeType: "boolean" }, value: "" },
      { attribute: { attributeName: "model", attributeType: "string" }, value: "", inputType: "model" },
      { attribute: { attributeName: "dataset", attributeType: "string" }, value: "", inputType: "dataset" },
    ]);
  });

  it("ignores comments, quoted examples, raw strings, and calls on other objects", () => {
    expect(
      parse(`
      # UiParameter("comment", AttributeType$INT)
      text <- 'UiParameter("example", AttributeType$INT)'
      text <- r"---(UiParameter("raw", AttributeType$INT))---"
      object$UiParameter("member", AttributeType$INT)
      package::UiParameter("namespace", AttributeType$INT)
      UiParameter("active", AttributeType$DOUBLE)
    `)
    ).toEqual([{ attribute: { attributeName: "active", attributeType: "double" }, value: "" }]);
  });

  it("decodes escaped names and accepts Unicode without confusing delimiters in strings", () => {
    expect(parse(String.raw`UiParameter("café\n\"(x)", AttributeType$STRING)`)[0].attribute.attributeName).toBe(
      'café\n"(x)'
    );
  });

  it.each([
    [String.raw`"\x63\u0061\U00000066\u{e9}"`, "café"],
    [String.raw`"\143\141\146\351"`, "café"],
    [String.raw`'single\'quote'`, "single'quote"],
    ['r"---[raw # (name)]---"', "raw # (name)"],
    ["r'(don't)'", "don't"],
    ["R'--[can't # (name)]--'", "can't # (name)"],
    ["r'{can't )\" stop}'", "can't )\" stop"],
  ])("preserves R string literal decoding for %s", (literal, name) => {
    expect(parse(`UiParameter(${literal}, AttributeType$STRING)`)[0].attribute.attributeName).toBe(name);
  });

  it("keeps declarations after single-quoted raw strings containing apostrophes and quoted examples", () => {
    expect(
      parse(`text <- r'(don't )" UiParameter("hidden", AttributeType$INT))'
UiParameter("active", AttributeType$INT)`)
    ).toEqual([{ attribute: { attributeName: "active", attributeType: "integer" }, value: "" }]);
  });

  it.each([String.raw`"\q"`, String.raw`"\U00110000"`, '"unfinished', "r'(unfinished)\"", "r'--[unfinished]-'"])(
    "ignores invalid or unfinished string literals: %s",
    literal => {
      expect(parse(`UiParameter(${literal}, AttributeType$STRING)`)).toEqual([]);
    }
  );

  it.each([
    'UiParameter("x")',
    'UiParameter("x", "integer")',
    'UiParameter("x", Other$INT)',
    'UiParameter("x", AttributeType$BINARY)',
    'UiParameter("x", AttributeType$INT, value=Resource$MODEL)',
    'UiParameter("x", AttributeType$STRING, value=Resource$OTHER)',
    'UiParameter("x", AttributeType$INT, type=AttributeType$DOUBLE)',
    'UiParameter("x", AttributeType$INT, unknown=TRUE)',
    'UiParameter("x", Resource$MODEL, attr_type=AttributeType$STRING)',
    'UiParameter("x", type=AttributeType$STRING, attr_type=AttributeType$STRING)',
    'UiParameter("x", AttributeType$INT',
  ])("does not infer an unsupported or incomplete declaration: %s", code => {
    expect(parse(code)).toEqual([]);
  });

  it("rejects duplicate names and handles an empty script", () => {
    expect(parse("")).toEqual([]);
    expect(() => parse('UiParameter("x", AttributeType$INT); UiParameter("x", AttributeType$STRING)')).toThrow(
      "UiParameter name 'x' is declared more than once."
    );
  });

  it("adds a declaration before the final function for both tuple and table scripts", () => {
    for (const code of ["function(table, port) { table }", "coro::generator(function() { yield(list()) })"]) {
      const edit = parser.computeParameterInsertion(code, "my count", "integer", "r");
      const updated = code.slice(0, edit.offset) + edit.text + code.slice(edit.offset);
      expect(updated).toContain('my_count <- UiParameter(name="my count", type=AttributeType$INT)$value');
      expect(updated.endsWith(code)).toBe(true);
      expect(parse(updated)).toEqual([
        { attribute: { attributeName: "my count", attributeType: "integer" }, value: "" },
      ]);
    }
  });

  it.each(["if", "1st", ".1st", "...", "..1", "UiParameter", "AttributeType", "Resource", ".texera_parameters"])(
    "keeps assignment targets valid without overwriting runtime helpers for %s",
    name => {
      const edit = parser.computeParameterInsertion("function() {}", name, "string", "r");
      expect(edit.text.split(" <- ")[0]).not.toBe(name);
      expect(parse(edit.text)[0].attribute.attributeName).toBe(name);
    }
  );

  it("does not reuse an existing variable when names sanitize to the same identifier", () => {
    const code = 'my_count <- UiParameter("my count", AttributeType$INT)$value\nmy_count_2 <- 3\nfunction() my_count';
    const edit = parser.computeParameterInsertion(code, "my_count", "integer", "r");
    expect(edit.text).toContain('my_count_3 <- UiParameter(name="my_count",');
    expect(parse(edit.text + code).map(parameter => parameter.attribute.attributeName)).toEqual([
      "my_count",
      "my count",
    ]);
  });

  it.each(["function() count", "helper(count = 1); function() count", "function() { count <- count + 1; count }"])(
    "keeps a referenced name available without a top-level binding: %s",
    code => {
      const edit = parser.computeParameterInsertion(code, "count", "integer", "r");
      expect(edit.text).toContain('count <- UiParameter(name="count",');
    }
  );

  it("rejects duplicate, empty, and unsupported additions before editing", () => {
    expect(() => parser.computeParameterInsertion("", " ", "string", "r")).toThrow("name is required");
    expect(() => parser.computeParameterInsertion("", "x", "binary", "r")).toThrow("not supported");
    expect(() => parser.computeParameterInsertion('UiParameter("x", AttributeType$INT)', "x", "integer", "r")).toThrow(
      "declared already"
    );
  });
});

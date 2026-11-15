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
import type { UiUdfLanguage } from "./ui-udf-parameter";

const parser = new UiUdfParametersParserService();
const language = "java" as UiUdfLanguage;
const udf = (body: string) => `public class JavaUDFOpExec extends MapOpExec { ${body} }`;
const row = (attributeName: string, attributeType: string, inputType?: string) => ({
  attribute: { attributeName, attributeType },
  value: "",
  ...(inputType ? { inputType } : {}),
});

describe("Java UI parameter declarations", () => {
  it("infers native field types and resource selectors", () => {
    expect(
      parser.parse(
        udf(`
      @UiParameter("count") private int count;
      @UiParameter(value="model", resource="model") private String directory;
      @org.apache.texera.amber.operator.udf.java.UiParameter("when") java.sql.Timestamp when;
    `),
        language
      )
    ).toEqual([row("count", "integer"), row("model", "string", "model"), row("when", "timestamp")]);
  });

  it("ignores comments, strings, local variables, helpers and nested classes", () => {
    expect(
      parser.parse(
        `class Helper { @UiParameter("helper") int helper; }
      ${udf(`// @UiParameter("comment") int comment;
        String example = "@UiParameter(\\\"example\\\") int example;";
        String text = """\n@UiParameter("text") int text;\n""";
        class Nested { @UiParameter("nested") int nested; }
        void method() { @UiParameter("local") int local; }
        @UiParameter("active") Integer active;
      `)}`,
        language
      )
    ).toEqual([row("active", "integer")]);
  });

  it.each([
    '@UiParameter("x") byte[] x;',
    '@UiParameter("x") static int x;',
    '@UiParameter("x") int x = 5;',
    '@UiParameter("x") int x, y;',
    "@UiParameter(value=dynamic) int x;",
    '@UiParameter(value="x", resource="unknown") String x;',
    '@UiParameter(value="x", resource="dataset") int x;',
    '@other.UiParameter("x") int x;',
  ])("ignores unsupported declarations: %s", declaration => {
    expect(parser.parse(udf(declaration), language)).toEqual([]);
  });

  it("decodes Java escapes and trims parameter names", () => {
    expect(parser.parse(udf(String.raw`@UiParameter(" caf\u00e9\n") String value;`), language)).toEqual([
      row("café", "string"),
    ]);
  });

  it("rejects duplicate names", () => {
    expect(() => parser.parse(udf('@UiParameter("x") int a; @UiParameter("x") int b;'), language)).toThrow(
      "declared more than once"
    );
  });

  it("inserts a collision-free field into the UDF class and preserves surrounding code", () => {
    const code = "/* { */ class Helper {}\r\n" + udf("private int count; private int count_2;");
    const edit = parser.computeParameterInsertion(code, "count", "integer", language);
    const changed = code.slice(0, edit.offset) + edit.text + code.slice(edit.offset);
    expect(changed).toContain("private int count_3;");
    expect(parser.parse(changed, language)).toEqual([row("count", "integer")]);
    expect(changed.replace(edit.text, "")).toBe(code);
  });

  it.each(["class", "UiParameter", "1 count", 'na"me', "你好"])("inserts a valid field for %s", name => {
    const code = udf("");
    const edit = parser.computeParameterInsertion(code, name, "string", language);
    expect(parser.parse(code.slice(0, edit.offset) + edit.text + code.slice(edit.offset), language)).toEqual([
      row(name, "string"),
    ]);
  });

  it("keeps the java package available to generated field initializers", () => {
    const edit = parser.computeParameterInsertion(udf(""), "java", "string", language);
    expect(edit.text).toContain("private String java_;");
  });

  it("reports missing and ambiguous UDF classes", () => {
    expect(() => parser.computeParameterInsertion("class Other {}", "x", "integer", language)).toThrow("JavaUDFOpExec");
    expect(() => parser.parse(udf("") + udf(""), language)).toThrow("Only one");
  });
});

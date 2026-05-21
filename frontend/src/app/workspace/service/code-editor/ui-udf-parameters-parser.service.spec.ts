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

import {
  UiUdfParametersParseError,
  UiUdfParametersParserService,
  type UiUdfParameter,
} from "./ui-udf-parameters-parser.service";

const MULTIPLE_SUPPORTED_CLASSES_ERROR = "Only one Python UDF class can declare UiParameter values.";
const DUPLICATE_NAME_ERROR = "UiParameter name 'threshold' is declared more than once.";

describe("UiUdfParametersParserService", () => {
  let service: UiUdfParametersParserService;

  beforeEach(() => {
    service = new UiUdfParametersParserService();
  });

  it("should parse supported positional, named, and attr_type arguments", () => {
    expectParsed(
      service,
      `
        self.UiParameter("count", AttributeType.INT)
        self.UiParameter(type=AttributeType.STRING, name="name")
        self.UiParameter(name="age", type=AttributeType.LONG)
        self.UiParameter("score", AttributeType.DOUBLE)
        self.UiParameter("enabled", AttributeType.BOOL)
        self.UiParameter("created_at", type=AttributeType.TIMESTAMP)
        self.UiParameter("alias", attr_type=AttributeType.INTEGER)
      `,
      [
        parameter("count", "integer"),
        parameter("name", "string"),
        parameter("age", "long"),
        parameter("score", "double"),
        parameter("enabled", "boolean"),
        parameter("created_at", "timestamp"),
        parameter("alias", "integer"),
      ]
    );
  });

  it("should parse multiline UiParameter calls with split arguments", () => {
    expectParsed(
      service,
      `
        self.UiParameter(
            name=
                "threshold",
            type=
                AttributeType.DOUBLE,
        )
        self.UiParameter(
            "label",
            type=
                AttributeType.STRING,
        )
      `,
      [parameter("threshold", "double"), parameter("label", "string")]
    );
  });

  (
    [
      [
        "ignore calls where name or type is missing",
        `
        self.UiParameter(name="a")
        self.UiParameter(type=AttributeType.DOUBLE)
      `,
        [],
      ],
      [
        "ignore invalid positional argument ordering",
        `
        self.UiParameter(AttributeType.INT, "count")
        self.UiParameter(name="valid", type=AttributeType.STRING)
      `,
        [parameter("valid", "string")],
      ],
      ["ignore legacy key= named argument", 'self.UiParameter(type=AttributeType.DOUBLE, key="a")', []],
      [
        "ignore non-self calls and non-AttributeType members",
        `
        some.UiParameter("not_self", AttributeType.INT)
        self.UiParameter("bad_type", OtherType.INT)
        self.UiParameter("valid", AttributeType.STRING)
      `,
        [parameter("valid", "string")],
      ],
      [
        "ignore empty and extra positional arguments",
        `
        self.UiParameter()
        self.UiParameter("too_many", AttributeType.STRING, "extra")
        self.UiParameter("valid", AttributeType.STRING)
      `,
        [parameter("valid", "string")],
      ],
      [
        "ignore commented out UiParameter calls",
        `
        # self.UiParameter("commented", AttributeType.INT)
        self.UiParameter("active", AttributeType.INT)  # self.UiParameter("trailing", AttributeType.STRING)
      `,
        [parameter("active", "integer")],
      ],
      [
        "ignore commented out multiline UiParameter sections",
        `
        # self.UiParameter(
        #     name="commented",
        #     type=AttributeType.INT,
        # )
        self.UiParameter(name="active", type=AttributeType.STRING)
      `,
        [parameter("active", "string")],
      ],
      [
        "ignore UiParameter examples inside triple-quoted strings",
        `
        """
        self.UiParameter("example", AttributeType.INT)
        """
        self.UiParameter("active", AttributeType.DOUBLE)
      `,
        [parameter("active", "double")],
      ],
      [
        "reject binary UiParameter types",
        `
        self.UiParameter("payload", AttributeType.BINARY)
        self.UiParameter("blob", AttributeType.LARGE_BINARY)
      `,
        [],
      ],
    ] as ReadonlyArray<readonly [string, string, UiUdfParameter[]]>
  ).forEach(([description, openBody, expectedParameters]) => {
    it(`should ${description}`, () => {
      expectParsed(service, openBody, expectedParameters);
    });
  });

  it("should ignore unsupported classes and custom-named subclasses", () => {
    const code = [
      pythonClass('self.UiParameter(type=AttributeType.DOUBLE, name="a")', "RandomClass", "ABC"),
      pythonClass('self.UiParameter("threshold", AttributeType.DOUBLE)', "MyTupleOp"),
      pythonClass('self.UiParameter("label", AttributeType.STRING)', "MyWrappedTupleOp", "ProcessTupleOperator"),
    ].join("\n");

    expect(service.parse(code)).toEqual([]);
  });

  it("should parse supported UiParameter calls when unsupported classes are present", () => {
    const code = [
      pythonClass('self.UiParameter("threshold", AttributeType.DOUBLE)'),
      pythonClass('self.UiParameter("ignored", AttributeType.STRING)', "RandomClass", "ABC"),
    ].join("\n");

    expect(service.parse(code)).toEqual([parameter("threshold", "double")]);
  });

  [
    {
      description: "multiple supported UDF classes",
      code: [
        pythonClass('self.UiParameter("threshold", AttributeType.DOUBLE)', "ProcessTupleOperator"),
        pythonClass('self.UiParameter(name="batch_size", type=AttributeType.INT)', "GenerateOperator"),
      ].join("\n"),
      message: MULTIPLE_SUPPORTED_CLASSES_ERROR,
    },
    {
      description: "duplicate parameter names",
      code: pythonClass(`
        self.UiParameter("threshold", AttributeType.DOUBLE)
        self.UiParameter("threshold", AttributeType.STRING)
        self.UiParameter("label", AttributeType.STRING)
      `),
      message: DUPLICATE_NAME_ERROR,
    },
  ].forEach(({ description, code, message }) => {
    it(`should raise an error for ${description}`, () => {
      expectParseError(service, code, message);
    });
  });
});

function expectParsed(
  service: UiUdfParametersParserService,
  openBody: string,
  expectedParameters: UiUdfParameter[]
): void {
  expect(service.parse(pythonClass(openBody))).toEqual(expectedParameters);
}

function expectParseError(service: UiUdfParametersParserService, code: string, message: string): void {
  expect(() => service.parse(code)).toThrow(UiUdfParametersParseError);
  expect(() => service.parse(code)).toThrow(message);
}

function pythonClass(openBody: string, className = "ProcessTupleOperator", baseClass = "UDFOperatorV2"): string {
  const openStatements = openBody
    .trim()
    .split("\n")
    .map(line => `        ${line.trim()}`)
    .join("\n");

  return `
    class ${className}(${baseClass}):
        def open(self):
${openStatements}
  `;
}

function parameter(attributeName: string, attributeType: UiUdfParameter["attribute"]["attributeType"]): UiUdfParameter {
  return { attribute: { attributeName, attributeType }, value: "" };
}

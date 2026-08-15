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
  UiUdfParametersEditError,
  UiUdfParametersParseError,
  UiUdfParametersParserService,
  type UiUdfParameter,
} from "./ui-udf-parameters-parser.service";
import type { AttributeType } from "../../types/workflow-compiling.interface";

const MULTIPLE_SUPPORTED_CLASSES_ERROR = "Only one Python UDF class can declare UiParameter values.";
const DUPLICATE_NAME_ERROR = "UiParameter name 'threshold' is declared more than once.";
const PASS_ONLY_OPEN = "class ProcessTupleOperator(UDFOperatorV2):\n    def open(self):\n        pass\n";

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

describe("UiUdfParametersParserService.computeParameterInsertion", () => {
  let service: UiUdfParametersParserService;

  beforeEach(() => {
    service = new UiUdfParametersParserService();
  });

  // Each case is the expected file, with the inserted lines marked ">": the input is the same
  // file without them. Every case also re-parses the result to prove the round trip.
  (
    [
      [
        "insert before existing UiParameter declarations",
        "b",
        "double",
        [
          "class ProcessTupleOperator(UDFOperatorV2):",
          "    def open(self):",
          '>        self.b = self.UiParameter(name="b", type=AttributeType.DOUBLE).value',
          '        self.a = self.UiParameter(name="a", type=AttributeType.INT).value',
          "        self.other = 1",
        ],
      ],
      [
        "insert after an open() docstring and before executable statements",
        "b",
        "integer",
        [
          "class ProcessTupleOperator(UDFOperatorV2):",
          "    def open(self):",
          '        """Load resources."""',
          '>        self.b = self.UiParameter(name="b", type=AttributeType.INT).value',
          "        self.other = 1",
        ],
      ],
      [
        "insert before a header-based UiParameter and its following compound statement",
        "limit",
        "string",
        [
          "class ProcessTupleOperator(UDFOperatorV2):",
          "    def open(self):",
          '>        self.limit = self.UiParameter(name="limit", type=AttributeType.STRING).value',
          '        if self.UiParameter(name="debug", type=AttributeType.BOOL).value:',
          "            self.x = 1",
          "        for i in range(3):",
          "            pass",
        ],
      ],
      [
        "create a decorated open() before the first method when the code uses @overrides",
        "b",
        "double",
        [
          "class ProcessTupleOperator(UDFOperatorV2):",
          ">    @overrides",
          ">    def open(self) -> None:",
          '>        self.b = self.UiParameter(name="b", type=AttributeType.DOUBLE).value',
          ">",
          "    @overrides",
          "    def process_tuple(self, tuple_, port):",
          "        yield tuple_",
        ],
      ],
      [
        "create an undecorated open() after a class docstring",
        "b",
        "boolean",
        [
          "class ProcessTupleOperator(UDFOperatorV2):",
          '    """Doc."""',
          ">",
          ">    def open(self) -> None:",
          '>        self.b = self.UiParameter(name="b", type=AttributeType.BOOL).value',
        ],
      ],
      [
        // Exact user flow: only the import and the class header line of the template are
        // uncommented, so the class body holds nothing but comments and lezer's error node.
        "start the class body with open() when the template body is still commented out",
        "carlos",
        "string",
        [
          "from pytexera import *",
          "# ",
          "class ProcessTupleOperator(UDFOperatorV2):",
          ">    def open(self) -> None:",
          '>        self.carlos = self.UiParameter(name="carlos", type=AttributeType.STRING).value',
          "#     ",
          "#     @overrides",
          "#     def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:",
          "#         yield tuple_",
          "# ",
          "# class ProcessBatchOperator(UDFBatchOperator):",
        ],
      ],
      [
        "start the open() body when its statements are still commented out",
        "b",
        "long",
        [
          "class ProcessTupleOperator(UDFOperatorV2):",
          "    def open(self):",
          '>        self.b = self.UiParameter(name="b", type=AttributeType.LONG).value',
          "        # self.a = 1",
          "",
          "    def process_tuple(self, tuple_, port):",
          "        yield tuple_",
        ],
      ],
    ] as ReadonlyArray<readonly [string, string, AttributeType, string[]]>
  ).forEach(([description, name, attributeType, annotatedLines]) => {
    it(`should ${description}`, () => {
      const input = pythonLines(...annotatedLines.filter(line => !line.startsWith(">")));
      const expected = pythonLines(...annotatedLines.map(line => (line.startsWith(">") ? line.slice(1) : line)));

      const updatedCode = insertParameter(service, input, name, attributeType);

      expect(updatedCode).toBe(expected);
      expect(service.parse(updatedCode).map(parsed => parsed.attribute.attributeName)).toContain(name);
    });
  });

  it("should sanitize assignment targets while keeping exact names, and accumulate declarations", () => {
    let code = PASS_ONLY_OPEN;
    code = insertParameter(service, code, "my param-1", "timestamp");
    code = insertParameter(service, code, "class");

    expect(code).toContain('self.my_param_1 = self.UiParameter(name="my param-1", type=AttributeType.TIMESTAMP).value');
    expect(code).toContain('self.class_ = self.UiParameter(name="class", type=AttributeType.DOUBLE).value');
    expect(service.parse(code)).toEqual([
      { attribute: { attributeName: "class", attributeType: "double" }, value: "" },
      { attribute: { attributeName: "my param-1", attributeType: "timestamp" }, value: "" },
    ]);
  });

  (
    [
      [
        "no supported UDF class",
        "class RandomClass(ABC):\n    def open(self):\n        pass",
        "b",
        "double",
        "No supported Python UDF class",
      ],
      [
        "a duplicate parameter name",
        PASS_ONLY_OPEN.replace("pass", 'self.b = self.UiParameter(name="b", type=AttributeType.DOUBLE).value'),
        "b",
        "double",
        "UiParameter name 'b' is declared already.",
      ],
      [
        "a single-line open() body",
        "class ProcessTupleOperator(UDFOperatorV2):\n    def open(self): pass",
        "b",
        "double",
        "need an indented block body",
      ],
      ["an empty parameter name", PASS_ONLY_OPEN, "   ", "double", "UiParameter name is required."],
      ["an unsupported parameter type", PASS_ONLY_OPEN, "b", "binary", "UiParameter type 'binary' is not supported."],
    ] as ReadonlyArray<readonly [string, string, string, AttributeType, string]>
  ).forEach(([description, code, name, attributeType, message]) => {
    it(`should raise an error for ${description}`, () => {
      expect(() => service.computeParameterInsertion(code, name, attributeType)).toThrow(UiUdfParametersEditError);
      expect(() => service.computeParameterInsertion(code, name, attributeType)).toThrow(message);
    });
  });
});

function insertParameter(
  service: UiUdfParametersParserService,
  code: string,
  name: string,
  attributeType: AttributeType = "double"
): string {
  const edit = service.computeParameterInsertion(code, name, attributeType);
  return code.slice(0, edit.offset) + edit.text + code.slice(edit.offset);
}

function pythonLines(...lines: string[]): string {
  return `${lines.join("\n")}\n`;
}

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

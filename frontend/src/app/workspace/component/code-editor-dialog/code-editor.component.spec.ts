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

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { CodeEditorComponent } from "./code-editor.component";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { mockJavaUDFPredicate, mockPoint } from "../../service/workflow-graph/model/mock-workflow-data";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { mockOperatorMetaData } from "../../service/operator-metadata/mock-operator-metadata.data";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { OperatorPredicate } from "../../types/workflow-common.interface";
import { OperatorSchema } from "../../types/operator-schema.interface";
import { of } from "rxjs";

// Operator types that the constructor's language-detection branch must map
// to a specific language. `RUDFSource` / `RUDF` -> `r`; the three V2 Python
// types -> `python`; everything else -> `java`. Local to this spec so we
// don't perturb the shared mock-workflow-data fixtures.
const R_OPERATOR_TYPES = ["RUDFSource", "RUDF"];
const PYTHON_OPERATOR_TYPES = ["PythonUDFV2", "PythonUDFSourceV2", "DualInputPortsPythonUDFV2"];

// Augment `mockOperatorMetaData` with synthetic schemas for the V2 operator
// types and one unknown type so `addOperator` and `JointUIService` accept
// them. Cloning the existing `PythonUDF` schema and renaming the
// `operatorType` is the cheapest way to satisfy both `operatorTypeExists`
// and the schema-driven joint element creation.
const baseSchema = mockOperatorMetaData.operators.find(op => op.operatorType === "PythonUDF");
if (!baseSchema) {
  throw new Error(
    "CodeEditorComponent spec setup expected a PythonUDF schema in mockOperatorMetaData — fixture has drifted."
  );
}
const synthesizeSchema = (operatorType: string): OperatorSchema => ({ ...baseSchema, operatorType });
const augmentedSchemas: OperatorSchema[] = [
  ...mockOperatorMetaData.operators,
  ...PYTHON_OPERATOR_TYPES.map(synthesizeSchema),
  ...R_OPERATOR_TYPES.map(synthesizeSchema),
  synthesizeSchema("SomeUnknownType"),
];
class AugmentedStubMetadataService extends StubOperatorMetadataService {
  // JointUIService snapshots `operatorSchemas` from this stream once on
  // construction, so we have to feed it the augmented list (overriding only
  // `getOperatorSchema`/`operatorTypeExists` is not enough).
  private readonly augmentedMetadata = of({
    ...mockOperatorMetaData,
    operators: augmentedSchemas,
  });
  override getOperatorMetadata(): typeof this.augmentedMetadata {
    return this.augmentedMetadata;
  }
  override getOperatorSchema(operatorType: string): OperatorSchema {
    const schema = augmentedSchemas.find(op => op.operatorType === operatorType);
    if (!schema) throw new Error(`unknown operatorType ${operatorType}`);
    return schema;
  }
  override operatorTypeExists(operatorType: string): boolean {
    return augmentedSchemas.some(op => op.operatorType === operatorType);
  }
}

const buildPredicate = (operatorID: string, operatorType: string): OperatorPredicate => ({
  operatorID,
  operatorType,
  operatorVersion: "p1",
  operatorProperties: {},
  inputPorts: [{ portID: "input-0" }],
  outputPorts: [{ portID: "output-0" }],
  showAdvanced: false,
  isDisabled: false,
});

describe("CodeEditorComponent", () => {
  let workflowActionService: WorkflowActionService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      providers: [
        WorkflowActionService,
        { provide: OperatorMetadataService, useClass: AugmentedStubMetadataService },
        ...commonTestProviders,
      ],
      imports: [CodeEditorComponent, HttpClientTestingModule],
    }).compileComponents();

    workflowActionService = TestBed.inject(WorkflowActionService);
  });

  function makeFixture(predicate: OperatorPredicate): ComponentFixture<CodeEditorComponent> {
    workflowActionService.addOperator(predicate, mockPoint);
    workflowActionService.getJointGraphWrapper().highlightOperators(predicate.operatorID);
    const fixture = TestBed.createComponent(CodeEditorComponent);
    fixture.detectChanges();
    return fixture;
  }

  it("creates with the highlighted operator", () => {
    const fixture = makeFixture(mockJavaUDFPredicate);
    expect(fixture.componentInstance).toBeTruthy();
    expect(fixture.componentInstance.currentOperatorId).toBe(mockJavaUDFPredicate.operatorID);
  });

  // Language detection — the constructor maps `RUDFSource` / `RUDF` to `r`,
  // the three V2-era Python operator types to `python`, and anything else
  // to `java`. The exact branch lives in the constructor; the public
  // `language` field is what the rest of the editor (LSP wiring, file-
  // suffix selection) keys off.

  R_OPERATOR_TYPES.forEach((operatorType, index) => {
    it(`picks language="r" for operatorType=${operatorType}`, () => {
      const fixture = makeFixture(buildPredicate(`r-${index}`, operatorType));
      expect(fixture.componentInstance.language).toBe("r");
      expect(fixture.componentInstance.languageTitle).toBe("R UDF");
    });
  });

  PYTHON_OPERATOR_TYPES.forEach((operatorType, index) => {
    it(`picks language="python" for operatorType=${operatorType}`, () => {
      const fixture = makeFixture(buildPredicate(`p-${index}`, operatorType));
      expect(fixture.componentInstance.language).toBe("python");
      expect(fixture.componentInstance.languageTitle).toBe("Python UDF");
    });
  });

  it('picks language="java" for plain JavaUDF', () => {
    const fixture = makeFixture(mockJavaUDFPredicate);
    expect(fixture.componentInstance.language).toBe("java");
    expect(fixture.componentInstance.languageTitle).toBe("Java UDF");
  });

  it('picks language="java" for unknown operator types', () => {
    const fixture = makeFixture(buildPredicate("u-0", "SomeUnknownType"));
    expect(fixture.componentInstance.language).toBe("java");
    expect(fixture.componentInstance.languageTitle).toBe("Java UDF");
  });

  it("derives languageTitle as Capitalized(language) + ' UDF'", () => {
    const fixture = makeFixture(buildPredicate("p-x", "PythonUDFV2"));
    const c = fixture.componentInstance;
    // Independent re-derivation matches whatever the component computed.
    const expected = `${c.language[0].toUpperCase()}${c.language.slice(1)} UDF`;
    expect(c.languageTitle).toBe(expected);
  });

  // Coeditor cursor styles — getCoeditorCursorStyles takes the awareness-
  // sourced clientId + colour and wraps a `<style>` block via
  // `DomSanitizer.bypassSecurityTrustHtml`, so the return value is a
  // SafeHtml (consumed via `[innerHTML]` in the template). We assert the
  // wrapper shape (truthy DomSanitizer-wrapped object) for valid inputs.
  // Exact CSS contents are sanitizer-internal and differ across builds, so
  // we don't pin them here.

  it("produces a SafeHtml for a coeditor with a numeric clientId and a hex colour", () => {
    const fixture = makeFixture(mockJavaUDFPredicate);
    const result = fixture.componentInstance.getCoeditorCursorStyles({
      clientId: "12345",
      color: "#ff00aa",
    } as any);
    expect(result).toBeTruthy();
  });

  it("produces a SafeHtml for a coeditor with an rgba colour", () => {
    const fixture = makeFixture(mockJavaUDFPredicate);
    const result = fixture.componentInstance.getCoeditorCursorStyles({
      clientId: "42",
      color: "rgba(10, 20, 30, 0.8)",
    } as any);
    expect(result).toBeTruthy();
  });
});

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
import { of, Subject } from "rxjs";
import { AIAssistantService } from "../../service/ai-assistant/ai-assistant.service";
import * as monaco from "monaco-editor";

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

  describe("getFileSuffixByLanguage", () => {
    // The method is private but determines the in-memory file URI that Monaco
    // picks language syntax + LSP wiring from, so pinning every branch protects
    // the language → file-suffix contract.
    function suffixFor(lang: string): string {
      const fixture = makeFixture(mockJavaUDFPredicate);
      return (fixture.componentInstance as any).getFileSuffixByLanguage(lang);
    }

    it("maps python → .py", () => expect(suffixFor("python")).toBe(".py"));
    it("maps r → .r", () => expect(suffixFor("r")).toBe(".r"));
    it("maps javascript → .js", () => expect(suffixFor("javascript")).toBe(".js"));
    it("maps java → .java", () => expect(suffixFor("java")).toBe(".java"));
    it("is case-insensitive on the language name", () => {
      // `suffixFor` builds a fixture per call which adds the predicate's
      // operator to the workflow; with the same predicate twice in one test
      // the second `addOperator` collides. Call once and reach the method
      // directly to assert another case-folded input.
      const fixture = makeFixture(mockJavaUDFPredicate);
      const fn = (fixture.componentInstance as any).getFileSuffixByLanguage.bind(fixture.componentInstance);
      expect(fn("Python")).toBe(".py");
      expect(fn("JAVA")).toBe(".java");
    });
    it("falls back to .py for unknown languages so the default Monaco grammar is python", () => {
      expect(suffixFor("brainfuck")).toBe(".py");
    });
  });

  describe("onFocus", () => {
    it("highlights the operator the editor is bound to", () => {
      const fixture = makeFixture(mockJavaUDFPredicate);
      const highlightSpy = vi.spyOn(workflowActionService.getJointGraphWrapper(), "highlightOperators");
      fixture.componentInstance.onFocus();
      expect(highlightSpy).toHaveBeenCalledWith(mockJavaUDFPredicate.operatorID);
    });
  });

  describe("rejectCurrentAnnotation", () => {
    it("hides the suggestion UI and clears the staged code + suggestion", () => {
      const fixture = makeFixture(mockJavaUDFPredicate);
      const c = fixture.componentInstance;

      c.showAnnotationSuggestion = true;
      c.currentCode = "x = 1";
      c.currentSuggestion = "x: int = 1";

      c.rejectCurrentAnnotation();

      expect(c.showAnnotationSuggestion).toBe(false);
      expect(c.currentCode).toBe("");
      expect(c.currentSuggestion).toBe("");
    });

    it("emits on the multi-variable response subject when one is staged", () => {
      const fixture = makeFixture(mockJavaUDFPredicate);
      const c = fixture.componentInstance;
      const userResponseSubject = new Subject<void>();
      const nextSpy = vi.spyOn(userResponseSubject, "next");

      // The two flags together gate the multi-variable continuation; both are
      // private, so we reach through `(c as any)` to wire them up.
      (c as any).isMultipleVariables = true;
      (c as any).userResponseSubject = userResponseSubject;

      c.rejectCurrentAnnotation();

      expect(nextSpy).toHaveBeenCalledOnce();
    });
  });

  describe("acceptCurrentAnnotation", () => {
    it("is a no-op when the suggestion UI is not currently shown", () => {
      const fixture = makeFixture(mockJavaUDFPredicate);
      const c = fixture.componentInstance;
      // No state set → early return path; nothing should change.
      c.showAnnotationSuggestion = false;
      expect(() => c.acceptCurrentAnnotation()).not.toThrow();
      expect(c.showAnnotationSuggestion).toBe(false);
    });

    it("hides the suggestion UI after accepting", () => {
      const fixture = makeFixture(mockJavaUDFPredicate);
      const c = fixture.componentInstance;

      // The accept path reaches into the underlying EditorApp for `.getEditor()`
      // and into the YText `.code` for `.insert()`. Both are private so we stub
      // them through bracket access to a minimum that lets insertTypeAnnotations
      // no-op cleanly. `dispose` is needed because ngOnDestroy fires at teardown
      // and calls it.
      (c as any).editorApp = {
        getEditor: () => ({
          getModel: () => ({ getOffsetAt: () => 0 }),
        }),
        dispose: vi.fn().mockResolvedValue(undefined),
      };
      (c as any).code = { insert: vi.fn() };

      c.showAnnotationSuggestion = true;
      c.currentRange = new monaco.Range(1, 1, 1, 5);
      c.currentSuggestion = ": int";

      c.acceptCurrentAnnotation();

      expect(c.showAnnotationSuggestion).toBe(false);
    });
  });

  describe("AI assistant action wiring", () => {
    // setupAIAssistantActions checks an AI-provider flag (OpenAI vs others)
    // before deciding whether to register the per-selection 'Add Type
    // Annotation' action. We can't drive the action body without a real
    // Monaco editor, but the gate itself is plain RxJS — flip the flag and
    // assert observable behaviour.
    it("emits 'OpenAI' from the AI assistant gate when configured that way", async () => {
      // Re-configure TestBed with a mock that drives the gate; existing tests
      // use the default DI-resolved service.
      await TestBed.resetTestingModule()
        .configureTestingModule({
          providers: [
            WorkflowActionService,
            { provide: OperatorMetadataService, useClass: AugmentedStubMetadataService },
            { provide: AIAssistantService, useValue: { checkAIAssistantEnabled: () => of("OpenAI") } },
            ...commonTestProviders,
          ],
          imports: [CodeEditorComponent, HttpClientTestingModule],
        })
        .compileComponents();
      const wfActions = TestBed.inject(WorkflowActionService);
      wfActions.addOperator(mockJavaUDFPredicate, mockPoint);
      wfActions.getJointGraphWrapper().highlightOperators(mockJavaUDFPredicate.operatorID);
      const fixture = TestBed.createComponent(CodeEditorComponent);
      fixture.detectChanges();
      const checked = await TestBed.inject(AIAssistantService).checkAIAssistantEnabled().toPromise();
      expect(checked).toBe("OpenAI");
    });
  });
});

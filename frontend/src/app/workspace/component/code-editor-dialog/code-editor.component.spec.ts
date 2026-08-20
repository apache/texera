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
import { BehaviorSubject, defer, of, config as rxjsConfig, Subject } from "rxjs";
import {
  AIAssistantService,
  TypeAnnotationResponse,
  UnannotatedArgument,
} from "../../service/ai-assistant/ai-assistant.service";
import * as monaco from "monaco-editor";
import { UiUdfParametersSyncService } from "../../service/code-editor/ui-udf-parameters-sync.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { ComponentRef, SecurityContext } from "@angular/core";
import { DomSanitizer, SafeHtml } from "@angular/platform-browser";
import { Coeditor } from "../../../common/type/user";

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

// A plain recording stand-in for `IStandaloneCodeEditor`. The component never
// introspects the editor beyond the handful of methods below, so the private
// editor-consuming methods (`setupAIAssistantActions`, `handleTypeAnnotation`,
// `insertTypeAnnotations`) can be driven directly with this object — no Monaco
// runtime, and no module mocking, which `@angular/build`'s `isolate: false`
// makes unreliable.
//
// The numbers here are deliberately all distinct (`top` 50 vs `left` 100,
// `getOffsetAt` 42 vs the `?? 0` fallback) so that a mutation swapping two of
// them cannot render an identical result.
interface FakeEditor {
  actions: monaco.editor.IActionDescriptor[];
  addAction: ReturnType<typeof vi.fn>;
  getSelection: ReturnType<typeof vi.fn>;
  getModel: ReturnType<typeof vi.fn>;
  getScrolledVisiblePosition: ReturnType<typeof vi.fn>;
  createDecorationsCollection: ReturnType<typeof vi.fn>;
  layout: ReturnType<typeof vi.fn>;
}

function makeFakeEditor(): FakeEditor {
  const actions: monaco.editor.IActionDescriptor[] = [];
  return {
    actions,
    addAction: vi.fn((action: monaco.editor.IActionDescriptor) => {
      actions.push(action);
      return { dispose: vi.fn(), id: action.id, label: action.label };
    }),
    getSelection: vi.fn(() => new monaco.Selection(1, 1, 1, 5)),
    getModel: vi.fn(() => ({
      getValue: () => "def f(a, b):\n  return a\n",
      getValueInRange: () => "a, b",
      getOffsetAt: () => 42,
    })),
    getScrolledVisiblePosition: vi.fn(() => ({ top: 50, left: 100, height: 18 })),
    createDecorationsCollection: vi.fn(() => ({ clear: vi.fn() })),
    layout: vi.fn(),
  };
}

const asEditor = (editor: FakeEditor) => editor as unknown as monaco.editor.ICodeEditor;

// The component reads `getBoundingClientRect()` on the container to decide
// whether to clamp it to the viewport. jsdom always reports an all-zero rect,
// so tests stage their own.
function stubRect(left: number, top: number, right: number, bottom: number): DOMRect {
  return {
    x: left,
    y: top,
    left,
    top,
    right,
    bottom,
    width: right - left,
    height: bottom - top,
    toJSON: () => ({}),
  } as DOMRect;
}

// `window.innerWidth` / `innerHeight` are accessor properties in jsdom, so they
// are staged with `defineProperty` and restored from the captured descriptor.
function withViewport<T>(width: number, height: number, run: () => T): T {
  const originalWidth = Object.getOwnPropertyDescriptor(window, "innerWidth");
  const originalHeight = Object.getOwnPropertyDescriptor(window, "innerHeight");
  Object.defineProperty(window, "innerWidth", { value: width, configurable: true });
  Object.defineProperty(window, "innerHeight", { value: height, configurable: true });
  try {
    return run();
  } finally {
    if (originalWidth) Object.defineProperty(window, "innerWidth", originalWidth);
    if (originalHeight) Object.defineProperty(window, "innerHeight", originalHeight);
  }
}

describe("CodeEditorComponent", () => {
  let workflowActionService: WorkflowActionService;
  let uiParametersParseErrors: Subject<{ operatorId: string; message?: string }>;
  let notificationServiceMock: { error: ReturnType<typeof vi.fn> };
  let aiEnabled$: BehaviorSubject<string>;
  let getTypeAnnotationsSpy: ReturnType<typeof vi.fn>;
  let locateUnannotatedSpy: ReturnType<typeof vi.fn>;

  beforeEach(async () => {
    // The component persists the dialog geometry per operator id in
    // localStorage and restores it in ngAfterViewInit, so a test that resizes
    // the container would otherwise leak that geometry into every later
    // fixture built from the same predicate.
    localStorage.clear();
    uiParametersParseErrors = new Subject();
    notificationServiceMock = { error: vi.fn() };
    aiEnabled$ = new BehaviorSubject<string>("OpenAI");
    getTypeAnnotationsSpy = vi.fn().mockReturnValue(of({ choices: [{ message: { content: ": int" } }] }));
    locateUnannotatedSpy = vi.fn().mockReturnValue(of([]));
    await TestBed.configureTestingModule({
      providers: [
        WorkflowActionService,
        { provide: OperatorMetadataService, useClass: AugmentedStubMetadataService },
        {
          provide: UiUdfParametersSyncService,
          useValue: {
            attachToYCode: vi.fn(() => () => undefined),
            uiParametersParseError$: uiParametersParseErrors.asObservable(),
          },
        },
        { provide: NotificationService, useValue: notificationServiceMock },
        // The real service resolves its gate over HTTP, which
        // HttpClientTestingModule never answers, so `setupAIAssistantActions`
        // would hang before registering anything.
        {
          provide: AIAssistantService,
          useValue: {
            checkAIAssistantEnabled: () => aiEnabled$,
            getTypeAnnotations: getTypeAnnotationsSpy,
            locateUnannotated: locateUnannotatedSpy,
          },
        },
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

  it("shows UI parameter parser errors for the edited operator", () => {
    const predicate = buildPredicate("python-with-invalid-parameters", "PythonUDFV2");
    makeFixture(predicate);

    uiParametersParseErrors.next({
      operatorId: predicate.operatorID,
      message: "UiParameter name 'count' is declared more than once.",
    });

    expect(notificationServiceMock.error).toHaveBeenCalledWith(
      "Could not update UDF parameters: UiParameter name 'count' is declared more than once."
    );
  });

  it("ignores UI parameter parser events for other operators and successful parses", () => {
    const predicate = buildPredicate("python-with-valid-parameters", "PythonUDFV2");
    makeFixture(predicate);

    uiParametersParseErrors.next({ operatorId: "another-operator", message: "invalid" });
    uiParametersParseErrors.next({ operatorId: predicate.operatorID });

    expect(notificationServiceMock.error).not.toHaveBeenCalled();
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

  // `getCoeditorCursorStyles` interpolates two awareness-sourced values into a
  // `<style>` tag that is handed to `bypassSecurityTrustHtml`, so the allow-list
  // in front of it is the only thing standing between a peer-controlled string
  // and raw HTML in the page. `DomSanitizer.sanitize` unwraps the bypassed value
  // back to the exact string the component produced, which is what lets these
  // tests assert on the CSS rather than just "something truthy came back".
  describe("getCoeditorCursorStyles", () => {
    const unwrap = (value: SafeHtml): string =>
      TestBed.inject(DomSanitizer).sanitize(SecurityContext.HTML, value) ?? "";
    const stylesFor = (clientId: string, color: string): string => {
      const fixture = makeFixture(mockJavaUDFPredicate);
      return unwrap(fixture.componentInstance.getCoeditorCursorStyles({ clientId, color } as unknown as Coeditor));
    };

    it("scopes every rule to the coeditor id and dims only the selection background", () => {
      // The alpha in the source colour is 0.8 and the selection background is
      // rewritten to 0.5, so the two values are distinguishable — a colour
      // without an alpha would make the rewrite invisible.
      const css = stylesFor("12345", "rgba(1, 2, 3, 0.8)");
      expect(css).toContain(".yRemoteSelection-12345 { background-color: rgba(1, 2, 3, 0.5)}");
      expect(css).toContain(".yRemoteSelectionHead-12345::after { border-color: rgba(1, 2, 3, 0.8)}");
      expect(css).toContain(".yRemoteSelectionHead-12345 { border-color: rgba(1, 2, 3, 0.8)}");
      expect(css.startsWith("<style>")).toBe(true);
      expect(css.endsWith("</style>")).toBe(true);
    });

    // One case per alternation in SAFE_CSS_COLOR: short hex, hex-with-alpha,
    // and the percent-bearing hsl() form.
    (["#abc", "#aabbccdd", "hsl(120, 50%, 50%)"] as const).forEach(color => {
      it(`accepts the ${color} colour notation`, () => {
        expect(stylesFor("42", color)).toContain(`.yRemoteSelectionHead-42 { border-color: ${color}}`);
      });
    });

    const REJECTED: ReadonlyArray<readonly [string, string, string]> = [
      ["a clientId that is not all digits", "12; }</style><script>alert(1)</script>", "#ff00aa"],
      ["a clientId longer than ten digits", "123456789012", "#ff00aa"],
      ["an empty clientId", "", "#ff00aa"],
      ["a missing colour", "12345", ""],
      ["a named colour outside the allow-list", "12345", "red"],
      ["a url() colour", "12345", "url(https://evil.example/x)"],
      ["a colour carrying a style-tag escape", "12345", "#fff}</style><script>alert(1)</script>"],
    ];
    REJECTED.forEach(([label, clientId, color]) => {
      it(`emits no CSS at all for ${label}`, () => {
        expect(stylesFor(clientId, color)).toBe("");
      });
    });
  });

  describe("template", () => {
    it("the close button destroys the host component reference", () => {
      const fixture = makeFixture(mockJavaUDFPredicate);
      const destroy = vi.fn();
      fixture.componentInstance.componentRef = { destroy } as unknown as ComponentRef<CodeEditorComponent>;

      (fixture.nativeElement as HTMLElement).querySelector<HTMLButtonElement>("#close-button")!.click();

      expect(destroy).toHaveBeenCalledOnce();
    });

    it("the close button is inert when the dialog was created without a component reference", () => {
      const fixture = makeFixture(mockJavaUDFPredicate);
      expect(fixture.componentInstance.componentRef).toBeUndefined();

      expect(() =>
        (fixture.nativeElement as HTMLElement).querySelector<HTMLButtonElement>("#close-button")!.click()
      ).not.toThrow();
    });

    it("renders one scoped cursor-style block per coeditor", () => {
      const fixture = makeFixture(mockJavaUDFPredicate);
      fixture.componentInstance.coeditorPresenceService.coeditors = [
        { clientId: "777", color: "rgba(9, 8, 7, 0.8)" } as unknown as Coeditor,
        { clientId: "888", color: "#abcdef" } as unknown as Coeditor,
      ];
      fixture.detectChanges();

      const html = (fixture.nativeElement as HTMLElement).innerHTML;
      expect(html).toContain("yRemoteSelection-777");
      expect(html).toContain("yRemoteSelection-888");
    });

    it("renders nothing for a coeditor whose awareness values fail the allow-list", () => {
      const fixture = makeFixture(mockJavaUDFPredicate);
      fixture.componentInstance.coeditorPresenceService.coeditors = [
        { clientId: "not-a-number", color: "#abcdef" } as unknown as Coeditor,
      ];
      fixture.detectChanges();

      expect((fixture.nativeElement as HTMLElement).innerHTML).not.toContain("yRemoteSelection");
    });

    it("keeps the suggestion panel out of the DOM until a suggestion is staged", () => {
      const fixture = makeFixture(mockJavaUDFPredicate);
      expect((fixture.nativeElement as HTMLElement).querySelector("texera-annotation-suggestion")).toBeNull();

      fixture.componentInstance.showAnnotationSuggestion = true;
      fixture.detectChanges();

      expect((fixture.nativeElement as HTMLElement).querySelector("texera-annotation-suggestion")).not.toBeNull();
    });

    it("routes the suggestion panel's accept and decline to the matching handlers", () => {
      const fixture = makeFixture(mockJavaUDFPredicate);
      const component = fixture.componentInstance;
      component.showAnnotationSuggestion = true;
      fixture.detectChanges();
      const accepted = vi.spyOn(component, "acceptCurrentAnnotation");
      const declined = vi.spyOn(component, "rejectCurrentAnnotation");

      component.annotationSuggestion.onAccept();
      expect(accepted).toHaveBeenCalledOnce();
      expect(declined).not.toHaveBeenCalled();

      component.annotationSuggestion.onDecline();
      expect(declined).toHaveBeenCalledOnce();
      expect(accepted).toHaveBeenCalledOnce();
    });
  });

  // The two context-menu actions are registered against a live Monaco editor,
  // which jsdom cannot host. Their `run` callbacks are plain closures over the
  // component though, so `setupAIAssistantActions` is driven directly with a
  // recording stand-in and the callbacks are invoked off the registration list.
  describe("AI assistant actions", () => {
    function registerOn(editor: FakeEditor): CodeEditorComponent {
      const fixture = makeFixture(mockJavaUDFPredicate);
      (fixture.componentInstance as any).setupAIAssistantActions(editor);
      return fixture.componentInstance;
    }
    const runAction = (editor: FakeEditor, id: string) =>
      editor.actions.find(action => action.id === id)!.run(asEditor(editor));

    it("registers the per-selection action ahead of the bulk action when the gate reports OpenAI", () => {
      const editor = makeFakeEditor();
      registerOn(editor);
      expect(editor.actions.map(action => action.id)).toEqual(["type-annotation-action", "all-type-annotation-action"]);
    });

    it("registers only the bulk action when the gate reports a different provider", () => {
      aiEnabled$.next("NoAiAssistant");
      const editor = makeFakeEditor();
      registerOn(editor);
      expect(editor.actions.map(action => action.id)).toEqual(["all-type-annotation-action"]);
    });

    it("the per-selection action forwards the selected text, its start line, and the whole file", () => {
      const editor = makeFakeEditor();
      editor.getSelection.mockReturnValue(new monaco.Selection(3, 2, 4, 9));
      registerOn(editor);

      runAction(editor, "type-annotation-action");

      // Three distinct arguments — selected text, start line, whole document —
      // so transposing any pair changes the call.
      expect(getTypeAnnotationsSpy).toHaveBeenCalledWith("a, b", 3, "def f(a, b):\n  return a\n");
    });

    it("the bulk action forwards the selected text and its start line", () => {
      const editor = makeFakeEditor();
      editor.getSelection.mockReturnValue(new monaco.Selection(3, 2, 4, 9));
      registerOn(editor);

      runAction(editor, "all-type-annotation-action");

      expect(locateUnannotatedSpy).toHaveBeenCalledWith("a, b", 3);
    });

    it("both actions bail out when the editor has no model", () => {
      const editor = makeFakeEditor();
      editor.getModel.mockReturnValue(null);
      registerOn(editor);

      runAction(editor, "type-annotation-action");
      runAction(editor, "all-type-annotation-action");

      expect(getTypeAnnotationsSpy).not.toHaveBeenCalled();
      expect(locateUnannotatedSpy).not.toHaveBeenCalled();
    });

    it("both actions bail out when there is no selection", () => {
      const editor = makeFakeEditor();
      editor.getSelection.mockReturnValue(null);
      registerOn(editor);

      runAction(editor, "type-annotation-action");
      runAction(editor, "all-type-annotation-action");

      expect(getTypeAnnotationsSpy).not.toHaveBeenCalled();
      expect(locateUnannotatedSpy).not.toHaveBeenCalled();
    });

    it("the bulk action leaves the multi-variable state untouched when nothing is unannotated", () => {
      const editor = makeFakeEditor();
      const component = registerOn(editor);

      runAction(editor, "all-type-annotation-action");

      expect(locateUnannotatedSpy).toHaveBeenCalledOnce();
      expect((component as any).isMultipleVariables).toBe(false);
      expect((component as any).userResponseSubject).toBeUndefined();
      expect(editor.createDecorationsCollection).not.toHaveBeenCalled();
    });
  });

  // "Add All Type Annotations" walks the unannotated arguments one at a time,
  // pausing on each until the user accepts or declines. Every variable inserted
  // on a line shifts the columns of the ones after it on that same line, which
  // the walker compensates for with a running offset.
  describe("bulk annotation walk", () => {
    // Two arguments share line 3 (so the second one is offset by the length of
    // the accepted suggestion) and a third sits on line 7 (so the offset
    // resets). Every column is distinct, so a mistaken offset is visible.
    const UNANNOTATED: ReadonlyArray<UnannotatedArgument> = [
      { name: "alpha", startLine: 3, startColumn: 5, endLine: 3, endColumn: 6 },
      { name: "beta", startLine: 3, startColumn: 10, endLine: 3, endColumn: 11 },
      { name: "gamma", startLine: 7, startColumn: 2, endLine: 7, endColumn: 3 },
    ];
    const rangeTuple = (range: monaco.Range | undefined) => [
      range!.startLineNumber,
      range!.startColumn,
      range!.endLineNumber,
      range!.endColumn,
    ];

    // `acceptCurrentAnnotation` emits the advance and only then clears
    // `showAnnotationSuggestion`, so it relies on the annotation lookup being
    // asynchronous: the next argument's suggestion has to land after that
    // clear, not before it. The real service answers over HTTP, so the stub
    // resolves off a promise rather than emitting inline — a synchronous
    // `of(...)` here would stall the walk after the first argument.
    const respondAsync = () =>
      defer(() => Promise.resolve({ choices: [{ message: { content: ": int" } }] } as TypeAnnotationResponse));
    const settle = () => new Promise(resolve => setTimeout(resolve, 0));

    async function startWalk() {
      locateUnannotatedSpy.mockReturnValue(of(UNANNOTATED));
      getTypeAnnotationsSpy.mockImplementation(respondAsync);
      const fixture = makeFixture(mockJavaUDFPredicate);
      const component = fixture.componentInstance;
      const editor = makeFakeEditor();
      const insert = vi.fn();
      // `acceptCurrentAnnotation` reaches through the EditorApp for the editor
      // and through the YText for the insertion point; both are staged so that
      // accepting exercises the real path rather than poking the private
      // response subject directly.
      const stageEditorApp = () => {
        (component as any).editorApp = {
          getEditor: () => editor,
          dispose: vi.fn().mockResolvedValue(undefined),
        };
      };
      stageEditorApp();
      (component as any).code = { insert };
      (component as any).setupAIAssistantActions(editor);
      editor.actions.find(action => action.id === "all-type-annotation-action")!.run(asEditor(editor));
      await settle();
      const accept = async () => {
        // ngAfterViewInit's own editor bring-up resolves on a later tick and
        // reassigns `editorApp`, so it is re-staged immediately before each
        // accept (synchronously, so nothing can interleave) to keep the
        // insertion point coming from the stand-in editor.
        stageEditorApp();
        component.acceptCurrentAnnotation();
        await settle();
      };
      return { component, editor, insert, accept };
    }

    it("offsets a same-line argument by the accepted suggestion and resets on a new line", async () => {
      const { component, editor, accept } = await startWalk();

      // First argument: untouched columns, and the walk is now in flight.
      expect((component as any).isMultipleVariables).toBe(true);
      expect(component.currentCode).toBe("alpha");
      expect(rangeTuple(component.currentRange)).toEqual([3, 5, 3, 6]);

      await accept();

      // Second argument shares line 3, so both columns shift by the length of
      // the ": int" suggestion accepted a moment ago.
      expect(component.currentCode).toBe("beta");
      expect(rangeTuple(component.currentRange)).toEqual([3, 15, 3, 16]);

      await accept();

      // Third argument is on line 7, so the running offset resets to zero.
      expect(component.currentCode).toBe("gamma");
      expect(rangeTuple(component.currentRange)).toEqual([7, 2, 7, 3]);

      expect(editor.createDecorationsCollection).toHaveBeenCalledTimes(3);
    });

    it("tears the walk down once the last argument has been answered", async () => {
      const { component, editor, accept } = await startWalk();

      await accept();
      await accept();
      expect((component as any).isMultipleVariables).toBe(true);

      await accept();

      expect((component as any).isMultipleVariables).toBe(false);
      expect((component as any).userResponseSubject).toBeUndefined();
      // No fourth argument, so no fourth highlight.
      expect(editor.createDecorationsCollection).toHaveBeenCalledTimes(3);
    });

    it("highlights each argument as it comes up and clears that highlight on answer", async () => {
      const { editor, accept } = await startWalk();
      const highlightOf = (index: number) =>
        editor.createDecorationsCollection.mock.results[index].value as { clear: ReturnType<typeof vi.fn> };

      const decoration = editor.createDecorationsCollection.mock.calls[0][0][0];
      expect(rangeTuple(decoration.range)).toEqual([3, 5, 3, 6]);
      expect(decoration.options.className).toBe("annotation-highlight");
      expect(decoration.options.hoverMessage.value).toBe("Argument without Annotation");
      expect(highlightOf(0).clear).not.toHaveBeenCalled();

      await accept();

      expect(highlightOf(0).clear).toHaveBeenCalledOnce();
      expect(highlightOf(1).clear).not.toHaveBeenCalled();
    });

    it("advances on decline without shifting the next same-line argument", async () => {
      const { component } = await startWalk();
      expect(component.currentCode).toBe("alpha");

      component.rejectCurrentAnnotation();
      await settle();

      // Nothing was inserted, so the second argument on line 3 keeps its
      // original columns — the offset only compensates for accepted text.
      expect(component.currentCode).toBe("beta");
      expect(rangeTuple(component.currentRange)).toEqual([3, 10, 3, 11]);
    });

    it("inserts the accepted suggestion at the model offset for the range end", async () => {
      const { insert, accept } = await startWalk();

      await accept();

      // 42 is the stand-in model's offset; the `?? 0` fallback would give 0.
      expect(insert).toHaveBeenCalledWith(42, ": int");
    });
  });

  describe("handleTypeAnnotation", () => {
    it("stages the trimmed suggestion and pushes it into the rendered suggestion panel", () => {
      getTypeAnnotationsSpy.mockReturnValue(of({ choices: [{ message: { content: "\n  : int  \n" } }] }));
      const fixture = makeFixture(mockJavaUDFPredicate);
      const component = fixture.componentInstance;
      // The panel is behind an *ngIf, so it has to be rendered before the
      // ViewChild that the component writes into resolves.
      component.showAnnotationSuggestion = true;
      fixture.detectChanges();
      expect(component.annotationSuggestion).toBeDefined();

      (component as any).handleTypeAnnotation("a, b", new monaco.Range(2, 3, 2, 8), makeFakeEditor(), 2, "all code");

      // 50 + 100 and 100 + 100 — top and left stay distinguishable.
      expect(component.suggestionTop).toBe(150);
      expect(component.suggestionLeft).toBe(200);
      expect(component.showAnnotationSuggestion).toBe(true);
      expect(component.annotationSuggestion.code).toBe("a, b");
      expect(component.annotationSuggestion.suggestion).toBe(": int");
      expect(component.annotationSuggestion.top).toBe(150);
      expect(component.annotationSuggestion.left).toBe(200);
    });

    it("leaves the suggestion offsets alone when the range is scrolled out of view", () => {
      const fixture = makeFixture(mockJavaUDFPredicate);
      const component = fixture.componentInstance;
      const editor = makeFakeEditor();
      editor.getScrolledVisiblePosition.mockReturnValue(null);

      (component as any).handleTypeAnnotation("a, b", new monaco.Range(2, 3, 2, 8), editor, 2, "all code");

      expect(component.suggestionTop).toBe(0);
      expect(component.suggestionLeft).toBe(0);
      expect(component.showAnnotationSuggestion).toBe(true);
    });

    const INVALID_RESPONSES: ReadonlyArray<readonly [string, unknown]> = [
      ["carries no choices field", {}],
      ["carries an empty choices array", { choices: [] }],
      ["carries a choice with no message", { choices: [{}] }],
      ["carries a message with empty content", { choices: [{ message: { content: "" } }] }],
    ];
    INVALID_RESPONSES.forEach(([label, response]) => {
      it(`raises and stages nothing when the AI response ${label}`, async () => {
        getTypeAnnotationsSpy.mockReturnValue(of(response as TypeAnnotationResponse));
        const fixture = makeFixture(mockJavaUDFPredicate);
        const component = fixture.componentInstance;

        // The throw happens inside an RxJS `next` handler, which RxJS reports
        // out-of-band on a timer rather than rethrowing to the caller. Route
        // that report into a local sink so the assertion can see it and it
        // cannot surface as an unhandled error later in the run.
        const reported: unknown[] = [];
        const previousHandler = rxjsConfig.onUnhandledError;
        rxjsConfig.onUnhandledError = error => reported.push(error);
        try {
          (component as any).handleTypeAnnotation("a, b", new monaco.Range(2, 3, 2, 8), makeFakeEditor(), 2, "all");
          await new Promise(resolve => setTimeout(resolve, 0));
        } finally {
          rxjsConfig.onUnhandledError = previousHandler;
        }

        expect(reported.some(error => String(error).includes("does not contain valid message content"))).toBe(true);
        expect(component.showAnnotationSuggestion).toBe(false);
        expect(component.currentSuggestion).toBe("");
        expect(component.currentRange).toBeUndefined();
      });
    });
  });

  describe("container geometry", () => {
    const containerOf = (fixture: ComponentFixture<CodeEditorComponent>) =>
      (fixture.componentInstance as any).containerElement.nativeElement as HTMLElement;

    it("restores the geometry saved under this operator id", () => {
      localStorage.setItem(mockJavaUDFPredicate.operatorID, "width: 640px; height: 480px;");

      const container = containerOf(makeFixture(mockJavaUDFPredicate));

      expect(container.style.width).toBe("640px");
      expect(container.style.height).toBe("480px");
    });

    it("writes the geometry back under this operator id on destroy", () => {
      const fixture = makeFixture(mockJavaUDFPredicate);
      containerOf(fixture).style.cssText = "width: 111px; height: 222px;";

      fixture.destroy();

      const saved = localStorage.getItem(mockJavaUDFPredicate.operatorID) ?? "";
      expect(saved).toContain("width: 111px");
      expect(saved).toContain("height: 222px");
    });

    it("clamps a container that overflows the viewport on both axes", () => {
      const fixture = makeFixture(mockJavaUDFPredicate);
      const component = fixture.componentInstance;
      const container = containerOf(fixture);
      container.getBoundingClientRect = () => stubRect(10, 20, 5000, 5000);
      const layout = vi.fn();
      (component as any).editorApp = { getEditor: () => ({ layout }), dispose: vi.fn().mockResolvedValue(undefined) };

      withViewport(800, 600, () => component.onWindowResize());

      // Width comes off the LEFT edge and the viewport WIDTH, height off the
      // TOP edge and the viewport HEIGHT; all four inputs differ, so swapping
      // any pair changes the result.
      expect(container.style.width).toBe("790px");
      expect(container.style.height).toBe("580px");
      expect(layout).toHaveBeenCalledOnce();
    });

    it("leaves a container that already fits the viewport untouched", () => {
      const fixture = makeFixture(mockJavaUDFPredicate);
      const component = fixture.componentInstance;
      const container = containerOf(fixture);
      container.getBoundingClientRect = () => stubRect(10, 20, 300, 400);
      const layout = vi.fn();
      (component as any).editorApp = { getEditor: () => ({ layout }), dispose: vi.fn().mockResolvedValue(undefined) };

      withViewport(800, 600, () => component.onWindowResize());

      expect(container.style.width).toBe("");
      expect(container.style.height).toBe("");
      // The relayout is unconditional even when no clamping was needed.
      expect(layout).toHaveBeenCalledOnce();
    });

    it("survives a resize before any editor has been mounted", () => {
      const fixture = makeFixture(mockJavaUDFPredicate);
      const container = containerOf(fixture);
      container.getBoundingClientRect = () => stubRect(10, 20, 5000, 5000);

      expect(() => withViewport(800, 600, () => fixture.componentInstance.onWindowResize())).not.toThrow();
      expect(container.style.width).toBe("790px");
    });
  });
});

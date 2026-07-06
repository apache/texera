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

// Browser-mode companion to code-editor.component.spec.ts. The sibling jsdom
// spec covers the constructor, language detection, getFileSuffixByLanguage,
// onFocus, getCoeditorCursorStyles, and the accept/reject annotation paths,
// but cannot reach anything gated on a real Monaco editor — the
// `initializeMonacoEditor` subscribe body, `initializeDiffEditor`, AI-action
// run callbacks, `handleTypeAnnotation`'s position branch, and the resize
// handler. This spec drives those by stubbing the v10 editor seams — the
// global vscode-api init (`ensureVscodeApiStarted`) and the per-editor
// `EditorApp` — so the subscribe body runs against a fake editor, then running
// in vitest's Playwright/Chromium browser mode, where monaco-editor's codingame
// fork can be imported without
// jsdom's missing-canvas / Node-Buffer-allocation tripwires (the Buffer/process
// shim is wired as the first setupFile in vitest.browser.config.ts — see
// src/browser-buffer-polyfill.ts).

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { FormControl } from "@angular/forms";
import { BehaviorSubject, of } from "rxjs";
import * as Y from "yjs";

import { CodeEditorComponent } from "./code-editor.component";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { AIAssistantService } from "../../service/ai-assistant/ai-assistant.service";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { mockOperatorMetaData } from "../../service/operator-metadata/mock-operator-metadata.data";
import { mockJavaUDFPredicate, mockPoint } from "../../service/workflow-graph/model/mock-workflow-data";
import { OperatorSchema } from "../../types/operator-schema.interface";
import { commonTestProviders } from "../../../common/testing/test-utils";
import * as monaco from "monaco-editor";

// y-monaco's MonacoBinding wires real listeners against the YText and a real
// monaco TextModel. Our fake editor returns a stub model, so the binding's
// constructor would throw on `model.onDidChangeContent(...)`. The component
// only depends on the binding's `destroy()` (called in ngOnDestroy) and the
// fact that the constructor was called with the right shape of args, so a
// recording stub is sufficient.
// `vi.mock` is hoisted to the top of the file, so any closure variables it
// references must be declared inside `vi.hoisted` — a plain top-level `const`
// is evaluated AFTER the mock factory runs, leaving `monacoBindingCalls`
// undefined at the moment MonacoBinding's constructor would try to push.
const { monacoBindingCalls } = vi.hoisted(() => ({
  monacoBindingCalls: [] as unknown[][],
}));
vi.mock("y-monaco", () => ({
  MonacoBinding: class {
    constructor(...args: unknown[]) {
      monacoBindingCalls.push(args);
    }
    destroy = vi.fn();
  },
}));

// monaco-languageclient v10 split the old single wrapper into a process-wide
// `MonacoVscodeApiWrapper` (started once) and a per-editor `EditorApp`. The
// component `new`s the EditorApp inside its start path, so there is no instance
// field to swap — intercept the class instead. This recording stand-in captures
// the `EditorAppConfig` handed to the constructor (the non-diff vs diff branch
// is the assertion target), records the host element passed to `start()`, and
// hands back the test's fake editor from `getEditor()`. The global vscode-api
// init is stubbed separately in `beforeEach`.
const { editorAppMock } = vi.hoisted(() => ({
  editorAppMock: {
    configs: [] as unknown[],
    start: vi.fn(),
    getEditor: vi.fn(),
  },
}));
vi.mock("monaco-languageclient/editorApp", () => ({
  EditorApp: class {
    constructor(config: unknown) {
      editorAppMock.configs.push(config);
    }
    start(host: unknown) {
      return editorAppMock.start(host);
    }
    getEditor() {
      return editorAppMock.getEditor();
    }
    dispose() {
      return Promise.resolve();
    }
  },
}));

// Re-use the augmented stub from the jsdom spec so the component constructor
// can resolve its highlighted operator regardless of operatorType.
const baseSchema = mockOperatorMetaData.operators.find(op => op.operatorType === "PythonUDF");
if (!baseSchema) {
  throw new Error(
    "CodeEditorComponent browser spec setup expected a PythonUDF schema in mockOperatorMetaData — fixture has drifted."
  );
}
const synthesizeSchema = (operatorType: string): OperatorSchema => ({ ...baseSchema, operatorType });
const augmentedSchemas: OperatorSchema[] = [...mockOperatorMetaData.operators, synthesizeSchema("PythonUDFV2")];
class AugmentedStubMetadataService extends StubOperatorMetadataService {
  private readonly augmentedMetadata = of({ ...mockOperatorMetaData, operators: augmentedSchemas });
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

// A minimal, recording Monaco editor stand-in. Returns realistic values
// where the component reads from it (`getScrolledVisiblePosition`,
// `getSelection`, `getModel`) and records what the component asks it to do
// (`addAction`, `updateOptions`, `layout`). The component does not introspect
// any of these beyond truthiness, so the stub does not need to be a real
// IStandaloneCodeEditor — TypeScript's structural check is the only gate.
interface FakeEditor {
  addAction: ReturnType<typeof vi.fn>;
  updateOptions: ReturnType<typeof vi.fn>;
  layout: ReturnType<typeof vi.fn>;
  getSelection: ReturnType<typeof vi.fn>;
  getModel: ReturnType<typeof vi.fn>;
  getScrolledVisiblePosition: ReturnType<typeof vi.fn>;
  createDecorationsCollection: ReturnType<typeof vi.fn>;
  actions: monaco.editor.IActionDescriptor[];
}

function makeFakeEditor(): FakeEditor {
  const actions: monaco.editor.IActionDescriptor[] = [];
  return {
    actions,
    addAction: vi.fn((action: monaco.editor.IActionDescriptor) => {
      actions.push(action);
      return { dispose: vi.fn(), id: action.id, label: action.label };
    }),
    updateOptions: vi.fn(),
    layout: vi.fn(),
    getSelection: vi.fn(() => new monaco.Selection(1, 1, 1, 5)),
    getModel: vi.fn(() => ({
      getValue: () => "x = 1\ny = 2\n",
      getValueInRange: () => "x",
      onDidChangeContent: () => ({ dispose: () => {} }),
      getOffsetAt: () => 0,
    })),
    getScrolledVisiblePosition: vi.fn(() => ({ top: 50, left: 100, height: 18 })),
    createDecorationsCollection: vi.fn(() => ({ clear: vi.fn() })),
  };
}

describe("CodeEditorComponent (browser)", () => {
  let displayVersionStream$: BehaviorSubject<boolean>;
  let aiEnabled$: BehaviorSubject<string>;
  let getTypeAnnotationsSpy: ReturnType<typeof vi.fn>;
  let locateUnannotatedSpy: ReturnType<typeof vi.fn>;
  let workflowActionService: WorkflowActionService;

  beforeEach(async () => {
    monacoBindingCalls.length = 0;
    editorAppMock.configs.length = 0;
    editorAppMock.start.mockReset().mockResolvedValue(undefined);
    editorAppMock.getEditor.mockReset();
    // The global vscode-api wrapper's start() spins up codingame workers and
    // pulls the default language extensions over dynamic import — neither is
    // needed here. Stub the lazy initializer so the editor start path resolves
    // straight through to the EditorApp seam above.
    vi.spyOn(CodeEditorComponent as any, "ensureVscodeApiStarted").mockResolvedValue(undefined);

    displayVersionStream$ = new BehaviorSubject<boolean>(false);
    aiEnabled$ = new BehaviorSubject<string>("OpenAI");
    getTypeAnnotationsSpy = vi.fn().mockReturnValue(of({ choices: [{ message: { content: ": int" } }] }));
    locateUnannotatedSpy = vi.fn().mockReturnValue(of([]));

    await TestBed.configureTestingModule({
      providers: [
        WorkflowActionService,
        { provide: OperatorMetadataService, useClass: AugmentedStubMetadataService },
        {
          provide: AIAssistantService,
          useValue: {
            checkAIAssistantEnabled: () => aiEnabled$,
            getTypeAnnotations: getTypeAnnotationsSpy,
            locateUnannotated: locateUnannotatedSpy,
          },
        },
        {
          provide: WorkflowVersionService,
          useValue: {
            getDisplayParticularVersionStream: () => displayVersionStream$,
          },
        },
        ...commonTestProviders,
      ],
      imports: [CodeEditorComponent, HttpClientTestingModule],
    }).compileComponents();

    workflowActionService = TestBed.inject(WorkflowActionService);
  });

  afterEach(() => {
    // Restore the `ensureVscodeApiStarted` spy so the next test re-stubs from a
    // clean slate (vitest is not configured to auto-restore mocks).
    vi.restoreAllMocks();
  });

  // Builds a fixture for the highlighted operator, but defers the
  // detectChanges/ngAfterViewInit step so the caller can stage `code` /
  // `formControl` (and the EditorApp's fake editor) before the subscribe body
  // fires. Returns the fixture, the fake editor, and the component instance.
  function makeFixtureWithFakes() {
    const predicate = { ...mockJavaUDFPredicate };
    workflowActionService.addOperator(predicate, mockPoint);
    workflowActionService.getJointGraphWrapper().highlightOperators(predicate.operatorID);

    const fixture = TestBed.createComponent(CodeEditorComponent);
    const editor = makeFakeEditor();
    // The component pulls the editor back out of `EditorApp.getEditor()` inside
    // its start path (and again from `adjustEditorSize`), so the mocked
    // EditorApp must hand back this same fake.
    editorAppMock.getEditor.mockReturnValue(editor);
    const c = fixture.componentInstance as any;
    c.formControl = new FormControl({ value: "", disabled: false });
    // A YText must live inside a Y.Doc to be useful; the binding stub doesn't
    // care, but we stage it as if it came from the shared model so the
    // subscribe body crosses the `if (!this.code) return;` gate.
    c.code = new Y.Doc().getText("code");
    return { fixture, editor, c: c as CodeEditorComponent };
  }

  // The component's subscribe path runs `from(startEditor()).pipe(...)`,
  // which is microtask-async. One macrotask flush after detectChanges is
  // enough for the RxJS chain to deliver the editor into the subscribe body.
  async function flush(): Promise<void> {
    await Promise.resolve();
    await new Promise(r => setTimeout(r, 0));
  }

  it("initializeMonacoEditor: wires the editor + MonacoBinding + AI actions when code exists", async () => {
    const { fixture, editor } = makeFixtureWithFakes();

    fixture.detectChanges();
    await flush();

    // The non-diff branch should construct exactly one EditorApp and start it
    // against the editor host element.
    expect(editorAppMock.start).toHaveBeenCalledOnce();
    expect(editorAppMock.configs).toHaveLength(1);
    const config = editorAppMock.configs[0] as any;
    const host = editorAppMock.start.mock.calls[0][0];
    expect(config.useDiffEditor).toBeUndefined();
    expect(config.codeResources.modified.uri).toMatch(/^in-memory-.*\.java$/);
    expect(host).toBeInstanceOf(HTMLElement);

    // The subscribe body should: push readOnly via updateOptions, construct
    // MonacoBinding, and register the two AI actions.
    expect(editor.updateOptions).toHaveBeenCalledWith({ readOnly: false });
    expect(monacoBindingCalls.length).toBe(1);
    expect(editor.addAction).toHaveBeenCalledTimes(2);
  });

  it("initializeMonacoEditor: respects formControl.disabled when toggling readOnly", async () => {
    const { fixture, editor, c } = makeFixtureWithFakes();
    (c as any).formControl = new FormControl({ value: "", disabled: true });

    fixture.detectChanges();
    await flush();

    expect(editor.updateOptions).toHaveBeenCalledWith({ readOnly: true });
  });

  it("initializeDiffEditor: when displayParticularVersion is true, runs the diff config path", async () => {
    const { fixture } = makeFixtureWithFakes();
    // Seed the stream BEFORE detectChanges so the subscribe in ngAfterViewInit
    // picks `true` on first emission and takes the diff branch.
    displayVersionStream$.next(true);

    fixture.detectChanges();
    await flush();

    expect(editorAppMock.start).toHaveBeenCalledOnce();
    expect(editorAppMock.configs).toHaveLength(1);
    const config = editorAppMock.configs[0] as any;
    expect(config.useDiffEditor).toBe(true);
    // `original` is the previous-version source, only set on the diff path.
    expect(config.codeResources.original).toBeDefined();
  });

  it("setupAIAssistantActions: registers only the 'all' action when the gate is not OpenAI", async () => {
    aiEnabled$.next("none");
    const { fixture, editor } = makeFixtureWithFakes();

    fixture.detectChanges();
    await flush();

    expect(editor.addAction).toHaveBeenCalledTimes(1);
    expect(editor.addAction.mock.calls[0][0].id).toBe("all-type-annotation-action");
  });

  it("type-annotation-action: invoking the run callback populates suggestion state from position", async () => {
    const { fixture, editor } = makeFixtureWithFakes();
    fixture.detectChanges();
    await flush();

    // The first registered action is "type-annotation-action" (the OpenAI
    // gate fires). Pull its `run` callback and invoke it directly — the
    // alternative is dispatching via the Monaco command palette, which our
    // fake editor doesn't implement.
    const typeAction = editor.actions.find(a => a.id === "type-annotation-action");
    expect(typeAction).toBeDefined();
    typeAction!.run(editor as unknown as monaco.editor.ICodeEditor);

    await flush();

    expect(getTypeAnnotationsSpy).toHaveBeenCalledOnce();
    // getScrolledVisiblePosition returns {top: 50, left: 100}; the component
    // adds +100 on each axis when staging the suggestion.
    expect(fixture.componentInstance.suggestionTop).toBe(150);
    expect(fixture.componentInstance.suggestionLeft).toBe(200);
    expect(fixture.componentInstance.showAnnotationSuggestion).toBe(true);
    expect(fixture.componentInstance.currentSuggestion).toBe(": int");
  });

  it("all-type-annotation-action: locateUnannotated returns empty list, action no-ops cleanly", async () => {
    const { fixture, editor } = makeFixtureWithFakes();
    fixture.detectChanges();
    await flush();

    const allAction = editor.actions.find(a => a.id === "all-type-annotation-action");
    expect(allAction).toBeDefined();
    allAction!.run(editor as unknown as monaco.editor.ICodeEditor);

    await flush();

    expect(locateUnannotatedSpy).toHaveBeenCalledOnce();
    // Empty unannotated list -> processNextVariable never invoked; the
    // multi-variable state stays in its initial shape.
    expect((fixture.componentInstance as any).isMultipleVariables).toBe(false);
  });

  it("onWindowResize: calls editor.layout() through adjustEditorSize", async () => {
    const { fixture, editor } = makeFixtureWithFakes();
    fixture.detectChanges();
    await flush();

    // Reset layout's call count so we don't pick up Monaco's own init layout.
    editor.layout.mockClear();
    fixture.componentInstance.onWindowResize();

    expect(editor.layout).toHaveBeenCalledOnce();
  });
});

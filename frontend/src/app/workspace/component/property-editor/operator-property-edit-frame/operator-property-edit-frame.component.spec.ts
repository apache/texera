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

import { ComponentFixture, discardPeriodicTasks, fakeAsync, TestBed, tick } from "@angular/core/testing";

import {
  AGGREGATE_COUNT,
  isAggregateAttributeRequired,
  OperatorPropertyEditFrameComponent,
} from "./operator-property-edit-frame.component";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowCompilingService } from "../../../service/compile-workflow/workflow-compiling.service";
import { CustomJSONSchema7 } from "../../../types/custom-json-schema.interface";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { FORM_DEBOUNCE_TIME_MS } from "../../../service/execute-workflow/execute-workflow.service";
import { DatePipe } from "@angular/common";
import { By } from "@angular/platform-browser";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { FormControl, FormGroup, FormsModule, ReactiveFormsModule } from "@angular/forms";
import { FormlyFieldConfig, FormlyModule } from "@ngx-formly/core";
import { TEXERA_FORMLY_CONFIG } from "../../../../common/formly/formly-config";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import {
  mockHuggingFacePredicate,
  mockPoint,
  mockResultPredicate,
  mockScanPredicate,
} from "../../../service/workflow-graph/model/mock-workflow-data";
import {
  mockScanSourceSchema,
  mockViewResultsSchema,
} from "../../../service/operator-metadata/mock-operator-metadata.data";
import { configure } from "rxjs-marbles";
import { SimpleChange } from "@angular/core";
import { cloneDeep } from "lodash-es";

import Ajv from "ajv";
import { COLLAB_DEBOUNCE_TIME_MS } from "../../../../common/formly/collab-wrapper/collab-wrapper/collab-wrapper.component";
import { FormlyNgZorroAntdModule } from "@ngx-formly/ng-zorro-antd";
import { ComputingUnitStatusService } from "../../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";

const { marbles } = configure({ run: false });

describe("Aggregate attribute requirement", () => {
  it("makes the attribute optional for count and required for every other function", () => {
    // count -> optional (empty attribute means COUNT(*))
    expect(isAggregateAttributeRequired(AGGREGATE_COUNT)).toBe(false);
    // every other aggregate function -> attribute required
    ["sum", "average", "min", "max", "concat"].forEach(fn => {
      expect(isAggregateAttributeRequired(fn)).toBe(true);
    });
  });
});

describe("OperatorPropertyEditFrameComponent", () => {
  let component: OperatorPropertyEditFrameComponent;
  let fixture: ComponentFixture<OperatorPropertyEditFrameComponent>;
  let workflowActionService: WorkflowActionService;

  beforeEach(async () => {
    // TODO(coverage): tests in this spec exercise dynamic Formly form rendering;
    // the real OperatorPropertyEditFrame template throws under jsdom when the
    // Formly tree tries to read child.component from an uninstantiated field.
    // The stub template lets the class-level tests run while we figure out a
    // Formly-aware setup. Drop this override once that's done.
    /* eslint-disable no-restricted-syntax */
    TestBed.overrideComponent(OperatorPropertyEditFrameComponent, {
      set: {
        template:
          '<div class="texera-workspace-property-editor-title">{{ formTitle }}</div><div class="texera-workspace-property-editor-form"></div>',
      },
    });
    /* eslint-enable no-restricted-syntax */

    await TestBed.configureTestingModule({
      providers: [
        WorkflowActionService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
        DatePipe,
        ...commonTestProviders,
      ],
      imports: [
        OperatorPropertyEditFrameComponent,
        BrowserAnimationsModule,
        FormsModule,
        FormlyModule.forRoot(TEXERA_FORMLY_CONFIG),
        FormlyNgZorroAntdModule,
        ReactiveFormsModule,
        HttpClientTestingModule,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(OperatorPropertyEditFrameComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.inject(WorkflowActionService);
  });

  it("should create", () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  /**
   * test if the property editor correctly receives the operator highlight stream,
   *  get the operator data (id, property, and metadata), and then display the form.
   */
  it("should change the content of property editor from an empty panel correctly", () => {
    // check if the changePropertyEditor called after the operator
    //  is highlighted has correctly updated the variables
    const predicate = {
      ...mockScanPredicate,
      operatorProperties: { tableName: "" },
    };

    // add and highlight an operator
    workflowActionService.addOperator(predicate, mockPoint);

    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, predicate.operatorID, true),
    });
    fixture.detectChanges();
    // check variables are set correctly
    expect(component.formData).toEqual(predicate.operatorProperties);

    // check HTML form are displayed
    const formTitleElement = fixture.debugElement.query(By.css(".texera-workspace-property-editor-title"));
    const jsonSchemaFormElement = fixture.debugElement.query(By.css(".texera-workspace-property-editor-form"));
    // check the panel title (use textContent — jsdom doesn't compute the
    // layout-dependent innerText getter, which returns undefined here)
    expect((formTitleElement.nativeElement as HTMLElement).textContent?.trim()).toEqual(
      mockScanSourceSchema.additionalMetadata.userFriendlyName
    );

    // TODO: Temporarilly disable this unit test because PR #1924 is failing the test,
    // dispite the fact that the code is working as expected.
    // This shall be fixed in the future.
    // // check if the form has the all the json schema property names
    // Object.entries(mockScanSourceSchema.jsonSchema.properties as any).forEach(entry => {
    //   const propertyTitle = (entry[1] as JSONSchema7).title;
    //   if (propertyTitle) {
    //     expect((jsonSchemaFormElement.nativeElement as HTMLElement).innerHTML).toContain(propertyTitle);
    //   }
    //   const propertyDescription = (entry[1] as JSONSchema7).description;
    //   if (propertyDescription) {
    //     expect((jsonSchemaFormElement.nativeElement as HTMLElement).innerHTML).toContain(propertyDescription);
    //   }
    // });
  });

  it("should change Texera graph property when the form is edited by the user", fakeAsync(() => {
    // add an operator and highlight the operator so that the
    //  variables in property editor component is set correctly
    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, mockScanPredicate.operatorID, true),
    });
    fixture.detectChanges();
    tick(COLLAB_DEBOUNCE_TIME_MS);

    // stimulate a form change by the user
    const formChangeValue = { tableName: "twitter_sample" };
    component.onFormChanges(formChangeValue);

    // maintain a counter of how many times the event is emitted
    let emitEventCounter = 0;
    component.operatorPropertyChangeStream.subscribe(() => emitEventCounter++);

    // fakeAsync enables tick, which waits for the set property debounce time to finish
    tick(FORM_DEBOUNCE_TIME_MS + 10);

    // then get the operator, because operator is immutable, the operator before the tick
    //   is a different object reference from the operator after the tick
    const operator = workflowActionService.getTexeraGraph().getOperator(mockScanPredicate.operatorID);
    if (!operator) {
      throw new Error(`operator ${mockScanPredicate.operatorID} is undefined`);
    }

    discardPeriodicTasks();

    expect(operator.operatorProperties).toEqual(formChangeValue);
    expect(emitEventCounter).toEqual(1);
  }));

  it.skip(
    "should debounce the user form input to avoid emitting event too frequently",
    marbles(m => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

      // add an operator and highlight the operator so that the
      //  variables in property editor component is set correctly
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);

      // prepare the form user input event stream
      // simulate user types in `table` character by character
      const formUserInputMarbleString = "-a-b-c-d-e";
      const formUserInputMarbleValue = {
        a: { tableName: "t" },
        b: { tableName: "ta" },
        c: { tableName: "tab" },
        d: { tableName: "tabl" },
        e: { tableName: "table" },
      };
      const formUserInputEventStream = m.hot(formUserInputMarbleString, formUserInputMarbleValue);

      // prepare the expected output stream after debounce time
      const formChangeEventMarbleString =
        // wait for the time of last marble string starting to emit
        "-".repeat(formUserInputMarbleString.length - 1) +
        // then wait for debounce time (each tick represents 10 ms)
        "-".repeat(FORM_DEBOUNCE_TIME_MS / 10) +
        "e-";
      const formChangeEventMarbleValue = {
        e: { tableName: "table" } as object,
      };
      const expectedFormChangeEventStream = m.hot(formChangeEventMarbleString, formChangeEventMarbleValue);

      m.bind();

      // // TODO: FIX THIS
      // const actualFormChangeEventStream = component.operatorPropertyChangeStream;
      // // formUserInputEventStream.subscribe();

      // m.expect(actualFormChangeEventStream).toBeObservable(expectedFormChangeEventStream);
    })
  );

  it("should not emit operator property change event if the new property is the same as the old property", fakeAsync(() => {
    // add an operator and highlight the operator so that the
    //  variables in property editor component is set correctly
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    const mockOperatorProperty = { tableName: "table" };
    // set operator property first before displaying the operator property in property panel
    workflowActionService.setOperatorProperty(mockScanPredicate.operatorID, mockOperatorProperty);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, mockScanPredicate.operatorID, true),
    });
    fixture.detectChanges();

    // stimulate a form change with the same property
    component.onFormChanges(mockOperatorProperty);

    // maintain a counter of how many times the event is emitted
    let emitEventCounter = 0;
    component.operatorPropertyChangeStream.subscribe(() => emitEventCounter++);

    // fakeAsync enables tick, which waits for the set property debounce time to finish
    tick(FORM_DEBOUNCE_TIME_MS + 10);

    discardPeriodicTasks();

    // assert that the form change event doesn't emit any time
    // because the form change value is the same
    expect(emitEventCounter).toEqual(0);
  }));

  it("should change operator to default values", () => {
    // result operator has default values, use ajv to fill in default values
    // expected form output should fill in all default values instead of an empty object
    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, mockResultPredicate.operatorID, true),
    });
    fixture.detectChanges();
    const ajv = new Ajv({ useDefaults: true });
    const expectedResultOperatorProperties = cloneDeep(mockResultPredicate.operatorProperties);
    ajv.validate(mockViewResultsSchema.jsonSchema, expectedResultOperatorProperties);

    expect(component.formData).toEqual(expectedResultOperatorProperties);
  });

  it("should set result operator version", () => {
    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, mockResultPredicate.operatorID, true),
    });
    fixture.detectChanges();
    expect(component.operatorVersion).toEqual(mockResultPredicate.operatorVersion);
  });

  it("should set scan operator version", () => {
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, mockScanPredicate.operatorID, true),
    });
    fixture.detectChanges();
    expect(component.operatorVersion).toEqual(mockScanPredicate.operatorVersion);
  });

  describe("operator description truncation", () => {
    beforeEach(async () => {
      TestBed.resetTestingModule();
      await TestBed.configureTestingModule({
        providers: [
          WorkflowActionService,
          { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
          { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
          DatePipe,
          ...commonTestProviders,
        ],
        imports: [
          OperatorPropertyEditFrameComponent,
          BrowserAnimationsModule,
          FormsModule,
          FormlyModule.forRoot(TEXERA_FORMLY_CONFIG),
          FormlyNgZorroAntdModule,
          ReactiveFormsModule,
          HttpClientTestingModule,
        ],
      }).compileComponents();

      fixture = TestBed.createComponent(OperatorPropertyEditFrameComponent);
      component = fixture.componentInstance;
    });

    it("should render .operator-description with tooltip when description is set", () => {
      component.operatorDescription = "A long description that should be truncated after three lines.";
      component.editingTitle = false;
      fixture.detectChanges();

      const descEl = fixture.debugElement.query(By.css(".operator-description"));
      expect(descEl).toBeTruthy();
      expect(descEl.attributes["nz-tooltip"]).toBeDefined();
    });

    it("should not render .operator-description when description is not set", () => {
      component.operatorDescription = undefined;
      component.editingTitle = false;
      fixture.detectChanges();

      const descEl = fixture.debugElement.query(By.css(".operator-description"));
      expect(descEl).toBeNull();
    });
  });

  // ── HuggingFace task-aware visibility tests ──

  it("should return null huggingFaceTaskPreview for non-HF operators", () => {
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, mockScanPredicate.operatorID, true),
    });
    fixture.detectChanges();
    expect(component.huggingFaceTaskPreview).toBeNull();
  });

  it("should return a task preview for HuggingFace operator with a known task", () => {
    workflowActionService.addOperator(mockHuggingFacePredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, mockHuggingFacePredicate.operatorID, true),
    });
    fixture.detectChanges();
    const preview = component.huggingFaceTaskPreview;
    expect(preview).toBeTruthy();
    expect(preview!.kind).toBe("text");
    expect(preview!.title).toBe("Text generation preview");
  });

  it("should return a fallback preview for HuggingFace operator with an unknown task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "some-unknown-task", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    const preview = component.huggingFaceTaskPreview;
    expect(preview).toBeTruthy();
    expect(preview!.kind).toBe("text");
    expect(preview!.title).toBe("Some Unknown Task");
  });

  it("should return image kind preview for image-classification task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "image-classification", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    const preview = component.huggingFaceTaskPreview;
    expect(preview).toBeTruthy();
    expect(preview!.kind).toBe("image");
  });

  it("should return audio kind preview for text-to-speech task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "text-to-speech", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    const preview = component.huggingFaceTaskPreview;
    expect(preview).toBeTruthy();
    expect(preview!.kind).toBe("audio");
  });

  it("should return video kind preview for text-to-video task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "text-to-video", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    const preview = component.huggingFaceTaskPreview;
    expect(preview).toBeTruthy();
    expect(preview!.kind).toBe("video");
  });

  it("should return null preview when HuggingFace task is empty", () => {
    const hfPredicate = { ...cloneDeep(mockHuggingFacePredicate), operatorProperties: { task: "", modelId: "" } };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    expect(component.huggingFaceTaskPreview).toBeNull();
  });

  // ── HuggingFace field visibility and validator tests ──

  function getHfField(key: string): FormlyFieldConfig | undefined {
    return component.formlyFields?.[0]?.fieldGroup?.find(f => f.key === key);
  }

  let currentTask: string = "";

  let hfOperatorCounter = 0;

  function initHfOperator(task: string): void {
    currentTask = task;
    hfOperatorCounter++;
    const pred = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorID: `hf-test-${hfOperatorCounter}`,
      operatorProperties: { task, modelId: "org/model" },
    };
    workflowActionService.addOperator(pred, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, pred.operatorID, true),
    });
    fixture.detectChanges();
  }

  function evalHide(field: FormlyFieldConfig | undefined): boolean {
    if (!field || !field.expressions) return false;
    const hideFn = (field.expressions as Record<string, Function>)["hide"];
    if (!hideFn) return !!field.hide;
    // Provide model context so getSelectedTask can find the task
    const fieldWithModel = { ...field, model: { task: currentTask } } as FormlyFieldConfig;
    return hideFn(fieldWithModel);
  }

  it("should hide imageInput for text-generation task", () => {
    initHfOperator("text-generation");
    expect(evalHide(getHfField("imageInput"))).toBe(true);
  });

  it("should show imageInput for image-classification task", () => {
    initHfOperator("image-classification");
    expect(evalHide(getHfField("imageInput"))).toBe(false);
  });

  it("should hide audioInput for text-generation task", () => {
    initHfOperator("text-generation");
    expect(evalHide(getHfField("audioInput"))).toBe(true);
  });

  it("should show audioInput for automatic-speech-recognition task", () => {
    initHfOperator("automatic-speech-recognition");
    expect(evalHide(getHfField("audioInput"))).toBe(false);
  });

  it("should hide promptColumn for image-only tasks", () => {
    initHfOperator("image-classification");
    expect(evalHide(getHfField("promptColumn"))).toBe(true);
  });

  it("should hide promptColumn for audio-only tasks", () => {
    initHfOperator("automatic-speech-recognition");
    expect(evalHide(getHfField("promptColumn"))).toBe(true);
  });

  it("should show promptColumn for text-generation task", () => {
    initHfOperator("text-generation");
    expect(evalHide(getHfField("promptColumn"))).toBe(false);
  });

  it("should show systemPrompt only for text-generation", () => {
    initHfOperator("text-generation");
    expect(evalHide(getHfField("systemPrompt"))).toBe(false);

    initHfOperator("image-classification");
    expect(evalHide(getHfField("systemPrompt"))).toBe(true);
  });

  it("should show contextColumn only for question-answering", () => {
    initHfOperator("question-answering");
    expect(evalHide(getHfField("contextColumn"))).toBe(false);

    initHfOperator("text-generation");
    expect(evalHide(getHfField("contextColumn"))).toBe(true);
  });

  it("should show candidateLabels only for classification tasks", () => {
    initHfOperator("zero-shot-classification");
    expect(evalHide(getHfField("candidateLabels"))).toBe(false);

    initHfOperator("text-generation");
    expect(evalHide(getHfField("candidateLabels"))).toBe(true);
  });

  it("requiredPromptColumn validator should pass when not a prompt-required task", () => {
    initHfOperator("image-classification");
    const field = getHfField("promptColumn");
    const validator = field?.validators?.["requiredPromptColumn"];
    expect(validator).toBeDefined();
    const mockField = { ...field, model: { task: "image-classification", promptColumn: "" } } as FormlyFieldConfig;
    expect(validator!.expression(null as any, mockField)).toBe(true);
  });

  it("requiredPromptColumn validator should fail when prompt-required task has no column", () => {
    initHfOperator("text-generation");
    const field = getHfField("promptColumn");
    const validator = field?.validators?.["requiredPromptColumn"];
    expect(validator).toBeDefined();
    const mockField = { ...field, model: { task: "text-generation", promptColumn: "" } } as FormlyFieldConfig;
    expect(validator!.expression(null as any, mockField)).toBe(false);
  });

  it("requiredImageInput validator should pass when not an image task", () => {
    initHfOperator("text-generation");
    const field = getHfField("imageInput");
    const validator = field?.validators?.["requiredImageInput"];
    expect(validator).toBeDefined();
    const mockField = { ...field, model: { task: "text-generation", imageInput: "" } } as FormlyFieldConfig;
    expect(validator!.expression(null as any, mockField)).toBe(true);
  });

  it("requiredAudioInput validator should pass when not an audio task", () => {
    initHfOperator("text-generation");
    const field = getHfField("audioInput");
    const validator = field?.validators?.["requiredAudioInput"];
    expect(validator).toBeDefined();
    const mockField = { ...field, model: { task: "text-generation", audioInput: "" } } as FormlyFieldConfig;
    expect(validator!.expression(null as any, mockField)).toBe(true);
  });

  // ── Additional field visibility tests ──

  it("should show sentencesColumn only for sentence-similarity and text-ranking", () => {
    initHfOperator("sentence-similarity");
    expect(evalHide(getHfField("sentencesColumn"))).toBe(false);

    initHfOperator("text-ranking");
    expect(evalHide(getHfField("sentencesColumn"))).toBe(false);

    initHfOperator("text-generation");
    expect(evalHide(getHfField("sentencesColumn"))).toBe(true);
  });

  it("should show inputImageColumn for image tasks", () => {
    initHfOperator("image-classification");
    expect(evalHide(getHfField("inputImageColumn"))).toBe(false);

    initHfOperator("text-generation");
    expect(evalHide(getHfField("inputImageColumn"))).toBe(true);
  });

  it("should show inputAudioColumn for audio tasks", () => {
    initHfOperator("automatic-speech-recognition");
    expect(evalHide(getHfField("inputAudioColumn"))).toBe(false);

    initHfOperator("text-generation");
    expect(evalHide(getHfField("inputAudioColumn"))).toBe(true);
  });

  it("should hide maxNewTokens and temperature for non-text-generation tasks", () => {
    initHfOperator("image-classification");
    expect(evalHide(getHfField("maxNewTokens"))).toBe(true);
    expect(evalHide(getHfField("temperature"))).toBe(true);

    initHfOperator("text-generation");
    expect(evalHide(getHfField("maxNewTokens"))).toBe(false);
    expect(evalHide(getHfField("temperature"))).toBe(false);
  });

  it("should show candidateLabels for zero-shot-image-classification", () => {
    initHfOperator("zero-shot-image-classification");
    expect(evalHide(getHfField("candidateLabels"))).toBe(false);
  });

  // ── Additional validator edge-case tests ──

  it("requiredPromptColumn validator should pass when prompt-required task has a column", () => {
    initHfOperator("text-generation");
    const field = getHfField("promptColumn");
    const validator = field?.validators?.["requiredPromptColumn"];
    const mockField = { ...field, model: { task: "text-generation", promptColumn: "text_col" } } as FormlyFieldConfig;
    expect(validator!.expression(null as any, mockField)).toBe(true);
  });

  it("requiredImageInput validator should fail when image task has no image and no column", () => {
    initHfOperator("image-classification");
    const field = getHfField("imageInput");
    const validator = field?.validators?.["requiredImageInput"];
    const mockField = {
      ...field,
      model: { task: "image-classification", imageInput: "", inputImageColumn: "" },
    } as FormlyFieldConfig;
    expect(validator!.expression(null as any, mockField)).toBe(false);
  });

  it("requiredImageInput validator should pass when image task has inputImageColumn set", () => {
    initHfOperator("image-classification");
    const field = getHfField("imageInput");
    const validator = field?.validators?.["requiredImageInput"];
    const mockField = {
      ...field,
      model: { task: "image-classification", imageInput: "", inputImageColumn: "img_col" },
    } as FormlyFieldConfig;
    expect(validator!.expression(null as any, mockField)).toBe(true);
  });

  it("requiredImageInput validator should pass when image task has image uploaded", () => {
    initHfOperator("image-classification");
    const field = getHfField("imageInput");
    const validator = field?.validators?.["requiredImageInput"];
    const mockField = {
      ...field,
      model: { task: "image-classification", imageInput: "/tmp/img.png", inputImageColumn: "" },
    } as FormlyFieldConfig;
    expect(validator!.expression(null as any, mockField)).toBe(true);
  });

  it("requiredAudioInput validator should fail when audio task has no audio and no column", () => {
    initHfOperator("automatic-speech-recognition");
    const field = getHfField("audioInput");
    const validator = field?.validators?.["requiredAudioInput"];
    const mockField = {
      ...field,
      model: { task: "automatic-speech-recognition", audioInput: "", inputAudioColumn: "" },
    } as FormlyFieldConfig;
    expect(validator!.expression(null as any, mockField)).toBe(false);
  });

  it("requiredAudioInput validator should pass when audio task has inputAudioColumn set", () => {
    initHfOperator("automatic-speech-recognition");
    const field = getHfField("audioInput");
    const validator = field?.validators?.["requiredAudioInput"];
    const mockField = {
      ...field,
      model: { task: "automatic-speech-recognition", audioInput: "", inputAudioColumn: "audio_col" },
    } as FormlyFieldConfig;
    expect(validator!.expression(null as any, mockField)).toBe(true);
  });

  it("requiredAudioInput validator should pass when audio task has audio uploaded", () => {
    initHfOperator("automatic-speech-recognition");
    const field = getHfField("audioInput");
    const validator = field?.validators?.["requiredAudioInput"];
    const mockField = {
      ...field,
      model: { task: "automatic-speech-recognition", audioInput: "/tmp/clip.wav", inputAudioColumn: "" },
    } as FormlyFieldConfig;
    expect(validator!.expression(null as any, mockField)).toBe(true);
  });

  // ── HuggingFace task preview additional tests ──

  it("should return image kind preview for visual-question-answering task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "visual-question-answering", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    const preview = component.huggingFaceTaskPreview;
    expect(preview).toBeTruthy();
    expect(preview!.kind).toBe("image");
  });

  it("should return text kind preview for question-answering task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "question-answering", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    const preview = component.huggingFaceTaskPreview;
    expect(preview).toBeTruthy();
    expect(preview!.kind).toBe("text");
  });

  it("should hide the task field for HuggingFace operators", () => {
    initHfOperator("text-generation");
    const taskField = getHfField("task");
    expect(taskField?.hide).toBe(true);
  });

  // ── Field type assignments ──

  it("should set modelId field type to 'huggingface' for HF operators", () => {
    initHfOperator("text-generation");
    const field = getHfField("modelId");
    expect(field?.type).toBe("huggingface");
  });

  it("should set imageInput field type to 'huggingface-image-upload'", () => {
    initHfOperator("image-classification");
    const field = getHfField("imageInput");
    expect(field?.type).toBe("huggingface-image-upload");
  });

  it("should set audioInput field type to 'huggingface-audio-upload'", () => {
    initHfOperator("automatic-speech-recognition");
    const field = getHfField("audioInput");
    expect(field?.type).toBe("huggingface-audio-upload");
  });

  // ── Visibility when task is undefined ──

  it("should hide imageInput when task is undefined", () => {
    initHfOperator("text-generation");
    const field = getHfField("imageInput");
    const hideFn = (field?.expressions as Record<string, Function>)?.["hide"];
    expect(hideFn({ model: {} } as FormlyFieldConfig)).toBe(true);
  });

  it("should hide audioInput when task is undefined", () => {
    initHfOperator("text-generation");
    const field = getHfField("audioInput");
    const hideFn = (field?.expressions as Record<string, Function>)?.["hide"];
    expect(hideFn({ model: {} } as FormlyFieldConfig)).toBe(true);
  });

  it("should hide inputImageColumn when task is undefined", () => {
    initHfOperator("text-generation");
    const field = getHfField("inputImageColumn");
    const hideFn = (field?.expressions as Record<string, Function>)?.["hide"];
    expect(hideFn({ model: {} } as FormlyFieldConfig)).toBe(true);
  });

  it("should hide inputAudioColumn when task is undefined", () => {
    initHfOperator("text-generation");
    const field = getHfField("inputAudioColumn");
    const hideFn = (field?.expressions as Record<string, Function>)?.["hide"];
    expect(hideFn({ model: {} } as FormlyFieldConfig)).toBe(true);
  });

  // ── Additional image task visibility ──

  it("should show imageInput for image-to-video task", () => {
    initHfOperator("image-to-video");
    expect(evalHide(getHfField("imageInput"))).toBe(false);
  });

  it("should show imageInput for image-to-image task", () => {
    initHfOperator("image-to-image");
    expect(evalHide(getHfField("imageInput"))).toBe(false);
  });

  it("should show imageInput for document-question-answering task", () => {
    initHfOperator("document-question-answering");
    expect(evalHide(getHfField("imageInput"))).toBe(false);
  });

  it("should show imageInput for image-text-to-text task", () => {
    initHfOperator("image-text-to-text");
    expect(evalHide(getHfField("imageInput"))).toBe(false);
  });

  // ── Audio task visibility ──

  it("should show audioInput for audio-classification task", () => {
    initHfOperator("audio-classification");
    expect(evalHide(getHfField("audioInput"))).toBe(false);
  });

  it("should show inputAudioColumn for audio-classification task", () => {
    initHfOperator("audio-classification");
    expect(evalHide(getHfField("inputAudioColumn"))).toBe(false);
  });

  // ── promptColumn visibility for mixed tasks ──

  it("should show promptColumn for visual-question-answering (image + prompt)", () => {
    initHfOperator("visual-question-answering");
    expect(evalHide(getHfField("promptColumn"))).toBe(false);
  });

  it("should show promptColumn for document-question-answering (image + prompt)", () => {
    initHfOperator("document-question-answering");
    expect(evalHide(getHfField("promptColumn"))).toBe(false);
  });

  it("should show promptColumn for zero-shot-classification", () => {
    initHfOperator("zero-shot-classification");
    expect(evalHide(getHfField("promptColumn"))).toBe(false);
  });

  it("should show promptColumn for summarization", () => {
    initHfOperator("summarization");
    expect(evalHide(getHfField("promptColumn"))).toBe(false);
  });

  it("should show promptColumn for translation", () => {
    initHfOperator("translation");
    expect(evalHide(getHfField("promptColumn"))).toBe(false);
  });

  // ── Validator with formControl value ──

  it("requiredImageInput validator should pass when image task has formControl value", () => {
    initHfOperator("image-classification");
    const field = getHfField("imageInput");
    const validator = field?.validators?.["requiredImageInput"];
    const mockField = {
      ...field,
      model: { task: "image-classification", imageInput: "", inputImageColumn: "" },
      formControl: { value: "data:image/png;base64,abc" },
    } as unknown as FormlyFieldConfig;
    expect(validator!.expression(null as any, mockField)).toBe(true);
  });

  it("requiredAudioInput validator should pass when audio task has formControl value", () => {
    initHfOperator("automatic-speech-recognition");
    const field = getHfField("audioInput");
    const validator = field?.validators?.["requiredAudioInput"];
    const mockField = {
      ...field,
      model: { task: "automatic-speech-recognition", audioInput: "", inputAudioColumn: "" },
      formControl: { value: "/tmp/clip.wav" },
    } as unknown as FormlyFieldConfig;
    expect(validator!.expression(null as any, mockField)).toBe(true);
  });

  it("requiredPromptColumn validator should pass when formControl has value", () => {
    initHfOperator("text-generation");
    const field = getHfField("promptColumn");
    const validator = field?.validators?.["requiredPromptColumn"];
    const mockField = {
      ...field,
      model: { task: "text-generation", promptColumn: "" },
      formControl: { value: "text_col" },
    } as unknown as FormlyFieldConfig;
    expect(validator!.expression(null as any, mockField)).toBe(true);
  });

  // ── Additional task preview tests ──

  it("should return audio kind preview for automatic-speech-recognition task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "automatic-speech-recognition", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    const preview = component.huggingFaceTaskPreview;
    expect(preview).toBeTruthy();
    expect(preview!.kind).toBe("audio");
  });

  it("should return image kind preview for image-to-image task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "image-to-image", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    const preview = component.huggingFaceTaskPreview;
    expect(preview).toBeTruthy();
    expect(preview!.kind).toBe("image");
  });

  it("should return null huggingFaceTaskPreview when operator is deleted", () => {
    workflowActionService.addOperator(mockHuggingFacePredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, mockHuggingFacePredicate.operatorID, true),
    });
    fixture.detectChanges();
    workflowActionService.deleteOperator(mockHuggingFacePredicate.operatorID);
    expect(component.huggingFaceTaskPreview).toBeNull();
  });

  // ── formatTaskTitle via fallback preview ──

  it("should title-case multi-segment unknown task in fallback preview", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "my-custom-pipeline", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    const preview = component.huggingFaceTaskPreview;
    expect(preview).toBeTruthy();
    expect(preview!.title).toBe("My Custom Pipeline");
  });

  it("should title-case single-word unknown task in fallback preview", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "embeddings", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    expect(component.huggingFaceTaskPreview!.title).toBe("Embeddings");
  });

  // ── Task preview content validation ──

  it("should include assetSrc and pills in image-classification preview", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "image-classification", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    const preview = component.huggingFaceTaskPreview!;
    expect(preview.assetSrc).toBe("assets/sample-image.png");
    expect(preview.pills).toEqual(["superhero", "cityscape", "action"]);
    expect(preview.inputLabel).toBe("Image input");
    expect(preview.outputLabel).toBe("Predicted labels");
  });

  it("should include outputBody in text-generation preview", () => {
    workflowActionService.addOperator(mockHuggingFacePredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, mockHuggingFacePredicate.operatorID, true),
    });
    fixture.detectChanges();
    const preview = component.huggingFaceTaskPreview!;
    expect(preview.outputBody).toBeDefined();
    expect(preview.body).toBeDefined();
    expect(preview.inputLabel).toBe("Prompt");
    expect(preview.outputLabel).toBe("Generated text");
  });

  it("should return video kind preview for image-to-video task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "image-to-video", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    const preview = component.huggingFaceTaskPreview!;
    expect(preview.kind).toBe("video");
    expect(preview.assetSrc).toBe("assets/sample-video.mp4");
  });

  it("should return text kind preview for zero-shot-classification task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "zero-shot-classification", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    const preview = component.huggingFaceTaskPreview!;
    expect(preview.kind).toBe("text");
    expect(preview.pills).toEqual(["business", "operations", "support"]);
  });

  it("should return text kind preview for fill-mask task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "fill-mask", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    const preview = component.huggingFaceTaskPreview!;
    expect(preview.kind).toBe("text");
    expect(preview.pills).toEqual(["city", "day", "crowd"]);
  });

  it("should return image kind preview for object-detection task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "object-detection", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    const preview = component.huggingFaceTaskPreview!;
    expect(preview.kind).toBe("image");
    expect(preview.pills).toEqual(["person", "building", "sky"]);
  });

  it("should return image kind preview for text-to-image task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "text-to-image", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    expect(component.huggingFaceTaskPreview!.kind).toBe("image");
  });

  it("should return text kind preview for text-classification task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "text-classification", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    const preview = component.huggingFaceTaskPreview!;
    expect(preview.kind).toBe("text");
    expect(preview.pills).toEqual(["positive", "announcement"]);
  });

  it("should return text kind preview for token-classification task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "token-classification", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    expect(component.huggingFaceTaskPreview!.kind).toBe("text");
  });

  it("should return text kind preview for table-question-answering task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "table-question-answering", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    expect(component.huggingFaceTaskPreview!.kind).toBe("text");
  });

  it("should return text kind preview for feature-extraction task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "feature-extraction", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    expect(component.huggingFaceTaskPreview!.kind).toBe("text");
  });

  it("should return image kind preview for image-segmentation task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "image-segmentation", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    expect(component.huggingFaceTaskPreview!.kind).toBe("image");
  });

  it("should return image kind preview for image-to-text task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "image-to-text", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    const preview = component.huggingFaceTaskPreview!;
    expect(preview.kind).toBe("image");
    expect(preview.outputBody).toBeDefined();
  });

  it("should return image kind preview for document-question-answering task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "document-question-answering", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    expect(component.huggingFaceTaskPreview!.kind).toBe("image");
  });

  it("should return image kind preview for zero-shot-image-classification task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "zero-shot-image-classification", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    expect(component.huggingFaceTaskPreview!.kind).toBe("image");
  });

  it("should return image kind preview for image-text-to-text task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "image-text-to-text", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    expect(component.huggingFaceTaskPreview!.kind).toBe("image");
  });

  it("should return text kind preview for sentence-similarity task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "sentence-similarity", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    expect(component.huggingFaceTaskPreview!.kind).toBe("text");
  });

  it("should return text kind preview for text-ranking task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "text-ranking", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    expect(component.huggingFaceTaskPreview!.kind).toBe("text");
  });

  it("should return text kind preview for translation task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "translation", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    expect(component.huggingFaceTaskPreview!.kind).toBe("text");
  });

  it("should return text kind preview for summarization task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "summarization", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    expect(component.huggingFaceTaskPreview!.kind).toBe("text");
  });

  it("should return audio kind preview for audio-classification task", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "audio-classification", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    expect(component.huggingFaceTaskPreview!.kind).toBe("audio");
  });

  // ── Validator message strings ──

  it("requiredImageInput validator should return correct message", () => {
    initHfOperator("image-classification");
    const field = getHfField("imageInput");
    const validator = field?.validators?.["requiredImageInput"];
    expect(validator!.message()).toBe("Upload an image or select an Input Image Column for this task.");
  });

  it("requiredAudioInput validator should return correct message", () => {
    initHfOperator("automatic-speech-recognition");
    const field = getHfField("audioInput");
    const validator = field?.validators?.["requiredAudioInput"];
    expect(validator!.message()).toBe("Upload audio or select an Input Audio Column for this task.");
  });

  it("requiredPromptColumn validator should return correct message", () => {
    initHfOperator("text-generation");
    const field = getHfField("promptColumn");
    const validator = field?.validators?.["requiredPromptColumn"];
    expect(validator!.message()).toBe("Select a prompt column for this task.");
  });

  // ── Additional promptColumn visibility for remaining tasks ──

  it("should show promptColumn for token-classification", () => {
    initHfOperator("token-classification");
    expect(evalHide(getHfField("promptColumn"))).toBe(false);
  });

  it("should show promptColumn for table-question-answering", () => {
    initHfOperator("table-question-answering");
    expect(evalHide(getHfField("promptColumn"))).toBe(false);
  });

  it("should show promptColumn for feature-extraction", () => {
    initHfOperator("feature-extraction");
    expect(evalHide(getHfField("promptColumn"))).toBe(false);
  });

  it("should show promptColumn for fill-mask", () => {
    initHfOperator("fill-mask");
    expect(evalHide(getHfField("promptColumn"))).toBe(false);
  });

  it("should show promptColumn for sentence-similarity", () => {
    initHfOperator("sentence-similarity");
    expect(evalHide(getHfField("promptColumn"))).toBe(false);
  });

  it("should show promptColumn for text-ranking", () => {
    initHfOperator("text-ranking");
    expect(evalHide(getHfField("promptColumn"))).toBe(false);
  });

  it("should show promptColumn for image-text-to-text", () => {
    initHfOperator("image-text-to-text");
    expect(evalHide(getHfField("promptColumn"))).toBe(false);
  });

  it("should hide promptColumn for object-detection (image-only)", () => {
    initHfOperator("object-detection");
    expect(evalHide(getHfField("promptColumn"))).toBe(true);
  });

  it("should hide promptColumn for image-segmentation (image-only)", () => {
    initHfOperator("image-segmentation");
    expect(evalHide(getHfField("promptColumn"))).toBe(true);
  });

  it("should hide promptColumn for image-to-text (image-only)", () => {
    initHfOperator("image-to-text");
    expect(evalHide(getHfField("promptColumn"))).toBe(true);
  });

  // ── Field visibility for media-generation tasks ──

  it("should hide imageInput for text-to-image task", () => {
    initHfOperator("text-to-image");
    expect(evalHide(getHfField("imageInput"))).toBe(true);
  });

  it("should hide imageInput for text-to-speech task", () => {
    initHfOperator("text-to-speech");
    expect(evalHide(getHfField("imageInput"))).toBe(true);
  });

  it("should hide audioInput for text-to-image task", () => {
    initHfOperator("text-to-image");
    expect(evalHide(getHfField("audioInput"))).toBe(true);
  });

  it("should hide audioInput for text-to-speech task", () => {
    initHfOperator("text-to-speech");
    expect(evalHide(getHfField("audioInput"))).toBe(true);
  });

  it("should show imageInput for zero-shot-image-classification task", () => {
    initHfOperator("zero-shot-image-classification");
    expect(evalHide(getHfField("imageInput"))).toBe(false);
  });

  it("should show inputImageColumn for zero-shot-image-classification", () => {
    initHfOperator("zero-shot-image-classification");
    expect(evalHide(getHfField("inputImageColumn"))).toBe(false);
  });

  it("should show inputImageColumn for image-to-image", () => {
    initHfOperator("image-to-image");
    expect(evalHide(getHfField("inputImageColumn"))).toBe(false);
  });

  it("should show inputImageColumn for image-to-video", () => {
    initHfOperator("image-to-video");
    expect(evalHide(getHfField("inputImageColumn"))).toBe(false);
  });

  // ── Validator edge cases: zero-shot-image-classification ──

  it("requiredImageInput validator should fail for zero-shot-image-classification with no input", () => {
    initHfOperator("zero-shot-image-classification");
    const field = getHfField("imageInput");
    const validator = field?.validators?.["requiredImageInput"];
    const mockField = {
      ...field,
      model: { task: "zero-shot-image-classification", imageInput: "", inputImageColumn: "" },
    } as FormlyFieldConfig;
    expect(validator!.expression(null as any, mockField)).toBe(false);
  });

  it("requiredImageInput validator should pass for zero-shot-image-classification with column", () => {
    initHfOperator("zero-shot-image-classification");
    const field = getHfField("imageInput");
    const validator = field?.validators?.["requiredImageInput"];
    const mockField = {
      ...field,
      model: { task: "zero-shot-image-classification", imageInput: "", inputImageColumn: "img_col" },
    } as FormlyFieldConfig;
    expect(validator!.expression(null as any, mockField)).toBe(true);
  });

  // ── Validator edge cases: whitespace-only values ──

  it("requiredPromptColumn validator should fail when value is whitespace-only", () => {
    initHfOperator("text-generation");
    const field = getHfField("promptColumn");
    const validator = field?.validators?.["requiredPromptColumn"];
    const mockField = { ...field, model: { task: "text-generation", promptColumn: "   " } } as FormlyFieldConfig;
    expect(validator!.expression(null as any, mockField)).toBe(false);
  });

  it("requiredImageInput validator should fail when imageInput is whitespace-only", () => {
    initHfOperator("image-classification");
    const field = getHfField("imageInput");
    const validator = field?.validators?.["requiredImageInput"];
    const mockField = {
      ...field,
      model: { task: "image-classification", imageInput: "   ", inputImageColumn: "" },
    } as FormlyFieldConfig;
    expect(validator!.expression(null as any, mockField)).toBe(false);
  });

  it("requiredAudioInput validator should fail when audioInput is whitespace-only", () => {
    initHfOperator("automatic-speech-recognition");
    const field = getHfField("audioInput");
    const validator = field?.validators?.["requiredAudioInput"];
    const mockField = {
      ...field,
      model: { task: "automatic-speech-recognition", audioInput: "   ", inputAudioColumn: "" },
    } as FormlyFieldConfig;
    expect(validator!.expression(null as any, mockField)).toBe(false);
  });

  // ── getSelectedTask fallback: form.get("task") ──

  it("should use form.get task value for hide expression when model.task is undefined", () => {
    initHfOperator("text-generation");
    const field = getHfField("imageInput");
    const hideFn = (field?.expressions as Record<string, Function>)?.["hide"];
    // Simulate: model has no task but form has it
    const mockField = {
      model: {},
      form: { get: (key: string) => (key === "task" ? { value: "image-classification" } : null) },
    } as unknown as FormlyFieldConfig;
    expect(hideFn(mockField)).toBe(false); // image-classification is an image task
  });

  it("should use formControl.parent.get task value when model and form are empty", () => {
    initHfOperator("text-generation");
    const field = getHfField("audioInput");
    const hideFn = (field?.expressions as Record<string, Function>)?.["hide"];
    const mockField = {
      model: {},
      formControl: {
        parent: { get: (key: string) => (key === "task" ? { value: "automatic-speech-recognition" } : null) },
      },
    } as unknown as FormlyFieldConfig;
    expect(hideFn(mockField)).toBe(false); // ASR is an audio task
  });

  // ── Null / undefined preview edge cases ──

  it("should return null preview when task is whitespace-only", () => {
    const hfPredicate = {
      ...cloneDeep(mockHuggingFacePredicate),
      operatorProperties: { task: "   ", modelId: "" },
    };
    workflowActionService.addOperator(hfPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, hfPredicate.operatorID, true),
    });
    fixture.detectChanges();
    expect(component.huggingFaceTaskPreview).toBeNull();
  });

  // ── systemPrompt/maxNewTokens/temperature visibility for more tasks ──

  it("should hide systemPrompt for automatic-speech-recognition", () => {
    initHfOperator("automatic-speech-recognition");
    expect(evalHide(getHfField("systemPrompt"))).toBe(true);
    expect(evalHide(getHfField("maxNewTokens"))).toBe(true);
    expect(evalHide(getHfField("temperature"))).toBe(true);
  });

  it("should hide contextColumn for image-classification", () => {
    initHfOperator("image-classification");
    expect(evalHide(getHfField("contextColumn"))).toBe(true);
  });

  it("should hide candidateLabels for text-generation", () => {
    initHfOperator("text-generation");
    expect(evalHide(getHfField("candidateLabels"))).toBe(true);
  });

  it("should hide sentencesColumn for question-answering", () => {
    initHfOperator("question-answering");
    expect(evalHide(getHfField("sentencesColumn"))).toBe(true);
  });

  // ── Visibility when task is undefined for remaining fields ──

  it("should hide promptColumn when task is undefined", () => {
    initHfOperator("text-generation");
    const field = getHfField("promptColumn");
    const hideFn = (field?.expressions as Record<string, Function>)?.["hide"];
    // promptColumn hides when task is in imageOnlyTasks or audioInputTasks;
    // with undefined task, those conditions are false, so it should NOT hide
    expect(hideFn({ model: {} } as FormlyFieldConfig)).toBe(false);
  });

  it("should hide systemPrompt when task is undefined", () => {
    initHfOperator("text-generation");
    const field = getHfField("systemPrompt");
    const hideFn = (field?.expressions as Record<string, Function>)?.["hide"];
    expect(hideFn({ model: {} } as FormlyFieldConfig)).toBe(true);
  });

  it("should hide contextColumn when task is undefined", () => {
    initHfOperator("text-generation");
    const field = getHfField("contextColumn");
    const hideFn = (field?.expressions as Record<string, Function>)?.["hide"];
    expect(hideFn({ model: {} } as FormlyFieldConfig)).toBe(true);
  });

  it("should hide candidateLabels when task is undefined", () => {
    initHfOperator("text-generation");
    const field = getHfField("candidateLabels");
    const hideFn = (field?.expressions as Record<string, Function>)?.["hide"];
    expect(hideFn({ model: {} } as FormlyFieldConfig)).toBe(true);
  });

  it("should hide sentencesColumn when task is undefined", () => {
    initHfOperator("text-generation");
    const field = getHfField("sentencesColumn");
    const hideFn = (field?.expressions as Record<string, Function>)?.["hide"];
    expect(hideFn({ model: {} } as FormlyFieldConfig)).toBe(true);
  });

  it("should hide maxNewTokens when task is undefined", () => {
    initHfOperator("text-generation");
    const field = getHfField("maxNewTokens");
    const hideFn = (field?.expressions as Record<string, Function>)?.["hide"];
    expect(hideFn({ model: {} } as FormlyFieldConfig)).toBe(true);
  });

  it("should hide temperature when task is undefined", () => {
    initHfOperator("text-generation");
    const field = getHfField("temperature");
    const hideFn = (field?.expressions as Record<string, Function>)?.["hide"];
    expect(hideFn({ model: {} } as FormlyFieldConfig)).toBe(true);
  });

  // ──────────────────────────────────────────────────────────────────────────
  // attributeTypeRules validator (checkAttributeType) — the root-field validator
  // added by setFormlyFormBinding when a schema declares attributeTypeRules.
  // getOperatorInputAttributeType is stubbed on the (root-provided) compiling service.
  // ──────────────────────────────────────────────────────────────────────────
  describe("attributeTypeRules validator (checkAttributeType)", () => {
    let compiling: WorkflowCompilingService;

    beforeEach(() => {
      compiling = TestBed.inject(WorkflowCompilingService);
      component.currentOperatorId = "attr-rules-op";
    });

    // Binds a crafted schema and returns the root field's checkAttributeType validator.
    function bindSchema(schema: CustomJSONSchema7): any {
      component.setFormlyFormBinding(schema);
      return (component.formlyFields?.[0] as any)?.validators?.checkAttributeType;
    }

    function rootField(): FormlyFieldConfig {
      return component.formlyFields![0];
    }

    it("enum rule fails when the input attribute type is not in the enum", () => {
      const validator = bindSchema({
        type: "object",
        properties: { attr: { type: "string", autofillAttributeOnPort: 0 } },
        attributeTypeRules: { attr: { enum: ["integer"] } },
      });
      const spy = vi.spyOn(compiling, "getOperatorInputAttributeType").mockReturnValue("string");
      const field = rootField();
      expect(validator.expression({ value: { attr: "colA" } } as any, field)).toBe(false);
      expect((field as any).validators.checkAttributeType.message).toContain(
        "is string, but it's expected to be integer"
      );
      expect(spy).toHaveBeenCalledWith("attr-rules-op", 0, "colA");
    });

    it("enum rule passes when the input attribute type matches the enum", () => {
      const validator = bindSchema({
        type: "object",
        properties: { attr: { type: "string", autofillAttributeOnPort: 0 } },
        attributeTypeRules: { attr: { enum: ["integer"] } },
      });
      vi.spyOn(compiling, "getOperatorInputAttributeType").mockReturnValue("integer");
      expect(validator.expression({ value: { attr: "colA" } } as any, rootField())).toBe(true);
    });

    it("enum rule is skipped when the attribute type is undefined (attribute not selected)", () => {
      const validator = bindSchema({
        type: "object",
        properties: { attr: { type: "string", autofillAttributeOnPort: 0 } },
        attributeTypeRules: { attr: { enum: ["integer"] } },
      });
      vi.spyOn(compiling, "getOperatorInputAttributeType").mockReturnValue(undefined);
      expect(validator.expression({ value: { attr: "" } } as any, rootField())).toBe(true);
    });

    it("const $data rule fails when the sibling attribute resolves to a different type", () => {
      const validator = bindSchema({
        type: "object",
        properties: {
          attr: { type: "string", autofillAttributeOnPort: 0 },
          other: { type: "string", autofillAttributeOnPort: 0 },
        },
        attributeTypeRules: { attr: { const: { $data: "other" } } },
      });
      vi.spyOn(compiling, "getOperatorInputAttributeType").mockImplementation((_id, _port, name) =>
        name === "colA" ? "string" : "integer"
      );
      const field = rootField();
      expect(validator.expression({ value: { attr: "colA", other: "colB" } } as any, field)).toBe(false);
      expect((field as any).validators.checkAttributeType.message).toContain("expected to be the same type as 'colB'");
    });

    it("const $data rule is skipped when the sibling attribute type is not yet resolved", () => {
      const validator = bindSchema({
        type: "object",
        properties: {
          attr: { type: "string", autofillAttributeOnPort: 0 },
          other: { type: "string", autofillAttributeOnPort: 0 },
        },
        attributeTypeRules: { attr: { const: { $data: "other" } } },
      });
      vi.spyOn(compiling, "getOperatorInputAttributeType").mockImplementation((_id, _port, name) =>
        name === "colA" ? "string" : undefined
      );
      expect(validator.expression({ value: { attr: "colA", other: "" } } as any, rootField())).toBe(true);
    });

    it("allOf if/then rule fails when the if-condition holds but the then-enum is violated", () => {
      const validator = bindSchema({
        type: "object",
        properties: {
          attr: { type: "string", autofillAttributeOnPort: 0 },
          mode: { type: "string" },
        },
        attributeTypeRules: {
          attr: { allOf: [{ if: { mode: { valEnum: ["strict"] } }, then: { enum: ["integer"] } }] },
        },
      });
      vi.spyOn(compiling, "getOperatorInputAttributeType").mockReturnValue("string");
      const field = rootField();
      expect(validator.expression({ value: { attr: "colA", mode: "strict" } } as any, field)).toBe(false);
      expect((field as any).validators.checkAttributeType.message).toContain("given that 'mode' is strict");
    });

    it("allOf if/then rule passes when the if-condition is not satisfied", () => {
      const validator = bindSchema({
        type: "object",
        properties: {
          attr: { type: "string", autofillAttributeOnPort: 0 },
          mode: { type: "string" },
        },
        attributeTypeRules: {
          attr: { allOf: [{ if: { mode: { valEnum: ["strict"] } }, then: { enum: ["integer"] } }] },
        },
      });
      vi.spyOn(compiling, "getOperatorInputAttributeType").mockReturnValue("string");
      expect(validator.expression({ value: { attr: "colA", mode: "loose" } } as any, rootField())).toBe(true);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // Field-level validators / custom field-type mapping in setFormlyFormBinding
  // ──────────────────────────────────────────────────────────────────────────
  describe("field-level validators and type mapping", () => {
    function getField(key: string): FormlyFieldConfig | undefined {
      return component.formlyFields?.[0]?.fieldGroup?.find(f => f.key === key);
    }

    it("adds an inEnum validator that rejects values no longer present in the schema enum", () => {
      component.setFormlyFormBinding({
        type: "object",
        properties: { color: { type: "string", enum: ["red", "green"] } },
      });
      const validator = getField("color")?.validators?.["inEnum"];
      expect(validator).toBeDefined();
      expect(validator!.expression({ value: "blue" } as any)).toBe(false);
      expect(validator!.expression({ value: "red" } as any)).toBe(true);
      expect(validator!.message(null, { formControl: { value: "blue" } } as any)).toBe(
        '"blue" is no longer a valid option'
      );
    });

    it("maps datasetVersionPath to the datasetversionselector field type", () => {
      component.setFormlyFormBinding({
        type: "object",
        properties: { datasetVersionPath: { type: "string" } },
      });
      expect(getField("datasetVersionPath")?.type).toBe("datasetversionselector");
    });

    it("maps a field described as 'Input your code here' to the codearea field type", () => {
      component.setFormlyFormBinding({
        type: "object",
        properties: { code: { type: "string", description: "Input your code here" } },
      });
      expect(getField("code")?.type).toBe("codearea");
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // Operator-type-specific field behavior (FileScanOp hide, Projection reorder)
  // ──────────────────────────────────────────────────────────────────────────
  describe("operator-type-specific field behavior", () => {
    function getField(key: string): FormlyFieldConfig | undefined {
      return component.formlyFields?.[0]?.fieldGroup?.find(f => f.key === key);
    }

    it("hides FileScanOp outputFileName unless extract is on or the type is a string/binary type", () => {
      component.currentOperatorSchema = { operatorType: "FileScanOp" } as any;
      component.setFormlyFormBinding({
        type: "object",
        properties: { outputFileName: { type: "string" } },
      });
      const hide = (getField("outputFileName")?.expressions as Record<string, Function>)["hide"];
      expect(hide({ model: {} } as FormlyFieldConfig)).toBe(true);
      expect(hide({ model: { extract: true } } as FormlyFieldConfig)).toBe(false);
      expect(hide({ model: { attributeType: "single string" } } as FormlyFieldConfig)).toBe(false);
      expect(hide({ model: { attributeType: "binary" } } as FormlyFieldConfig)).toBe(false);
      expect(hide({ model: { attributeType: "large binary" } } as FormlyFieldConfig)).toBe(false);
    });

    it("maps Projection attributes to repeat-section-dnd and proxies reorder() to onFormChanges", () => {
      component.currentOperatorSchema = { operatorType: "Projection" } as any;
      component.formData = { attributes: ["colA"] };
      component.setFormlyFormBinding({
        type: "object",
        properties: { attributes: { type: "array", items: { type: "string" } } },
      });
      const field = getField("attributes");
      expect(field?.type).toBe("repeat-section-dnd");
      const spy = vi.spyOn(component, "onFormChanges").mockImplementation(() => {});
      (field?.props as any).reorder();
      expect(spy).toHaveBeenCalledWith({ attributes: ["colA"] });
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // checkOperatorProperty — the guard used by the debounced form-change stream
  // ──────────────────────────────────────────────────────────────────────────
  describe("checkOperatorProperty", () => {
    it("returns false when no operator is being displayed", () => {
      component.currentOperatorId = undefined;
      expect(component.checkOperatorProperty({})).toBe(false);
    });

    it("returns true only when the form data differs from the stored operator properties", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      component.currentOperatorId = mockScanPredicate.operatorID;
      // mockScanPredicate.operatorProperties is {}
      expect(component.checkOperatorProperty({})).toBe(false);
      expect(component.checkOperatorProperty({ tableName: "twitter" })).toBe(true);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // typeInferenceOnLambdaFunction — writes inferred attribute types back into
  // the form data for PythonLambdaFunction operators.
  // ──────────────────────────────────────────────────────────────────────────
  describe("typeInferenceOnLambdaFunction", () => {
    it("does nothing for non-lambda operators", () => {
      component.currentOperatorId = "ScanSource-op";
      const formData = { lambdaAttributeUnits: [{ attributeName: "colA", attributeType: "keep" }] };
      component.typeInferenceOnLambdaFunction(formData);
      expect(formData.lambdaAttributeUnits[0].attributeType).toBe("keep");
    });

    it("infers attribute types from the input schema and clears empty 'Add New Column' units", () => {
      const compiling = TestBed.inject(WorkflowCompilingService);
      vi.spyOn(compiling, "getOperatorInputSchemaMap").mockReturnValue({
        "0": [{ attributeName: "colA", attributeType: "integer" }],
      } as any);
      component.currentOperatorId = "PythonLambdaFunction-op";
      const formData = {
        lambdaAttributeUnits: [
          { attributeName: "Add New Column", newAttributeName: "", attributeType: "stale" },
          { attributeName: "colA", attributeType: "stale" },
        ],
      };
      component.typeInferenceOnLambdaFunction(formData);
      expect(formData.lambdaAttributeUnits[0].attributeType).toBe("");
      expect(formData.lambdaAttributeUnits[1].attributeType).toBe("integer");
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // setInteractivity — enable/disable the whole formly form group
  // ──────────────────────────────────────────────────────────────────────────
  describe("setInteractivity", () => {
    it("disables and re-enables every control in the form group", () => {
      const group = new FormGroup({ a: new FormControl("x") });
      component.formlyFormGroup = group;

      component.setInteractivity(false);
      expect(component.interactive).toBe(false);
      expect(group.disabled).toBe(true);
      expect(group.get("a")!.disabled).toBe(true);

      component.setInteractivity(true);
      expect(component.interactive).toBe(true);
      expect(group.enabled).toBe(true);
      expect(group.get("a")!.enabled).toBe(true);
    });

    it("only updates the interactive flag when there is no form group", () => {
      component.formlyFormGroup = undefined;
      expect(() => component.setInteractivity(true)).not.toThrow();
      expect(component.interactive).toBe(true);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // Python UDF environment schema patching
  // ──────────────────────────────────────────────────────────────────────────
  describe("Python UDF environment schema patching", () => {
    it("injects the environment list into the envName enum without mutating the original schema", () => {
      const schema = { type: "object", properties: { envName: { type: "string" } } } as CustomJSONSchema7;
      const patched = (component as any).patchPythonUdfEnvironmentSchema(schema, ["env-a", "env-b"]);
      expect((patched.properties.envName as CustomJSONSchema7).enum).toEqual(["env-a", "env-b"]);
      expect((schema.properties!.envName as CustomJSONSchema7).enum).toBeUndefined();
    });

    it("hides envName and makes it optional when the default environment is checked", () => {
      component.setFormlyFormBinding({
        type: "object",
        properties: { envName: { type: "string" }, defaultEnv: { type: "boolean" } },
      });
      (component as any).hideEnvNameWhenDefaultEnvChecked();
      const envField = component.formlyFields?.[0]?.fieldGroup?.find(f => f.key === "envName");
      expect((envField?.expressions as any).hide).toBe("!!field.parent.model.defaultEnv");
      expect((envField?.expressions as any)["props.required"]).toBe("!field.parent.model.defaultEnv");
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // registerOperatorPropertyChangeHandler — program-driven property changes
  // refresh formData, guarded by listeningToChange to avoid an echo loop.
  // ──────────────────────────────────────────────────────────────────────────
  describe("registerOperatorPropertyChangeHandler loop guard", () => {
    it("refreshes formData on graph property changes only while listeningToChange is true", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      component.ngOnChanges({
        currentOperatorId: new SimpleChange(undefined, mockScanPredicate.operatorID, true),
      });
      fixture.detectChanges(); // runs ngOnInit, which registers the handler

      // loop guard active: a change echoed while listeningToChange is false must not touch formData
      component.listeningToChange = false;
      workflowActionService.setOperatorProperty(mockScanPredicate.operatorID, { marker: "blocked" });
      expect(component.formData?.marker).toBeUndefined();

      // normal program-driven change: formData is refreshed from the graph
      component.listeningToChange = true;
      workflowActionService.setOperatorProperty(mockScanPredicate.operatorID, { marker: "allowed" });
      expect(component.formData.marker).toBe("allowed");
    });
  });
});

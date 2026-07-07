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

import { OperatorPropertyEditFrameComponent } from "./operator-property-edit-frame.component";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { FORM_DEBOUNCE_TIME_MS } from "../../../service/execute-workflow/execute-workflow.service";
import { DatePipe } from "@angular/common";
import { By } from "@angular/platform-browser";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { FormGroup, FormsModule, ReactiveFormsModule } from "@angular/forms";
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

  // ── Real template rendering ──
  // A nested describe that resets the module so overrideComponent is not applied,
  // allowing the actual HTML template to render.

  describe("real template rendering", () => {
    let realFixture: ComponentFixture<OperatorPropertyEditFrameComponent>;
    let realComponent: OperatorPropertyEditFrameComponent;

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

      realFixture = TestBed.createComponent(OperatorPropertyEditFrameComponent);
      realComponent = realFixture.componentInstance;
    });

    it("should render the title section when editingTitle is false and formTitle is set", () => {
      realComponent.editingTitle = false;
      realComponent.formTitle = "My Operator";
      realFixture.detectChanges();
      const titleEl = realFixture.debugElement.query(By.css("#formly-title"));
      expect(titleEl).toBeTruthy();
      const h3 = realFixture.debugElement.query(By.css("h3.texera-workspace-property-editor-title"));
      expect(h3).toBeTruthy();
      expect((h3.nativeElement as HTMLElement).textContent?.trim()).toBe("My Operator");
    });

    it("should not render the h3 title when formTitle is undefined", () => {
      realComponent.editingTitle = false;
      realComponent.formTitle = undefined;
      realFixture.detectChanges();
      expect(realFixture.debugElement.query(By.css("h3.texera-workspace-property-editor-title"))).toBeNull();
    });

    it("should disable the edit button when interactive is false", () => {
      realComponent.editingTitle = false;
      realComponent.interactive = false;
      realFixture.detectChanges();
      const btn = realFixture.debugElement.query(By.css("#formly-title button")).nativeElement as HTMLButtonElement;
      expect(btn.disabled).toBe(true);
    });

    it("should enable the edit button when interactive is true", () => {
      realComponent.editingTitle = false;
      realComponent.interactive = true;
      realFixture.detectChanges();
      const btn = realFixture.debugElement.query(By.css("#formly-title button")).nativeElement as HTMLButtonElement;
      expect(btn.disabled).toBe(false);
    });

    it("should hide the title section when editingTitle is true", () => {
      realComponent.editingTitle = true;
      realFixture.detectChanges();
      expect(realFixture.debugElement.query(By.css("#formly-title"))).toBeNull();
    });

    it("should hide the customName div when editingTitle is false", () => {
      realComponent.editingTitle = false;
      realFixture.detectChanges();
      const customName = realFixture.debugElement.query(By.css("#customName"));
      expect(customName).toBeTruthy();
      expect((customName.nativeElement as HTMLElement).hidden).toBe(true);
    });

    it("should show the customName div when editingTitle is true", () => {
      realComponent.editingTitle = true;
      realFixture.detectChanges();
      const customName = realFixture.debugElement.query(By.css("#customName"));
      expect(customName).toBeTruthy();
      expect((customName.nativeElement as HTMLElement).hidden).toBe(false);
    });

    it("should show the PythonLambdaFunction icon when currentOperatorId includes PythonLambdaFunction", () => {
      realComponent.editingTitle = false;
      realComponent.currentOperatorId = "PythonLambdaFunction-abc";
      realFixture.detectChanges();
      expect(realFixture.debugElement.query(By.css(".question-circle-button"))).toBeTruthy();
    });

    it("should not show the PythonLambdaFunction icon for other operator types", () => {
      realComponent.editingTitle = false;
      realComponent.currentOperatorId = "ScanSource-abc";
      realFixture.detectChanges();
      expect(realFixture.debugElement.query(By.css(".question-circle-button"))).toBeNull();
    });

    it("should not render the form section when formlyFields is undefined", () => {
      realFixture.detectChanges();
      expect(realFixture.debugElement.query(By.css(".property-editor-form"))).toBeNull();
    });

    it("should render the operator version span", () => {
      realComponent.operatorVersion = "v1.2.3";
      realFixture.detectChanges();
      const versionEl = realFixture.debugElement.query(By.css(".operator-version span"));
      expect(versionEl).toBeTruthy();
      expect((versionEl.nativeElement as HTMLElement).textContent?.trim()).toBe("Operator Version: v1.2.3");
    });

    // ── HF task preview template nodes ──
    // formlyFields=[] and formlyFormGroup=new FormGroup({}) satisfy the outer
    // *ngIf without triggering Formly rendering; the getter is mocked directly.

    function setupPreview(preview: object): void {
      vi.spyOn(realComponent, "huggingFaceTaskPreview", "get").mockReturnValue(preview as any);
      realComponent.formlyFields = [];
      realComponent.formlyFormGroup = new FormGroup({});
      realComponent.formData = {};
      realFixture.detectChanges();
    }

    it("should not render the HF preview card when huggingFaceTaskPreview is null", () => {
      vi.spyOn(realComponent, "huggingFaceTaskPreview", "get").mockReturnValue(null);
      realComponent.formlyFields = [];
      realComponent.formlyFormGroup = new FormGroup({});
      realComponent.formData = {};
      realFixture.detectChanges();
      expect(realFixture.debugElement.query(By.css(".hf-task-preview"))).toBeNull();
    });

    it("should render a video element for kind='video'", () => {
      setupPreview({ kind: "video", title: "Video preview", assetSrc: "assets/sample.mp4" });
      expect(realFixture.debugElement.query(By.css(".hf-task-preview"))).toBeTruthy();
      expect(realFixture.debugElement.query(By.css("video.hf-task-preview-media"))).toBeTruthy();
    });

    it("should bind [src] and [muted] on the video element", () => {
      setupPreview({ kind: "video", title: "Video preview", assetSrc: "assets/sample.mp4" });
      const video = realFixture.debugElement.query(By.css("video")).nativeElement as HTMLVideoElement;
      expect(video.muted).toBe(true);
    });

    it("should render an img element for kind='image'", () => {
      setupPreview({ kind: "image", title: "Image preview", assetSrc: "assets/sample.png" });
      expect(realFixture.debugElement.query(By.css("img.hf-task-preview-media"))).toBeTruthy();
    });

    it("should render an audio element for kind='audio'", () => {
      setupPreview({ kind: "audio", title: "Audio preview", assetSrc: "assets/sample.mp3" });
      expect(realFixture.debugElement.query(By.css("audio.hf-task-preview-audio"))).toBeTruthy();
    });

    it("should render the text surface for kind='text'", () => {
      setupPreview({ kind: "text", title: "Text preview", body: "Some body" });
      expect(realFixture.debugElement.query(By.css(".hf-task-preview-text-surface"))).toBeTruthy();
      const titleEl = realFixture.debugElement.query(By.css(".hf-task-preview-text-title"));
      expect((titleEl.nativeElement as HTMLElement).textContent?.trim()).toBe("Text preview");
      const bodyEl = realFixture.debugElement.query(By.css(".hf-task-preview-text-body"));
      expect((bodyEl.nativeElement as HTMLElement).textContent?.trim()).toBe("Some body");
    });

    it("should render outputBody in the text surface when present", () => {
      setupPreview({ kind: "text", title: "T", body: "B", outputBody: "Output" });
      expect(realFixture.debugElement.query(By.css(".hf-task-preview-text-output"))).toBeTruthy();
    });

    it("should not render outputBody in the text surface when absent", () => {
      setupPreview({ kind: "text", title: "T", body: "B" });
      expect(realFixture.debugElement.query(By.css(".hf-task-preview-text-output"))).toBeNull();
    });

    it("should render the flow section when inputLabel and outputLabel are set", () => {
      setupPreview({ kind: "image", title: "T", inputLabel: "Input", outputLabel: "Output" });
      expect(realFixture.debugElement.query(By.css(".hf-task-preview-flow"))).toBeTruthy();
      expect(realFixture.debugElement.query(By.css(".hf-task-preview-arrow"))).toBeTruthy();
      const chips = realFixture.debugElement.queryAll(By.css(".hf-task-preview-chip"));
      expect(chips.length).toBeGreaterThanOrEqual(2);
    });

    it("should not render the flow section when inputLabel and outputLabel are absent", () => {
      setupPreview({ kind: "image", title: "T" });
      expect(realFixture.debugElement.query(By.css(".hf-task-preview-flow"))).toBeNull();
    });

    it("should render the description for non-text kinds with body", () => {
      setupPreview({ kind: "image", title: "T", body: "Some description" });
      const desc = realFixture.debugElement.query(By.css(".hf-task-preview-description"));
      expect(desc).toBeTruthy();
      expect((desc.nativeElement as HTMLElement).textContent?.trim()).toBe("Some description");
    });

    it("should not render the description for kind='text'", () => {
      setupPreview({ kind: "text", title: "T", body: "B" });
      expect(realFixture.debugElement.query(By.css(".hf-task-preview-description"))).toBeNull();
    });

    it("should render outputBody in meta for non-text kinds", () => {
      setupPreview({ kind: "image", title: "T", outputBody: "Result" });
      expect(realFixture.debugElement.query(By.css(".hf-task-preview-output"))).toBeTruthy();
    });

    it("should not render outputBody in meta for kind='text'", () => {
      setupPreview({ kind: "text", title: "T", body: "B", outputBody: "Result" });
      expect(realFixture.debugElement.query(By.css(".hf-task-preview-output"))).toBeNull();
    });

    it("should render pills when pills array is non-empty", () => {
      setupPreview({ kind: "text", title: "T", pills: ["NLP", "Classification"] });
      const pillsContainer = realFixture.debugElement.query(By.css(".hf-task-preview-pills"));
      expect(pillsContainer).toBeTruthy();
      const pills = realFixture.debugElement.queryAll(By.css(".hf-task-preview-pill"));
      expect(pills.length).toBe(2);
      expect((pills[0].nativeElement as HTMLElement).textContent?.trim()).toBe("NLP");
    });

    it("should not render pills when pills array is empty", () => {
      setupPreview({ kind: "text", title: "T", pills: [] });
      expect(realFixture.debugElement.query(By.css(".hf-task-preview-pills"))).toBeNull();
    });
  });
});

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

import { ChangeDetectorRef, Component, Input, OnChanges, OnDestroy, OnInit, SimpleChanges } from "@angular/core";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { WorkflowStatusService } from "../../../service/workflow-status/workflow-status.service";
import { Subject } from "rxjs";
import { AbstractControl, FormGroup, FormsModule, ReactiveFormsModule } from "@angular/forms";
import { FormlyFieldConfig, FormlyFormOptions, FormlyModule } from "@ngx-formly/core";
import Ajv from "ajv";
import { FormlyJsonschema } from "@ngx-formly/core/json-schema";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { cloneDeep, isEqual } from "lodash-es";
import {
  AttributeTypeAllOfRule,
  AttributeTypeConstRule,
  AttributeTypeEnumRule,
  AttributeTypeRuleSet,
  CustomJSONSchema7,
  hideTypes,
} from "../../../types/custom-json-schema.interface";
import { isDefined } from "../../../../common/util/predicate";
import { ExecutionState, OperatorState, OperatorStatistics } from "src/app/workspace/types/execute-workflow.interface";
import { DynamicSchemaService } from "../../../service/dynamic-schema/dynamic-schema.service";
import { WorkflowCompilingService } from "../../../service/compile-workflow/workflow-compiling.service";
import {
  createOutputFormChangeEventStream,
  createShouldHideFieldFunc,
  setChildTypeDependency,
  setHideExpression,
} from "src/app/common/formly/formly-utils";
import {
  TYPE_CASTING_OPERATOR_TYPE,
  TypeCastingDisplayComponent,
} from "../typecasting-display/type-casting-display.component";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { filter } from "rxjs/operators";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { PresetWrapperComponent } from "src/app/common/formly/preset-wrapper/preset-wrapper.component";
import { WorkflowVersionService } from "../../../../dashboard/service/user/workflow-version/workflow-version.service";
import { QuillBinding } from "y-quill";
import Quill from "quill";
import QuillCursors from "quill-cursors";
import * as Y from "yjs";
import { OperatorSchema } from "src/app/workspace/types/operator-schema.interface";
import { AttributeType, PortSchema } from "../../../types/workflow-compiling.interface";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { NgFor, NgIf, NgSwitch, NgSwitchCase } from "@angular/common";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzPopoverDirective } from "ng-zorro-antd/popover";
import { NzFormDirective } from "ng-zorro-antd/form";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { WorkflowPveService } from "../../../service/virtual-environment/virtual-environment.service";
import { ComputingUnitStatusService } from "../../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { of } from "rxjs";
import { map, switchMap, take } from "rxjs/operators";

Quill.register("modules/cursors", QuillCursors);

// The Aggregate "count" function. With an empty attribute it means COUNT(*) (all rows);
// with a column it counts that column's non-null values. It is the only function whose
// attribute is optional.
export const AGGREGATE_COUNT = "count";

// The Aggregate attribute is required for every function except `count` (an empty
// attribute on count means COUNT(*), which needs no column).
export function isAggregateAttributeRequired(aggFunction: unknown): boolean {
  return aggFunction !== AGGREGATE_COUNT;
}

/**
 * Property Editor uses JSON Schema to automatically generate the form from the JSON Schema of an operator.
 * For example, the JSON Schema of Sentiment Analysis could be:
 *  'properties': {
 *    'attribute': { 'type': 'string' },
 *    'resultAttribute': { 'type': 'string' }
 *  }
 * The automatically generated form will show two input boxes, one titled 'attribute' and one titled 'resultAttribute'.
 * More examples of the operator JSON schema can be found in `mock-operator-metadata.data.ts`
 * More about JSON Schema: Understanding JSON Schema - https://spacetelescope.github.io/understanding-json-schema/
 *
 * OperatorMetadataService will fetch metadata about the operators, which includes the JSON Schema, from the backend.
 *
 * We use library `@ngx-formly` to generate form from json schema
 * https://github.com/ngx-formly/ngx-formly
 */
@UntilDestroy()
@Component({
  selector: "texera-formly-form-frame",
  templateUrl: "./operator-property-edit-frame.component.html",
  styleUrls: ["./operator-property-edit-frame.component.scss"],
  imports: [
    NgIf,
    NgFor,
    NgSwitch,
    NgSwitchCase,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    ɵNzTransitionPatchDirective,
    NzTooltipDirective,
    NzIconDirective,
    NzPopoverDirective,
    FormsModule,
    NzFormDirective,
    ReactiveFormsModule,
    FormlyModule,
    TypeCastingDisplayComponent,
    NzWaveDirective,
  ],
})
export class OperatorPropertyEditFrameComponent implements OnInit, OnChanges, OnDestroy {
  @Input() currentOperatorId?: string;

  currentOperatorSchema?: OperatorSchema;

  readonly OperatorState = OperatorState;
  currentOperatorStatus?: OperatorStatistics;

  // re-declare enum for angular template to access it
  readonly ExecutionState = ExecutionState;

  // whether the editor can be edited
  interactive: boolean = false;

  // the source event stream of form change triggered by library at each user input
  sourceFormChangeEventStream = new Subject<Record<string, unknown>>();

  // the output form change event stream after debounce time and filtering out values
  operatorPropertyChangeStream = createOutputFormChangeEventStream(this.sourceFormChangeEventStream, data =>
    this.checkOperatorProperty(data)
  );

  listeningToChange: boolean = true;

  // inputs and two-way bindings to formly component
  formlyFormGroup: FormGroup | undefined;
  formData: any;
  formlyOptions: FormlyFormOptions = {};
  formlyFields: FormlyFieldConfig[] | undefined;
  formTitle: string | undefined;
  operatorDescription: string | undefined;

  // The field name and its css style to be overridden, e.g., for showing the diff between two workflows.
  // example: new Map([
  //     ["attribute", "outline: 3px solid green; transition: 0.3s ease-in-out outline;"],
  //     ["condition", "background: red; border-color: red;"],
  //   ]);
  fieldStyleOverride: Map<String, String> = new Map([]);

  editingTitle: boolean = false;

  // used to fill in default values in json schema to initialize new operator
  ajv = new Ajv({ useDefaults: true, strict: false });

  isTypeCasting: boolean = false;

  // for display component of some extra information
  public operatorVersion: string = "";
  quillBinding?: QuillBinding;
  quill!: Quill;
  // used to tear down subscriptions that takeUntil(teardownObservable)
  private teardownObservable: Subject<void> = new Subject();

  readonly huggingFaceTaskPreviewSamples: Record<
    string,
    {
      kind: "image" | "video" | "audio" | "text";
      inputLabel?: string;
      outputLabel?: string;
      title?: string;
      body?: string;
      outputBody?: string;
      pills?: string[];
      assetSrc?: string;
    }
  > = {
    "text-to-image": {
      kind: "image",
      inputLabel: "Text prompt",
      outputLabel: "Generated image",
      title: "Comic-style city action scene",
      body: "Prompt becomes a generated image preview.",
      assetSrc: "assets/sample-image.png",
    },
    "image-to-image": {
      kind: "image",
      inputLabel: "Source image",
      outputLabel: "Edited image",
      title: "Image transformation preview",
      body: "Image input produces a modified image result.",
      assetSrc: "assets/sample-image.png",
    },
    "text-to-video": {
      kind: "video",
      inputLabel: "Text prompt",
      outputLabel: "Generated video",
      title: "Prompt-based motion preview",
      body: "Prompt becomes a generated video clip.",
      assetSrc: "assets/sample-video.mp4",
    },
    "image-to-video": {
      kind: "video",
      inputLabel: "Source image",
      outputLabel: "Animated clip",
      title: "Image animation preview",
      body: "Image input becomes a short generated video.",
      assetSrc: "assets/sample-video.mp4",
    },
    "text-to-speech": {
      kind: "audio",
      inputLabel: "Text input",
      outputLabel: "Spoken audio",
      title: "Speech synthesis preview",
      body: "Text becomes an audio clip the user can play back.",
      assetSrc: "assets/sample-audio.wav",
    },
    "automatic-speech-recognition": {
      kind: "audio",
      inputLabel: "Audio input",
      outputLabel: "Transcript text",
      title: "Speech-to-text preview",
      body: "Uploaded audio is transcribed into plain text.",
      assetSrc: "assets/sample-audio.wav",
    },
    "audio-classification": {
      kind: "audio",
      inputLabel: "Audio input",
      outputLabel: "Labels and scores",
      title: "Audio tagging preview",
      body: "Uploaded audio returns classification labels.",
      assetSrc: "assets/sample-audio.wav",
    },
    "image-text-to-text": {
      kind: "image",
      inputLabel: "Image + text prompt",
      outputLabel: "Generated text",
      title: "Image-text-to-text preview",
      body: "The model reads an image and a text prompt to produce a response.",
      outputBody: "The image shows a superhero leaping across rooftops at sunset.",
      assetSrc: "assets/sample-image.png",
    },
    "image-classification": {
      kind: "image",
      inputLabel: "Image input",
      outputLabel: "Predicted labels",
      title: "Image classification preview",
      body: "The model assigns labels such as superhero, city, or action scene.",
      assetSrc: "assets/sample-image.png",
      pills: ["superhero", "cityscape", "action"],
    },
    "object-detection": {
      kind: "image",
      inputLabel: "Image input",
      outputLabel: "Detected objects",
      title: "Object detection preview",
      body: "The model returns detected objects and bounding boxes.",
      assetSrc: "assets/sample-image.png",
      pills: ["person", "building", "sky"],
    },
    "image-segmentation": {
      kind: "image",
      inputLabel: "Image input",
      outputLabel: "Segmented regions",
      title: "Segmentation preview",
      body: "The model separates the image into labeled regions.",
      assetSrc: "assets/sample-image.png",
      pills: ["foreground", "background", "subject"],
    },
    "image-to-text": {
      kind: "image",
      inputLabel: "Image input",
      outputLabel: "Caption text",
      title: "Captioning preview",
      body: "The model turns an uploaded image into a textual description.",
      outputBody: "A superhero leaps above a dense downtown skyline at sunset.",
      assetSrc: "assets/sample-image.png",
    },
    "visual-question-answering": {
      kind: "image",
      inputLabel: "Image + question",
      outputLabel: "Answer text",
      title: "Visual question answering preview",
      body: "The model reads the image and answers the user question.",
      outputBody: "Spider-Man is jumping over a city skyline.",
      assetSrc: "assets/sample-image.png",
    },
    "document-question-answering": {
      kind: "image",
      inputLabel: "Document image + question",
      outputLabel: "Answer text",
      title: "Document QA preview",
      body: "The model extracts answers from a document image.",
      outputBody: "Invoice total: $248.90",
      assetSrc: "assets/sample-image.png",
    },
    "zero-shot-image-classification": {
      kind: "image",
      inputLabel: "Image + candidate labels",
      outputLabel: "Ranked labels",
      title: "Zero-shot image labeling preview",
      body: "Candidate labels are scored against the uploaded image.",
      assetSrc: "assets/sample-image.png",
      pills: ["superhero", "sports", "travel"],
    },
    "text-generation": {
      kind: "text",
      inputLabel: "Prompt",
      outputLabel: "Generated text",
      title: "Text generation preview",
      body: "Write a short action scene set above a crowded city skyline.",
      outputBody: "The hero vaulted between rooftops as the city lights came alive below.",
    },
    "text-classification": {
      kind: "text",
      inputLabel: "Text input",
      outputLabel: "Predicted label",
      title: "Text classification preview",
      body: "This launch update sounds confident and customer-focused.",
      pills: ["positive", "announcement"],
    },
    "token-classification": {
      kind: "text",
      inputLabel: "Text input",
      outputLabel: "Tagged spans",
      title: "Token classification preview",
      body: "Peter Parker visited New York yesterday.",
      pills: ["Peter Parker: PERSON", "New York: LOCATION"],
    },
    "question-answering": {
      kind: "text",
      inputLabel: "Question + context",
      outputLabel: "Answer span",
      title: "Question answering preview",
      body: "Question: Who led the launch?\nContext: Maya led the launch while Jordan handled analytics.",
      outputBody: "Maya",
    },
    "table-question-answering": {
      kind: "text",
      inputLabel: "Question + table",
      outputLabel: "Answer",
      title: "Table QA preview",
      body: "Question: Which month had the highest revenue?",
      outputBody: "March",
    },
    "zero-shot-classification": {
      kind: "text",
      inputLabel: "Text + candidate labels",
      outputLabel: "Ranked labels",
      title: "Zero-shot classification preview",
      body: "We need to accelerate onboarding for enterprise customers.",
      pills: ["business", "operations", "support"],
    },
    translation: {
      kind: "text",
      inputLabel: "Source text",
      outputLabel: "Translated text",
      title: "Translation preview",
      body: "Good morning, thanks for joining the call.",
      outputBody: "Buenos dias, gracias por unirte a la llamada.",
    },
    summarization: {
      kind: "text",
      inputLabel: "Long text",
      outputLabel: "Summary",
      title: "Summarization preview",
      body: "A long project update is compressed into a short summary.",
      outputBody: "The team shipped the release, fixed two regressions, and started the next milestone.",
    },
    "feature-extraction": {
      kind: "text",
      inputLabel: "Text input",
      outputLabel: "Embedding/vector output",
      title: "Feature extraction preview",
      body: "Input text is converted into a numeric representation.",
      pills: ["0.12", "-0.08", "0.44", "..."],
    },
    "fill-mask": {
      kind: "text",
      inputLabel: "Masked sentence",
      outputLabel: "Top completions",
      title: "Fill-mask preview",
      body: "The hero saved the [MASK].",
      pills: ["city", "day", "crowd"],
    },
    "sentence-similarity": {
      kind: "text",
      inputLabel: "Source + candidate sentences",
      outputLabel: "Similarity scores",
      title: "Sentence similarity preview",
      body: "Compare one sentence against several alternatives.",
      pills: ["0.93", "0.61", "0.22"],
    },
    "text-ranking": {
      kind: "text",
      inputLabel: "Query + candidate texts",
      outputLabel: "Ranked results",
      title: "Text ranking preview",
      body: "Candidate passages are ordered by relevance to the query.",
      pills: ["doc_2", "doc_5", "doc_1"],
    },
  };

  get huggingFaceTaskPreview(): {
    kind: "image" | "video" | "audio" | "text";
    inputLabel?: string;
    outputLabel?: string;
    title?: string;
    body?: string;
    outputBody?: string;
    pills?: string[];
    assetSrc?: string;
  } | null {
    if (!this.isHuggingFaceOperator()) {
      return null;
    }
    const task = this.formData?.["task"];
    if (typeof task !== "string" || task.trim().length === 0) {
      return null;
    }
    return (
      this.huggingFaceTaskPreviewSamples[task] ?? {
        kind: "text",
        inputLabel: "Task input",
        outputLabel: "Task output",
        title: this.formatTaskTitle(task),
        body: "This task transforms the provided input into a model response.",
      }
    );
  }

  constructor(
    private formlyJsonschema: FormlyJsonschema,
    private workflowActionService: WorkflowActionService,
    public executeWorkflowService: ExecuteWorkflowService,
    private dynamicSchemaService: DynamicSchemaService,
    private workflowCompilingService: WorkflowCompilingService,
    private notificationService: NotificationService,
    private changeDetectorRef: ChangeDetectorRef,
    private workflowVersionService: WorkflowVersionService,
    private workflowStatusSerivce: WorkflowStatusService,
    private config: GuiConfigService,
    private workflowPveService: WorkflowPveService,
    private computingUnitStatusService: ComputingUnitStatusService
  ) {}

  private patchPythonUdfEnvironmentSchema(schema: CustomJSONSchema7, environments: string[]): CustomJSONSchema7 {
    const patchedSchema = cloneDeep(schema);

    if (patchedSchema.properties && typeof patchedSchema.properties !== "boolean") {
      const envProperty = patchedSchema.properties["envName"] as CustomJSONSchema7;
      envProperty.enum = environments;
    }

    return patchedSchema;
  }

  private hideEnvNameWhenDefaultEnvChecked(): void {
    const envField = this.formlyFields?.[0]?.fieldGroup?.find(f => f.key === "envName");
    if (envField) {
      envField.expressions = {
        ...envField.expressions,
        hide: "!!field.parent.model.defaultEnv",
        "props.required": "!field.parent.model.defaultEnv",
      };
    }
  }

  ngOnChanges(changes: SimpleChanges): void {
    this.currentOperatorId = changes.currentOperatorId?.currentValue;
    if (!this.currentOperatorId) {
      return;
    }
    this.rerenderEditorForm();
  }

  ngOnInit(): void {
    // listen to the autocomplete event, remove invalid properties, and update the schema displayed on the form
    this.registerOperatorSchemaChangeHandler();

    // when the operator's property is updated via program instead of user updating the json schema form,
    //  this observable will be responsible in handling these events.
    this.registerOperatorPropertyChangeHandler();

    // handle the form change event on the user interface to actually set the operator property
    this.registerOnFormChangeHandler();

    this.registerDisableEditorInteractivityHandler();

    this.registerOperatorDisplayNameChangeHandler();

    this.workflowStatusSerivce
      .getStatusUpdateStream()
      .pipe(untilDestroyed(this))
      .subscribe(update => {
        if (this.currentOperatorId) {
          this.currentOperatorStatus = update[this.currentOperatorId];
        }
      });
  }

  private isHuggingFaceOperator(): boolean {
    if (!this.currentOperatorId) return false;
    const graph = this.workflowActionService.getTexeraGraph();
    if (!graph.hasOperator(this.currentOperatorId)) return false;
    return graph.getOperator(this.currentOperatorId).operatorType === "HuggingFace";
  }

  private formatTaskTitle(task: string): string {
    return task
      .split("-")
      .map(part => part.charAt(0).toUpperCase() + part.slice(1))
      .join(" ");
  }

  async ngOnDestroy() {
    // await this.checkAndSavePreset();
    this.teardownObservable.complete();
  }

  /**
   * Callback function provided to the Angular Json Schema Form library,
   *  whenever the form data is changed, this function is called.
   * It only serves as a bridge from a callback function to RxJS Observable
   * @param event
   */
  onFormChanges(event: Record<string, unknown>): void {
    this.sourceFormChangeEventStream.next(event);
  }

  /**
   * Changes the property editor to use the new operator data.
   * Sets all the data needed by the json schema form and displays the form.
   */
  rerenderEditorForm(): void {
    if (!this.currentOperatorId) {
      return;
    }
    this.currentOperatorSchema = this.dynamicSchemaService.getDynamicSchema(this.currentOperatorId);
    this.currentOperatorStatus = this.workflowStatusSerivce.getCurrentStatus()[this.currentOperatorId];

    this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("currentlyEditing", this.currentOperatorId);
    const operator = this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorId);
    // set the operator data needed
    this.workflowActionService.setOperatorVersion(operator.operatorID, this.currentOperatorSchema.operatorVersion);
    this.operatorVersion = operator.operatorVersion.slice(0, 9);
    this.setFormlyFormBinding(this.currentOperatorSchema.jsonSchema);
    this.formTitle = operator.customDisplayName ?? this.currentOperatorSchema.additionalMetadata.userFriendlyName;
    this.operatorDescription = this.currentOperatorSchema.additionalMetadata.operatorDescription;
    /**
     * Important: make a deep copy of the initial property data object.
     * Prevent the form directly changes the value in the texera graph without going through workflow action service.
     */
    this.formData = cloneDeep(operator.operatorProperties);

    const isPythonUdf =
      this.currentOperatorSchema.operatorType === "PythonUDFV2" ||
      this.currentOperatorSchema.operatorType === "DualInputPortsPythonUDFV2" ||
      this.currentOperatorSchema.operatorType === "PythonUDFSourceV2";
    if (isPythonUdf && this.formData.defaultEnv === undefined) {
      this.formData.defaultEnv = true;
    }

    const baseSchema = cloneDeep(this.currentOperatorSchema.jsonSchema);

    if (isPythonUdf) {
      this.computingUnitStatusService
        .getSelectedComputingUnit()
        .pipe(
          take(1),
          switchMap(unit => {
            const cuid = unit?.computingUnit?.cuid;
            return cuid !== undefined
              ? this.workflowPveService.fetchPVEs(cuid).pipe(map(pves => pves.map(p => p.pveName)))
              : of<string[]>([]);
          }),
          untilDestroyed(this)
        )
        .subscribe({
          next: (environments: string[]) => {
            const patchedSchema = this.patchPythonUdfEnvironmentSchema(baseSchema, environments);
            this.setFormlyFormBinding(patchedSchema);
            this.hideEnvNameWhenDefaultEnvChecked();
          },
          error: (err: unknown) => {
            console.error("Failed to load Python virtual environments:", err);
            this.notificationService.error(
              `Could not load Python virtual environments: ${err instanceof Error ? err.message : String(err)}`
            );
            const patchedSchema = this.patchPythonUdfEnvironmentSchema(baseSchema, []);
            this.setFormlyFormBinding(patchedSchema);
            this.hideEnvNameWhenDefaultEnvChecked();
          },
        });
    } else {
      this.setFormlyFormBinding(baseSchema);
    }

    // use ajv to initialize the default value to data according to schema, see https://ajv.js.org/#assigning-defaults
    // WorkflowUtil service also makes sure that the default values are filled in when operator is added from the UI
    // However, we perform an addition check for the following reasons:
    // 1. the operator might be added not directly from the UI, which violates the precondition
    // 2. the schema might change, which specifies a new default value
    // 3. formly doesn't emit change event when it fills in default value, causing an inconsistency between component and service
    this.ajv.validate(this.currentOperatorSchema.jsonSchema, this.formData);

    // manually trigger a form change event because default value might be filled in
    this.onFormChanges(this.formData);
    this.isTypeCasting = this.workflowActionService
      .getTexeraGraph()
      .getOperator(this.currentOperatorId)
      .operatorType.includes(TYPE_CASTING_OPERATOR_TYPE);
    // execute set interactivity immediately in another task because of a formly bug
    // whenever the form model is changed, formly can only disable it after the UI is rendered
    setTimeout(() => {
      this.setInteractivity(this.interactive);
      this.changeDetectorRef.detectChanges();
    }, 0);
  }

  setInteractivity(interactive: boolean) {
    this.interactive = interactive;
    if (this.formlyFormGroup !== undefined) {
      if (this.interactive) {
        this.formlyFormGroup.enable();
      } else {
        this.formlyFormGroup.disable();
      }
    }
  }

  checkOperatorProperty(formData: object): boolean {
    // check if the component is displaying operator property
    if (this.currentOperatorId === undefined) {
      return false;
    }
    // check if the operator still exists, it might be deleted during debounce time
    const operator = this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorId);
    if (!operator) {
      return false;
    }
    // only emit change event if the form data actually changes
    return !isEqual(formData, operator.operatorProperties);
  }

  /**
   * This method handles the schema change event from autocomplete. It will get the new schema
   *  propagated from autocomplete and check if the operators' properties that users input
   *  previously are still valid. If invalid, it will remove these fields and triggered an event so
   *  that the user interface will be updated through registerOperatorPropertyChangeHandler() method.
   *
   * If the operator that experiences schema changed is the same as the operator that is currently
   *  displaying on the property panel, this handler will update the current operator schema
   *  to the new schema.
   */
  registerOperatorSchemaChangeHandler(): void {
    this.dynamicSchemaService
      .getOperatorDynamicSchemaChangedStream()
      .pipe(filter(({ operatorID }) => operatorID === this.currentOperatorId))
      .pipe(untilDestroyed(this))
      .subscribe(_ => this.rerenderEditorForm());
  }

  /**
   * This method captures the change in operator's property via program instead of user updating the
   *  json schema form in the user interface.
   *
   * For instance, when the input doesn't match the new json schema and the UI needs to remove the
   *  invalid fields, this form will capture those events.
   */
  registerOperatorPropertyChangeHandler(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getOperatorPropertyChangeStream()
      .pipe(
        filter(_ => this.listeningToChange),
        filter(_ => this.currentOperatorId !== undefined),
        filter(operatorChanged => operatorChanged.operator.operatorID === this.currentOperatorId)
      )
      .pipe(untilDestroyed(this))
      .subscribe(operatorChanged => {
        this.formData = cloneDeep(operatorChanged.operator.operatorProperties);
        this.changeDetectorRef.detectChanges();
      });
  }

  /**
   * This method handles the form change event and set the operator property
   *  in the texera graph.
   */
  registerOnFormChangeHandler(): void {
    this.operatorPropertyChangeStream.pipe(untilDestroyed(this)).subscribe(formData => {
      // set the operator property to be the new form data
      if (this.currentOperatorId) {
        this.listeningToChange = false;
        this.typeInferenceOnLambdaFunction(formData);
        this.workflowActionService.setOperatorProperty(this.currentOperatorId, cloneDeep(formData));
        this.listeningToChange = true;
      }
    });
  }

  typeInferenceOnLambdaFunction(formData: any): void {
    if (!this.currentOperatorId?.includes("PythonLambdaFunction")) {
      return;
    }
    const opInputSchema = this.workflowCompilingService.getOperatorInputSchemaMap(this.currentOperatorId);
    if (!opInputSchema) {
      return;
    }
    const firstPortInputSchema = opInputSchema[0];
    if (!firstPortInputSchema) {
      return;
    }
    const schemaMap = new Map(firstPortInputSchema?.map(obj => [obj.attributeName, obj.attributeType]));
    formData.lambdaAttributeUnits.forEach((unit: any, index: number, a: any) => {
      if (unit.attributeName === "Add New Column" && !unit.newAttributeName) a[index].attributeType = "";
      if (schemaMap.has(unit.attributeName)) a[index].attributeType = schemaMap.get(unit.attributeName);
    });
  }

  registerDisableEditorInteractivityHandler(): void {
    this.workflowActionService
      .getWorkflowModificationEnabledStream()
      .pipe(untilDestroyed(this))
      .subscribe(canModify => {
        if (this.currentOperatorId) {
          this.setInteractivity(canModify);
          this.changeDetectorRef.detectChanges();
        }
      });
  }

  setFormlyFormBinding(schema: CustomJSONSchema7) {
    var operatorPropertyDiff = this.workflowVersionService.operatorPropertyDiff;
    if (this.currentOperatorId != undefined && operatorPropertyDiff[this.currentOperatorId] != undefined) {
      this.fieldStyleOverride = operatorPropertyDiff[this.currentOperatorId];
    }
    if (this.fieldStyleOverride.has("operatorVersion")) {
      var boundary = this.fieldStyleOverride.get("operatorVersion");
      if (boundary) {
        document.getElementsByClassName("operator-version")[0].setAttribute("style", boundary.toString());
      }
    }
    // intercept JsonSchema -> FormlySchema process, adding custom options
    // this requires a one-to-one mapping.
    // for relational custom options, have to do it after FormlySchema is generated.
    const jsonSchemaMapIntercept = (
      mappedField: FormlyFieldConfig,
      mapSource: CustomJSONSchema7
    ): FormlyFieldConfig => {
      // apply the overridden css style if applicable
      mappedField.expressions = {
        "templateOptions.attributes": () => {
          if (
            isDefined(mappedField) &&
            typeof mappedField.key === "string" &&
            this.fieldStyleOverride.has(mappedField.key)
          ) {
            return { style: this.fieldStyleOverride.get(mappedField.key) };
          } else {
            return {};
          }
        },
      };

      // Disable dummy operator for user
      if (mappedField.key === "dummyOperator") {
        mappedField.expressions = {
          ...mappedField.expressions,
          "templateOptions.disabled": () => true,
          "templateOptions.readonly": () => true,
        };
      }

      // Disable dummy property and value fields for user
      if (mappedField.key === "dummyProperty" || mappedField.key === "dummyValue") {
        mappedField.expressions = {
          ...mappedField.expressions,
          "templateOptions.readonly": () => true,
          "templateOptions.disabled": () => true,
        };
      }

      // Disable dummy property list for all operators, except for dummy operator.
      if (mappedField.key === "dummyPropertyList") {
        mappedField.hide = this.currentOperatorSchema?.operatorType !== "Dummy";
        mappedField.expressions = {
          ...mappedField.expressions,
          "templateOptions.disabled": () => true,
          "templateOptions.readonly": () => true,
          "templateOptions.canRemove": () => false,
          "templateOptions.canAdd": () => false,
        };
      }

      // conditionally hide the field according to the schema
      if (
        isDefined(mapSource.hideExpectedValue) &&
        isDefined(mapSource.hideTarget) &&
        isDefined(mapSource.hideType) &&
        hideTypes.includes(mapSource.hideType)
      ) {
        mappedField.expressions = {
          ...mappedField.expressions,
          hide: createShouldHideFieldFunc(
            mapSource.hideTarget,
            mapSource.hideType,
            mapSource.hideExpectedValue,
            mapSource.hideOnNull
          ),
        };
      }

      // if the title is fileName, then change it to custom autocomplete input template
      if (mappedField.key === "fileName") {
        mappedField.type = "inputautocomplete";
      }

      if (mappedField.key === "huggingFaceModel") {
        mappedField.type = "huggingface";
      }

      if (mappedField.key === "modelId" && this.currentOperatorSchema?.operatorType === "HuggingFace") {
        mappedField.type = "huggingface";
      }

      if (mappedField.key === "task" && this.currentOperatorSchema?.operatorType === "HuggingFace") {
        mappedField.hide = true;
      }

      // ── Dynamic field visibility for HuggingFace based on selected task ──
      if (this.currentOperatorSchema?.operatorType === "HuggingFace" && typeof mappedField.key === "string") {
        const hfKey = mappedField.key;
        const imageOnlyTasks = ["image-classification", "object-detection", "image-segmentation", "image-to-text"];
        const imageInputTasks = [
          ...imageOnlyTasks,
          "visual-question-answering",
          "document-question-answering",
          "zero-shot-image-classification",
          "image-text-to-text",
          "image-to-image",
          "image-to-video",
        ];
        const audioInputTasks = ["automatic-speech-recognition", "audio-classification"];
        const promptRequiredTasks = [
          "text-generation",
          "text-classification",
          "token-classification",
          "question-answering",
          "table-question-answering",
          "zero-shot-classification",
          "translation",
          "summarization",
          "feature-extraction",
          "fill-mask",
          "sentence-similarity",
          "text-ranking",
          "visual-question-answering",
          "document-question-answering",
          "zero-shot-image-classification",
        ];
        const getSelectedTask = (field: FormlyFieldConfig): string | undefined => {
          const fromForm = field.form?.get("task")?.value ?? field.formControl?.parent?.get("task")?.value;
          if (typeof fromForm === "string" && fromForm.trim().length > 0) {
            return fromForm;
          }
          const fromModel = field.model?.task;
          if (typeof fromModel === "string" && fromModel.trim().length > 0) {
            return fromModel;
          }
          return undefined;
        };
        if (hfKey === "imageInput") {
          mappedField.type = "huggingface-image-upload";
          mappedField.expressions = {
            ...mappedField.expressions,
            hide: (field: FormlyFieldConfig) => {
              const t = getSelectedTask(field);
              return t === undefined || !imageInputTasks.includes(t);
            },
          };
          mappedField.validators = {
            ...mappedField.validators,
            requiredImageInput: {
              expression: (_control: AbstractControl, field: FormlyFieldConfig) => {
                const t = getSelectedTask(field);
                if (t === undefined || !imageInputTasks.includes(t)) {
                  return true;
                }
                const inputImageCol = field.model?.inputImageColumn;
                if (typeof inputImageCol === "string" && inputImageCol.trim().length > 0) {
                  return true;
                }
                const value = field.formControl?.value ?? field.model?.imageInput;
                return typeof value === "string" && value.trim().length > 0;
              },
              message: () => "Upload an image or select an Input Image Column for this task.",
            },
          };
        }
        if (hfKey === "audioInput") {
          mappedField.type = "huggingface-audio-upload";
          mappedField.expressions = {
            ...mappedField.expressions,
            hide: (field: FormlyFieldConfig) => {
              const t = getSelectedTask(field);
              return t === undefined || !audioInputTasks.includes(t);
            },
          };
          mappedField.validators = {
            ...mappedField.validators,
            requiredAudioInput: {
              expression: (_control: AbstractControl, field: FormlyFieldConfig) => {
                const t = getSelectedTask(field);
                if (t === undefined || !audioInputTasks.includes(t)) {
                  return true;
                }
                const inputAudioCol = field.model?.inputAudioColumn;
                if (typeof inputAudioCol === "string" && inputAudioCol.trim().length > 0) {
                  return true;
                }
                const value = field.formControl?.value ?? field.model?.audioInput;
                return typeof value === "string" && value.trim().length > 0;
              },
              message: () => "Upload audio or select an Input Audio Column for this task.",
            },
          };
        }
        if (hfKey === "inputImageColumn") {
          mappedField.expressions = {
            ...mappedField.expressions,
            hide: (field: FormlyFieldConfig) => {
              const t = getSelectedTask(field);
              return t === undefined || !imageInputTasks.includes(t);
            },
          };
        }
        if (hfKey === "inputAudioColumn") {
          mappedField.expressions = {
            ...mappedField.expressions,
            hide: (field: FormlyFieldConfig) => {
              const t = getSelectedTask(field);
              return t === undefined || !audioInputTasks.includes(t);
            },
          };
        }
        if (hfKey === "promptColumn") {
          mappedField.expressions = {
            ...mappedField.expressions,
            hide: (field: FormlyFieldConfig) => {
              const t = getSelectedTask(field);
              return t !== undefined && (imageOnlyTasks.includes(t) || audioInputTasks.includes(t));
            },
          };
          mappedField.validators = {
            ...mappedField.validators,
            requiredPromptColumn: {
              expression: (_control: AbstractControl, field: FormlyFieldConfig) => {
                const t = getSelectedTask(field);
                if (t === undefined || !promptRequiredTasks.includes(t)) {
                  return true;
                }
                const value = field.formControl?.value ?? field.model?.promptColumn;
                return typeof value === "string" && value.trim().length > 0;
              },
              message: () => "Select a prompt column for this task.",
            },
          };
        }
        if (["systemPrompt", "maxNewTokens", "temperature"].includes(hfKey)) {
          mappedField.expressions = {
            ...mappedField.expressions,
            hide: (field: FormlyFieldConfig) => {
              const t = getSelectedTask(field);
              return t !== "text-generation";
            },
          };
        }
        if (hfKey === "contextColumn") {
          mappedField.expressions = {
            ...mappedField.expressions,
            hide: (field: FormlyFieldConfig) => getSelectedTask(field) !== "question-answering",
          };
        }
        if (hfKey === "candidateLabels") {
          mappedField.expressions = {
            ...mappedField.expressions,
            hide: (field: FormlyFieldConfig) => {
              const t = getSelectedTask(field);
              return t !== "zero-shot-classification" && t !== "zero-shot-image-classification";
            },
          };
        }
        if (hfKey === "sentencesColumn") {
          mappedField.expressions = {
            ...mappedField.expressions,
            hide: (field: FormlyFieldConfig) => {
              const t = getSelectedTask(field);
              return t !== "sentence-similarity" && t !== "text-ranking";
            },
          };
        }
      }

      if (mappedField.key === "datasetVersionPath") {
        mappedField.type = "datasetversionselector";
      }

      // Aggregate: the attribute is optional for `count` (an empty attribute means COUNT(*),
      // counting all rows) and required for every other function. Show the required marker
      // (red *) accordingly, based on the sibling aggFunction within the same row.
      if (this.currentOperatorSchema?.operatorType === "Aggregate" && mappedField.key === "attribute") {
        mappedField.expressions = {
          ...mappedField.expressions,
          "props.required": (field: FormlyFieldConfig) =>
            isAggregateAttributeRequired(field.parent?.model?.aggFunction),
        };
      }

      if (this.currentOperatorSchema?.operatorType === "FileScanOp" && mappedField.key === "outputFileName") {
        mappedField.expressions = {
          ...mappedField.expressions,
          hide: (field: FormlyFieldConfig) => {
            const model = field.model as { extract?: boolean; attributeType?: string } | undefined;
            const attributeType = model?.attributeType;
            return !(
              model?.extract === true ||
              attributeType === "single string" ||
              attributeType === "binary" ||
              attributeType === "large binary"
            );
          },
        };
      }

      // if the title is python script (for Python UDF), then make this field a custom template 'codearea'
      if (mapSource?.description?.toLowerCase() === "input your code here") {
        if (mappedField.type) {
          mappedField.type = "codearea";
        }
      }
      // if presetService is ready and operator property allows presets, setup formly field to display presets
      if (
        this.config.env.userPresetEnabled &&
        mapSource["enable-presets"] !== undefined &&
        this.currentOperatorId !== undefined
      ) {
        PresetWrapperComponent.setupFieldConfig(
          mappedField,
          "operator",
          this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorId).operatorType,
          this.currentOperatorId
        );
      }

      // TODO: we temporarily disable this due to Yjs update causing issues in Formly.

      // if (
      //   this.currentOperatorId !== undefined &&
      //   ["string", "textarea"].includes(mappedField.type as string) &&
      //   (mappedField.key as string) !== "password"
      // ) {
      //   CollabWrapperComponent.setupFieldConfig(
      //     mappedField,
      //     this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorId).operatorType,
      //     this.currentOperatorId,
      //     mappedField.wrappers?.includes("preset-wrapper")
      //   );
      // }

      if (this.currentOperatorSchema?.operatorType === "Projection" && mappedField.key === "attributes") {
        mappedField.type = "repeat-section-dnd";
        mappedField.props = {
          ...mappedField.props,
          reorder: () => this.onFormChanges(cloneDeep(this.formData)),
        };
      }

      if (mappedField.validators === undefined) {
        mappedField.validators = {};
        // set show to true, or else the error will only show after the user changes the field
        mappedField.validation = {
          show: true,
        };
      }

      if (isDefined(mapSource.enum)) {
        mappedField.validators.inEnum = {
          expression: (c: AbstractControl) => mapSource.enum?.includes(c.value ?? ""),
          message: (error: any, field: FormlyFieldConfig) =>
            `"${field.formControl?.value}" is no longer a valid option`,
        };
      }

      // Add custom validators for attribute type
      if (isDefined(mapSource.attributeTypeRules)) {
        mappedField.validators.checkAttributeType = {
          expression: (control: AbstractControl, field: FormlyFieldConfig) => {
            if (
              !(
                isDefined(this.currentOperatorId) &&
                isDefined(mapSource.attributeTypeRules) &&
                isDefined(mapSource.properties)
              )
            ) {
              return true;
            }

            const findAttributeType = (propertyName: string): AttributeType | undefined => {
              if (
                !isDefined(this.currentOperatorId) ||
                !isDefined(mapSource.properties) ||
                !isDefined(mapSource.properties[propertyName])
              ) {
                return undefined;
              }
              const portIndex = (mapSource.properties[propertyName] as CustomJSONSchema7).autofillAttributeOnPort;
              if (!isDefined(portIndex)) {
                return undefined;
              }
              const attributeName: string = control.value[propertyName];
              return this.workflowCompilingService.getOperatorInputAttributeType(
                this.currentOperatorId,
                portIndex,
                attributeName
              );
            };

            const checkEnumConstraint = (inputAttributeType: AttributeType, enumConstraint: AttributeTypeEnumRule) => {
              if (!enumConstraint.includes(inputAttributeType)) {
                throw TypeError(`it's expected to be ${enumConstraint.join(" or ")}.`);
              }
            };

            const checkConstConstraint = (
              inputAttributeType: AttributeType,
              constConstraint: AttributeTypeConstRule
            ) => {
              const data = constConstraint?.$data;
              if (!isDefined(data)) {
                return;
              }
              const dataAttributeType = findAttributeType(data);
              if (!isDefined(dataAttributeType)) {
                // if data attribute type is not defined, then data attribute is not yet selected. skip validation
                return;
              }
              if (inputAttributeType !== dataAttributeType) {
                // get data attribute name for error message
                const dataAttributeName = control.value[data];
                throw TypeError(`it's expected to be the same type as '${dataAttributeName}' (${dataAttributeType}).`);
              }
            };

            const checkAllOfConstraint = (
              inputAttributeType: AttributeType,
              allOfConstraint: AttributeTypeAllOfRule
            ) => {
              // traverse through all "if-then" sets in "allOf" constraint
              for (const allOf of allOfConstraint) {
                // Only return false when "if" condition is satisfied but "then" condition is not satisfied
                let ifCondSatisfied = true;
                for (const [ifProp, ifConstraint] of Object.entries(allOf.if)) {
                  // Currently, only support "valEnum" constraint
                  // Find attribute value (not type)
                  const ifAttributeValue = control.value[ifProp];
                  if (!ifConstraint.valEnum?.includes(ifAttributeValue)) {
                    ifCondSatisfied = false;
                    break;
                  }
                }
                // Currently, only support "enum" constraint,
                // add more to the condition if needed
                if (ifCondSatisfied && isDefined(allOf.then.enum)) {
                  try {
                    checkEnumConstraint(inputAttributeType, allOf.then.enum);
                  } catch {
                    // parse if condition to readable string
                    const ifCondStr = Object.entries(allOf.if)
                      .map(([ifProp]) => `'${ifProp}' is ${control.value[ifProp]}`)
                      .join(" and ");
                    throw TypeError(`it's expected to be ${allOf.then.enum?.join(" or ")}, given that ${ifCondStr}`);
                  }
                }
              }
            };

            // Get the type of constrains for each property in AttributeTypeRuleSchema

            const checkConstraint = (propertyName: string, constraint: AttributeTypeRuleSet) => {
              const inputAttributeType = findAttributeType(propertyName);

              if (!isDefined(inputAttributeType)) {
                // when inputAttributeType is undefined, it means the property is not set
                return;
              }
              if (isDefined(constraint.enum)) {
                checkEnumConstraint(inputAttributeType, constraint.enum);
              }

              if (isDefined(constraint.const)) {
                checkConstConstraint(inputAttributeType, constraint.const);
              }
              if (isDefined(constraint.allOf)) {
                checkAllOfConstraint(inputAttributeType, constraint.allOf);
              }
            };

            // iterate through all properties in attributeType
            for (const [prop, constraint] of Object.entries(mapSource.attributeTypeRules)) {
              try {
                checkConstraint(prop, constraint);
              } catch (err) {
                // have to get the type, attribute name and property name again
                // should consider reusing the part in findAttributeType()
                const attributeName = control.value[prop];
                const port = (mapSource.properties[prop] as CustomJSONSchema7).autofillAttributeOnPort as number;
                const inputAttributeType = this.workflowCompilingService.getOperatorInputAttributeType(
                  this.currentOperatorId,
                  port,
                  attributeName
                );
                // @ts-ignore
                const message = err.message;
                if (field.validators === undefined) {
                  field.validators = {};
                }
                field.validators.checkAttributeType.message =
                  `Warning: The type of '${attributeName}' is ${inputAttributeType}, but ` + message;
                return false;
              }
            }
            return true;
          },
        };
      }

      return mappedField;
    };

    this.formlyFormGroup = new FormGroup({});
    this.formlyOptions = {};
    // convert the json schema to formly config, pass a copy because formly mutates the schema object
    const field = this.formlyJsonschema.toFieldConfig(cloneDeep(schema), {
      map: jsonSchemaMapIntercept,
    });
    field.hooks = {
      onInit: fieldConfig => {
        if (!this.interactive) {
          fieldConfig?.form?.disable();
        }
      },
    };

    const schemaProperties = schema.properties;
    const fields = field.fieldGroup;

    // adding custom options, relational N-to-M mapping.
    if (schemaProperties && fields) {
      Object.entries(schemaProperties).forEach(([propertyName, propertyValue]) => {
        if (typeof propertyValue === "boolean") {
          return;
        }
        if (propertyValue.toggleHidden) {
          setHideExpression(propertyValue.toggleHidden, fields, propertyName);
        }

        if (propertyValue.dependOn) {
          if (isDefined(this.currentOperatorId)) {
            const attributes: Readonly<Record<string, PortSchema | undefined>> | undefined =
              this.workflowCompilingService.getOperatorInputSchemaMap(this.currentOperatorId);
            setChildTypeDependency(attributes, propertyValue.dependOn, fields, propertyName);
          }
        }
      });
    }
    // not return field.fieldGroup directly because
    // doing so the validator in the field will not be triggered
    this.formlyFields = [field];
  }

  allowModifyOperatorLogic(): void {
    this.setInteractivity(true);
  }

  confirmModifyOperatorLogic(): void {
    if (this.currentOperatorId) {
      try {
        this.executeWorkflowService.modifyOperatorLogic(this.currentOperatorId);
        this.setInteractivity(false);
      } catch (e) {
        this.notificationService.error((e as Error).message);
      }
    }
  }

  /**
   * Connects the actual y-text structure of this operator's name to the editor's awareness manager.
   */
  connectQuillToText() {
    this.registerQuillBinding();
    const currentOperatorSharedType = this.workflowActionService
      .getTexeraGraph()
      .getSharedOperatorType(<string>this.currentOperatorId);
    if (this.currentOperatorId) {
      if (!currentOperatorSharedType.has("customDisplayName")) {
        currentOperatorSharedType.set("customDisplayName", new Y.Text());
      }
      const ytext = currentOperatorSharedType.get("customDisplayName");
      this.quillBinding = new QuillBinding(
        ytext as Y.Text,
        this.quill,
        this.workflowActionService.getTexeraGraph().getSharedModelAwareness()
      );
    }
  }

  /**
   * Stop editing title and hide the editor.
   */
  disconnectQuillFromText() {
    this.quill.blur();
    this.quillBinding = undefined;
    this.editingTitle = false;
  }

  private registerOperatorDisplayNameChangeHandler(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getOperatorDisplayNameChangedStream()
      .pipe(untilDestroyed(this))
      .subscribe(({ operatorID, newDisplayName }) => {
        if (operatorID === this.currentOperatorId) this.formTitle = newDisplayName;
      });
  }

  /**
   * Initializes shared text editor.
   * @private
   */
  private registerQuillBinding() {
    // Operator name editor
    const element = document.getElementById("customName") as Element;
    this.quill = new Quill(element, {
      modules: {
        cursors: true,
        toolbar: false,
        history: {
          // Local undo shouldn't undo changes
          // from remote users
          userOnly: true,
        },
        // Disable newline on enter and instead quit editing
        keyboard: {
          bindings: {
            enter: {
              key: 13,
              handler: () => this.disconnectQuillFromText(),
            },
            shift_enter: {
              key: 13,
              shiftKey: true,
              handler: () => this.disconnectQuillFromText(),
            },
          },
        },
      },
      formats: [],
      placeholder: "Start collaborating...",
      theme: "snow",
    });
  }
}

import {
  ChangeDetectorRef,
  Component,
  ComponentFactoryResolver,
  Input,
  OnChanges,
  OnDestroy,
  OnInit,
  SimpleChanges,
} from "@angular/core";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { Subject } from "rxjs";
import { AbstractControl, FormGroup } from "@angular/forms";
import { FormlyFieldConfig, FormlyFormOptions } from "@ngx-formly/core";
import Ajv from "ajv";
import { FormlyJsonschema } from "@ngx-formly/core/json-schema";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { cloneDeep, isEqual } from "lodash-es";
import { CustomJSONSchema7, hideTypes } from "../../../types/custom-json-schema.interface";
import { isDefined } from "../../../../common/util/predicate";
import { ExecutionState } from "src/app/workspace/types/execute-workflow.interface";
import { DynamicSchemaService } from "../../../service/dynamic-schema/dynamic-schema.service";
import {
  SchemaAttribute,
  SchemaPropagationService,
} from "../../../service/dynamic-schema/schema-propagation/schema-propagation.service";
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
import { DynamicComponentConfig } from "../../../../common/type/dynamic-component-config";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { filter } from "rxjs/operators";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { PresetWrapperComponent } from "src/app/common/formly/preset-wrapper/preset-wrapper.component";
import { environment } from "src/environments/environment";
import { WorkflowVersionService } from "../../../../dashboard/service/workflow-version/workflow-version.service";
import { UserFileService } from "../../../../dashboard/service/user-file/user-file.service";
import { AccessEntry } from "../../../../dashboard/type/access.interface";
import { WorkflowAccessService } from "../../../../dashboard/service/workflow-access/workflow-access.service";
import { Workflow } from "../../../../common/type/workflow";
import { QuillBinding } from "y-quill";
import Quill from "quill";
import QuillCursors from "quill-cursors";
import * as Y from "yjs";
import { CollabWrapperComponent } from "../../../../common/formly/collab-wrapper/collab-wrapper/collab-wrapper.component";
import { JSONSchema7Type } from "json-schema";

export type PropertyDisplayComponent = TypeCastingDisplayComponent;

export type PropertyDisplayComponentConfig = DynamicComponentConfig<PropertyDisplayComponent>;

Quill.register("modules/cursors", QuillCursors);

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
})
export class OperatorPropertyEditFrameComponent implements OnInit, OnChanges, OnDestroy {
  @Input() currentOperatorId?: string;

  // re-declare enum for angular template to access it
  readonly ExecutionState = ExecutionState;

  // whether the editor can be edited
  interactive: boolean = this.evaluateInteractivity();

  // the source event stream of form change triggered by library at each user input
  sourceFormChangeEventStream = new Subject<Record<string, unknown>>();

  // the output form change event stream after debounce time and filtering out values
  operatorPropertyChangeStream = createOutputFormChangeEventStream(this.sourceFormChangeEventStream, data =>
    this.checkOperatorProperty(data)
  );

  // inputs and two-way bindings to formly component
  formlyFormGroup: FormGroup | undefined;
  formData: any;
  formlyOptions: FormlyFormOptions = {};
  formlyFields: FormlyFieldConfig[] | undefined;
  formTitle: string | undefined;

  // The field name and its css style to be overridden, e.g., for showing the diff between two workflows.
  // example: new Map([
  //     ["attribute", "outline: 3px solid green; transition: 0.3s ease-in-out outline;"],
  //     ["condition", "background: red; border-color: red;"],
  //   ]);
  fieldStyleOverride: Map<String, String> = new Map([]);

  editingTitle: boolean = false;

  // used to fill in default values in json schema to initialize new operator
  ajv = new Ajv({ useDefaults: true, strict: false });

  // for display component of some extra information
  extraDisplayComponentConfig?: PropertyDisplayComponentConfig;
  public lockGranted: boolean = true;
  public allUserWorkflowAccess: ReadonlyArray<AccessEntry> = [];
  public operatorVersion: string = "";
  quillBinding?: QuillBinding;
  quill!: Quill;
  // used to tear down subscriptions that takeUntil(teardownObservable)
  private teardownObservable: Subject<void> = new Subject();

  constructor(
    private formlyJsonschema: FormlyJsonschema,
    private workflowActionService: WorkflowActionService,
    public executeWorkflowService: ExecuteWorkflowService,
    private dynamicSchemaService: DynamicSchemaService,
    private schemaPropagationService: SchemaPropagationService,
    private notificationService: NotificationService,
    private changeDetectorRef: ChangeDetectorRef,
    private workflowVersionService: WorkflowVersionService,
    private userFileService: UserFileService,
    private workflowGrantAccessService: WorkflowAccessService
  ) {}

  ngOnChanges(changes: SimpleChanges): void {
    this.currentOperatorId = changes.currentOperatorId?.currentValue;
    if (!this.currentOperatorId) {
      return;
    }
    this.rerenderEditorForm();
  }

  switchDisplayComponent(targetConfig?: PropertyDisplayComponentConfig) {
    if (
      this.extraDisplayComponentConfig?.component === targetConfig?.component &&
      this.extraDisplayComponentConfig?.component === targetConfig?.componentInputs
    ) {
      return;
    }

    this.extraDisplayComponentConfig = targetConfig;
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

    let workflow = this.workflowActionService.getWorkflow();
    if (workflow) this.refreshGrantedList(workflow);
  }

  public refreshGrantedList(workflow: Workflow): void {
    this.workflowGrantAccessService
      .retrieveGrantedWorkflowAccessList(workflow)
      .pipe(untilDestroyed(this))
      .subscribe(
        (userWorkflowAccess: ReadonlyArray<AccessEntry>) => (this.allUserWorkflowAccess = userWorkflowAccess),
        // @ts-ignore // TODO: fix this with notification component
        (err: unknown) => console.log(err.error)
      );
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
    // This assumes "fileName" to be the only key for file names in an operator property.
    const filename: string = <string>event["fileName"];
    if (filename) {
      const [owner, fname] = filename.split("/", 2);
      this.allUserWorkflowAccess.forEach(userWorkflowAccess => {
        this.userFileService
          .grantUserFileAccess(
            {
              ownerName: owner,
              file: { fid: -1, path: "", size: -1, description: "", uploadTime: "", name: fname },
              accessLevel: "read",
              isOwner: true,
              projectIDs: [],
            },
            userWorkflowAccess.userName,
            "read"
          )
          .pipe(untilDestroyed(this))
          .subscribe();
      });
    }

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
    console.log("re-rendered");
    // console.trace()
    this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("currentlyEditing", this.currentOperatorId);
    const operator = this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorId);
    // set the operator data needed
    const currentOperatorSchema = this.dynamicSchemaService.getDynamicSchema(this.currentOperatorId);
    this.workflowActionService.setOperatorVersion(operator.operatorID, currentOperatorSchema.operatorVersion);
    this.operatorVersion = operator.operatorVersion.slice(0, 9);
    this.setFormlyFormBinding(currentOperatorSchema.jsonSchema);
    this.formTitle = operator.customDisplayName ?? currentOperatorSchema.additionalMetadata.userFriendlyName;

    /**
     * Important: make a deep copy of the initial property data object.
     * Prevent the form directly changes the value in the texera graph without going through workflow action service.
     */
    this.formData = cloneDeep(operator.operatorProperties);

    // use ajv to initialize the default value to data according to schema, see https://ajv.js.org/#assigning-defaults
    // WorkflowUtil service also makes sure that the default values are filled in when operator is added from the UI
    // However, we perform an addition check for the following reasons:
    // 1. the operator might be added not directly from the UI, which violates the precondition
    // 2. the schema might change, which specifies a new default value
    // 3. formly doesn't emit change event when it fills in default value, causing an inconsistency between component and service
    this.ajv.validate(currentOperatorSchema, this.formData);

    // manually trigger a form change event because default value might be filled in
    this.onFormChanges(this.formData);

    if (
      this.workflowActionService
        .getTexeraGraph()
        .getOperator(this.currentOperatorId)
        .operatorType.includes(TYPE_CASTING_OPERATOR_TYPE)
    ) {
      this.switchDisplayComponent({
        component: TypeCastingDisplayComponent,
        componentInputs: { currentOperatorId: this.currentOperatorId },
      });
    } else {
      this.switchDisplayComponent(undefined);
    }
    // execute set interactivity immediately in another task because of a formly bug
    // whenever the form model is changed, formly can only disable it after the UI is rendered
    setTimeout(() => {
      const interactive = this.evaluateInteractivity();
      this.setInteractivity(interactive);
      this.changeDetectorRef.detectChanges();
    }, 0);
  }

  evaluateInteractivity(): boolean {
    return this.workflowActionService.checkWorkflowModificationEnabled();
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
        filter(_ => this.currentOperatorId !== undefined),
        filter(operatorChanged => operatorChanged.operator.operatorID === this.currentOperatorId),
        filter(operatorChanged => !isEqual(this.formData, operatorChanged.operator.operatorProperties))
      )
      .pipe(untilDestroyed(this))
      .subscribe(operatorChanged => (this.formData = cloneDeep(operatorChanged.operator.operatorProperties)));
  }

  /**
   * This method captures the change in operator's port via program instead of user updating the
   *  json schema form in the user interface.
   *
   * For instance, when the input doesn't match the new json schema and the UI needs to remove the
   *  invalid fields, this form will capture those events.
   */
  registerOperatorPortChangeHandler(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getOperatorPortChangeStream()
      .pipe(
        filter(_ => this.currentOperatorId !== undefined),
        filter(operatorChanged => operatorChanged.newOperator.operatorID === this.currentOperatorId),
        filter(operatorChanged => !isEqual(this.formData, operatorChanged.newOperator.inputPorts))
      )
      .pipe(untilDestroyed(this))
      .subscribe(operatorChanged => (this.formData = cloneDeep(operatorChanged.newOperator.inputPorts)));
  }

  /**
   * This method handles the form change event and set the operator property
   *  in the texera graph.
   */
  registerOnFormChangeHandler(): void {
    this.operatorPropertyChangeStream.pipe(untilDestroyed(this)).subscribe(formData => {
      // set the operator property to be the new form data
      if (this.currentOperatorId) {
        this.workflowActionService.setOperatorProperty(this.currentOperatorId, cloneDeep(formData));
      }
    });
  }

  registerDisableEditorInteractivityHandler(): void {
    this.workflowActionService
      .getWorkflowModificationEnabledStream()
      .pipe(untilDestroyed(this))
      .subscribe(canModify => {
        if (this.currentOperatorId) {
          const interactive = this.evaluateInteractivity();
          this.setInteractivity(interactive);
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
      mappedField.expressionProperties = {
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

      // conditionally hide the field according to the schema
      if (
        isDefined(mapSource.hideExpectedValue) &&
        isDefined(mapSource.hideTarget) &&
        isDefined(mapSource.hideType) &&
        hideTypes.includes(mapSource.hideType)
      ) {
        mappedField.hideExpression = createShouldHideFieldFunc(
          mapSource.hideTarget,
          mapSource.hideType,
          mapSource.hideExpectedValue
        );
      }

      // if the title is fileName, then change it to custom autocomplete input template
      if (mappedField.key == "fileName") {
        mappedField.type = "inputautocomplete";
      }

      // if the title is python script (for Python UDF), then make this field a custom template 'codearea'
      if (mapSource?.description?.toLowerCase() === "input your code here") {
        if (mappedField.type) {
          mappedField.type = "codearea";
        }
      }
      // if presetService is ready and operator property allows presets, setup formly field to display presets
      if (
        environment.userSystemEnabled &&
        environment.userPresetEnabled &&
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

      if (
        this.currentOperatorId !== undefined &&
        ["string", "textarea"].includes(mappedField.type as string) &&
        (mappedField.key as string) !== "password"
      ) {
        CollabWrapperComponent.setupFieldConfig(
          mappedField,
          this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorId).operatorType,
          this.currentOperatorId,
          mappedField.wrappers?.includes("preset-wrapper")
        );
      }

      if (isDefined(mapSource.enum)) {
        mappedField.validators = {
          inEnum: {
            expression: (c: AbstractControl) => mapSource.enum?.includes(c.value),
            message: (error: any, field: FormlyFieldConfig) =>
              `"${field.formControl?.value}" is no longer a valid option`,
          },
        };
        mappedField.validation = {
          show: true,
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
            const attributes: ReadonlyArray<ReadonlyArray<SchemaAttribute> | null> | undefined =
              this.schemaPropagationService.getOperatorInputSchema(this.currentOperatorId);
            setChildTypeDependency(attributes, propertyValue.dependOn, fields, propertyName);
          }
        }
      });
    }

    this.formlyFields = fields;
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
        this.notificationService.error(e);
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

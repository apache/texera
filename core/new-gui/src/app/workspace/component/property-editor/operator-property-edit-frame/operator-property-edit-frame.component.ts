import { ChangeDetectorRef, Component, Input, OnChanges, OnDestroy, OnInit, SimpleChanges } from "@angular/core";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { Subject } from "rxjs";
import { FormGroup } from "@angular/forms";
import { FormlyFieldConfig, FormlyFormOptions } from "@ngx-formly/core";
import Ajv from "ajv";
import { FormlyJsonschema } from "@ngx-formly/core/json-schema";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { cloneDeep, isEqual } from "lodash-es";
import { CustomJSONSchema7 } from "../../../types/custom-json-schema.interface";
import { isDefined } from "../../../../common/util/predicate";
import { ExecutionState } from "src/app/workspace/types/execute-workflow.interface";
import { DynamicSchemaService } from "../../../service/dynamic-schema/dynamic-schema.service";
import {
  SchemaAttribute,
  SchemaPropagationService,
} from "../../../service/dynamic-schema/schema-propagation/schema-propagation.service";
import {
  createOutputFormChangeEventStream,
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
import { WorkflowCollabService } from "../../../service/workflow-collab/workflow-collab.service";

export type PropertyDisplayComponent = TypeCastingDisplayComponent;

export type PropertyDisplayComponentConfig = DynamicComponentConfig<PropertyDisplayComponent>;

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

  editingTitle: boolean = false;

  // used to fill in default values in json schema to initialize new operator
  ajv = new Ajv({ useDefaults: true, strict: false });

  // for display component of some extra information
  extraDisplayComponentConfig?: PropertyDisplayComponentConfig;

  // used to tear down subscriptions that takeUntil(teardownObservable)
  private teardownObservable: Subject<void> = new Subject();
  public lockGranted: boolean = true;

  constructor(
    private formlyJsonschema: FormlyJsonschema,
    private workflowActionService: WorkflowActionService,
    public executeWorkflowService: ExecuteWorkflowService,
    private dynamicSchemaService: DynamicSchemaService,
    private schemaPropagationService: SchemaPropagationService,
    private notificationService: NotificationService,
    private workflowCollabService: WorkflowCollabService,
    private changeDetectorRef: ChangeDetectorRef
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

    this.registerLockChangeHandler();
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
    const operator = this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorId);
    // set the operator data needed
    const currentOperatorSchema = this.dynamicSchemaService.getDynamicSchema(this.currentOperatorId);
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
   * For instance, when the input doesn't matching the new json schema and the UI needs to remove the
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

  private registerOperatorDisplayNameChangeHandler(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getOperatorDisplayNameChangedStream()
      .pipe(untilDestroyed(this))
      .subscribe(({ operatorID, newDisplayName }) => {
        if (operatorID === this.currentOperatorId) this.formTitle = newDisplayName;
      });
  }

  private registerLockChangeHandler(): void {
    this.workflowCollabService
      .getLockStatusStream()
      .pipe(untilDestroyed(this))
      .subscribe((lockGranted: boolean) => {
        this.lockGranted = lockGranted;
        this.changeDetectorRef.detectChanges();
      });
  }

  setFormlyFormBinding(schema: CustomJSONSchema7) {
    // intercept JsonSchema -> FormlySchema process, adding custom options
    // this requires a one-to-one mapping.
    // for relational custom options, have to do it after FormlySchema is generated.
    const jsonSchemaMapIntercept = (
      mappedField: FormlyFieldConfig,
      mapSource: CustomJSONSchema7
    ): FormlyFieldConfig => {
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

  confirmChangeOperatorCustomName(customDisplayName: string) {
    if (this.currentOperatorId) {
      const currentOperatorSchema = this.dynamicSchemaService.getDynamicSchema(this.currentOperatorId);
      const userFriendlyName = currentOperatorSchema.additionalMetadata.userFriendlyName;
      // fall back to the original userFriendlyName if no valid name is provided
      const newDisplayName =
        customDisplayName === "" || customDisplayName === undefined
          ? currentOperatorSchema.additionalMetadata.userFriendlyName
          : customDisplayName;
      this.workflowActionService.setOperatorCustomName(this.currentOperatorId, newDisplayName, userFriendlyName);
      this.formTitle = newDisplayName;
    }

    this.editingTitle = false;
  }
}

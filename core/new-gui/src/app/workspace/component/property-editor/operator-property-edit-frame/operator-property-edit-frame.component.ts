import {
  AfterViewChecked,
  ChangeDetectorRef,
  Component,
  Input,
  OnChanges,
  OnDestroy,
  OnInit,
  SimpleChanges,
} from "@angular/core";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { WorkflowStatusService } from "../../../service/workflow-status/workflow-status.service";
import { Subject } from "rxjs";
import { AbstractControl, FormGroup } from "@angular/forms";
import { FormlyFieldConfig, FormlyFormOptions } from "@ngx-formly/core";
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
import {
  PortInputSchema,
  AttributeType,
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
import { WorkflowVersionService } from "../../../../dashboard/user/service/workflow-version/workflow-version.service";
import { UserFileService } from "../../../../dashboard/user/service/user-file/user-file.service";
import { ShareAccess } from "../../../../dashboard/user/type/share-access.interface";
import { ShareAccessService } from "../../../../dashboard/user/service/share-access/share-access.service";
import { QuillBinding } from "y-quill";
import Quill from "quill";
import QuillCursors from "quill-cursors";
import * as Y from "yjs";
import { CollabWrapperComponent } from "../../../../common/formly/collab-wrapper/collab-wrapper/collab-wrapper.component";
import { OperatorSchema } from "src/app/workspace/types/operator-schema.interface";

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
export class OperatorPropertyEditFrameComponent implements OnInit, OnChanges, OnDestroy, AfterViewChecked {
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
  public allUserWorkflowAccess: ReadonlyArray<ShareAccess> = [];
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
    private workflowGrantAccessService: ShareAccessService,
    private workflowStatusSerivce: WorkflowStatusService
  ) {}

  ngOnChanges(changes: SimpleChanges): void {
    this.currentOperatorId = changes.currentOperatorId?.currentValue;
    if (!this.currentOperatorId) {
      return;
    }
    this.rerenderEditorForm();
  }

  ngAfterViewChecked(): void {
    this.changeDetectorRef.detectChanges();
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

    this.workflowStatusSerivce
      .getStatusUpdateStream()
      .pipe(untilDestroyed(this))
      .subscribe(update => {
        if (this.currentOperatorId) {
          this.currentOperatorStatus = update[this.currentOperatorId];
        }
      });
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
    this.ajv.validate(this.currentOperatorSchema, this.formData);

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
    const opInputSchema = this.schemaPropagationService.getOperatorInputSchema(this.currentOperatorId);
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

      if (mappedField.validators === undefined) {
        mappedField.validators = {};
        // set show to true, or else the error will only show after the user changes the field
        mappedField.validation = {
          show: true,
        };
      }

      if (isDefined(mapSource.enum)) {
        mappedField.validators.inEnum = {
          expression: (c: AbstractControl) => mapSource.enum?.includes(c.value),
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
              return this.schemaPropagationService.getOperatorInputAttributeType(
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
                const inputAttributeType = this.schemaPropagationService.getOperatorInputAttributeType(
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
            const attributes: ReadonlyArray<PortInputSchema | undefined> | undefined =
              this.schemaPropagationService.getOperatorInputSchema(this.currentOperatorId);
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

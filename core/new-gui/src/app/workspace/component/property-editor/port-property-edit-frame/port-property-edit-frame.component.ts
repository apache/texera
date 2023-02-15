import { Component, Input, OnChanges, OnInit, SimpleChanges } from "@angular/core";
import { OperatorPort, PortDescription } from "../../../types/workflow-common.interface";
import { Subject } from "rxjs";
import { createOutputFormChangeEventStream } from "../../../../common/formly/formly-utils";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { isEqual } from "lodash";
import { CustomJSONSchema7 } from "../../../types/custom-json-schema.interface";
import { FormlyFieldConfig, FormlyFormOptions } from "@ngx-formly/core";
import { FormGroup } from "@angular/forms";
import { cloneDeep } from "lodash-es";
import { FormlyJsonschema } from "@ngx-formly/core/json-schema";
import { filter } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import * as Y from "yjs";
import { QuillBinding } from "y-quill";
import Quill from "quill";
import { YType } from "../../../types/shared-editing.interface";
import QuillCursors from "quill-cursors";
import { mockOperatorPortSchema } from "../../../service/operator-metadata/mock-operator-metadata.data";

Quill.register("modules/cursors", QuillCursors);

@UntilDestroy()
@Component({
  selector: "texera-port-property-edit-frame",
  templateUrl: "./port-property-edit-frame.component.html",
  styleUrls: ["./port-property-edit-frame.component.scss"],
})
export class PortPropertyEditFrameComponent implements OnInit, OnChanges {
  @Input() currentOperatorPortID: OperatorPort | undefined;

  // whether the editor can be edited
  interactive: boolean = true;

  listeningToChange: boolean = true;

  formlyFormGroup: FormGroup | undefined;
  formData: any;
  formlyOptions: FormlyFormOptions = {};
  formlyFields: FormlyFieldConfig[] | undefined;
  formTitle: string | undefined;

  editingTitle: boolean = false;

  quillBinding?: QuillBinding;
  quill!: Quill;

  // the source event stream of form change triggered by library at each user input
  sourceFormChangeEventStream = new Subject<Record<string, unknown>>();

  // the output form change event stream after debounce time and filtering out values
  operatorPortPropertyChangeStream = createOutputFormChangeEventStream(this.sourceFormChangeEventStream, data =>
    this.checkOperatorPort(data)
  );

  constructor(private formlyJsonschema: FormlyJsonschema, private workflowActionService: WorkflowActionService) {}

  ngOnInit(): void {
    this.registerOperatorPortPropertyChangeHandler();
    this.registerOperatorPortDisplayNameChangeHandler();
    this.registerOnFormChangeHandler();
  }

  ngOnChanges(changes: SimpleChanges): void {
    this.currentOperatorPortID = changes.currentOperatorPortID.currentValue;
    if (this.currentOperatorPortID) this.showOperatorPortPropertyEditor(this.currentOperatorPortID);
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
   * Connects the actual y-text structure of this operator's name to the editor's awareness manager.
   */
  connectQuillToText() {
    this.registerQuillBinding();
    if (!this.currentOperatorPortID) return;
    const currentPortDescriptorSharedType = this.workflowActionService
      .getTexeraGraph()
      .getsharedOperatorPortDescriptionType(this.currentOperatorPortID);
    if (currentPortDescriptorSharedType === undefined) return;
    if (!currentPortDescriptorSharedType.has("displayName"))
      currentPortDescriptorSharedType.set("displayName", new Y.Text());
    const ytext = currentPortDescriptorSharedType.get("displayName");
    this.quillBinding = new QuillBinding(
      ytext as Y.Text,
      this.quill,
      this.workflowActionService.getTexeraGraph().getSharedModelAwareness()
    );
  }

  /**
   * Stop editing title and hide the editor.
   */
  disconnectQuillFromText() {
    this.quill.blur();
    this.quillBinding = undefined;
    this.editingTitle = false;
  }

  private showOperatorPortPropertyEditor(operatorPortID: OperatorPort): void {
    if (!this.workflowActionService.getTexeraGraph().hasOperatorPort(operatorPortID)) {
      throw new Error(
        `change property editor: operator port ${operatorPortID.operatorID}, ${operatorPortID.portID}} does not exist`
      );
    }
    this.currentOperatorPortID = operatorPortID;
    const portDescriptor = this.workflowActionService
      .getTexeraGraph()
      .getOperatorPort(operatorPortID) as PortDescription;
    this.formTitle = portDescriptor.displayName;
    if (!this.currentOperatorPortID.operatorID.includes("PythonUDF")) return;

    const portInfo = portDescriptor?.partitionRequirement;
    this.formData = portInfo !== undefined ? cloneDeep(portInfo) : {};
    const portSchema = mockOperatorPortSchema.jsonSchema;
    this.setFormlyFormBinding(portSchema);
  }

  private checkOperatorPort(formData: Record<string, unknown>): boolean {
    // check if the component is displaying the port
    if (!this.currentOperatorPortID) return false;
    if (!this.workflowActionService.getTexeraGraph().hasOperatorPort(this.currentOperatorPortID)) return false;
    const operatorPortDescription = this.workflowActionService
      .getTexeraGraph()
      .getOperatorPort(this.currentOperatorPortID);
    console.log(formData, operatorPortDescription?.partitionRequirement);
    return !isEqual(formData, operatorPortDescription?.partitionRequirement);
  }

  /**
   * This method handles the form change event
   */
  private registerOnFormChangeHandler(): void {
    this.operatorPortPropertyChangeStream.pipe(untilDestroyed(this)).subscribe(formData => {
      if (this.currentOperatorPortID) {
        this.listeningToChange = false;
        this.workflowActionService.setOperatorPortProperty(this.currentOperatorPortID, cloneDeep(formData));
        this.listeningToChange = true;
      }
    });
  }

  /**
   * This method captures the change in operator's property via program instead of user updating the
   *  json schema form in the user interface.
   *
   * For instance, when the input doesn't matching the new json schema and the UI needs to remove the
   *  invalid fields, this form will capture those events.
   */
  private registerOperatorPortPropertyChangeHandler(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getOperatorPortPropertyChangedStream()
      .pipe(
        filter(_ => this.listeningToChange),
        filter(_ => this.currentOperatorPortID !== undefined),
        filter(event => isEqual(event.operatorPortID, this.currentOperatorPortID)),
        filter(event => !isEqual(this.formData, event.newProperty))
      )
      .pipe(untilDestroyed(this))
      .subscribe(event => (this.formData = cloneDeep(event.newProperty)));
  }

  private setFormlyFormBinding(schema: CustomJSONSchema7) {
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
    this.formlyFields = field.fieldGroup;
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

  private registerOperatorPortDisplayNameChangeHandler(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getOperatorPortDisplayNameChangedSubject()
      .pipe(untilDestroyed(this))
      .subscribe(({ operatorID, portID, newDisplayName }) => {
        if (operatorID === this.currentOperatorPortID?.operatorID && portID === this.currentOperatorPortID?.portID)
          this.formTitle = newDisplayName;
      });
  }
}

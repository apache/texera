import { Component, Input, OnChanges, OnInit, SimpleChanges } from "@angular/core";
import { LogicalPort, PortDescription } from "../../../types/workflow-common.interface";
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
import QuillCursors from "quill-cursors";
import { mockPortSchema } from "../../../service/operator-metadata/mock-operator-metadata.data";
import { DynamicSchemaService } from "../../../service/dynamic-schema/dynamic-schema.service";

Quill.register("modules/cursors", QuillCursors);

@UntilDestroy()
@Component({
  selector: "texera-port-property-edit-frame",
  templateUrl: "./port-property-edit-frame.component.html",
  styleUrls: ["./port-property-edit-frame.component.scss"],
})
export class PortPropertyEditFrameComponent implements OnInit, OnChanges {
  @Input() currentPortID: LogicalPort | undefined;

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
  portPropertyChangeStream = createOutputFormChangeEventStream(this.sourceFormChangeEventStream, data =>
    this.checkPort(data)
  );

  constructor(
    private formlyJsonschema: FormlyJsonschema,
    private workflowActionService: WorkflowActionService,
    private dynamicSchemaService: DynamicSchemaService
  ) {}

  ngOnInit(): void {
    this.registerPortPropertyChangeHandler();
    this.registerPortDisplayNameChangeHandler();
    this.registerOnFormChangeHandler();
  }

  ngOnChanges(changes: SimpleChanges): void {
    this.currentPortID = changes.currentPortID.currentValue;
    if (this.currentPortID) this.showPortPropertyEditor(this.currentPortID);
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
    if (!this.currentPortID) return;
    const currentPortDescriptorSharedType = this.workflowActionService
      .getTexeraGraph()
      .getSharedPortDescriptionType(this.currentPortID);
    if (currentPortDescriptorSharedType === undefined) return;
    if (!currentPortDescriptorSharedType.has("displayName")) {
      currentPortDescriptorSharedType.set("displayName", new Y.Text());
    }
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

  private showPortPropertyEditor(operatorPortID: LogicalPort): void {
    if (!this.workflowActionService.getTexeraGraph().hasPort(operatorPortID)) {
      throw new Error(
        `change property editor: operator port ${operatorPortID.operatorID}, ${operatorPortID.portID}} does not exist`
      );
    }
    this.currentPortID = operatorPortID;
    const portDescriptor = this.workflowActionService
      .getTexeraGraph()
      .getPortDescription(operatorPortID) as PortDescription;
    this.formTitle = portDescriptor.displayName;
    const currentOperatorSchema = this.dynamicSchemaService.getDynamicSchema(this.currentPortID.operatorID);
    // Only specific types of operators and input ports can have the following customization.
    if (!(currentOperatorSchema.additionalMetadata.allowPortCustomization && portDescriptor.portID.includes("input")))
      return;

    const portInfo = {
      partitionInfo: portDescriptor?.partitionRequirement,
      dependencies: portDescriptor?.dependencies,
    };
    this.formData = cloneDeep(portInfo);
    const portSchema = mockPortSchema.jsonSchema;
    this.setFormlyFormBinding(portSchema);
  }

  private checkPort(formData: Record<string, unknown>): boolean {
    // check if the component is displaying the port
    if (!this.currentPortID) return false;
    if (!this.workflowActionService.getTexeraGraph().hasPort(this.currentPortID)) return false;
    const operatorPortDescription = this.workflowActionService.getTexeraGraph().getPortDescription(this.currentPortID);
    return !isEqual(formData, operatorPortDescription?.partitionRequirement);
  }

  /**
   * This method handles the form change event
   */
  private registerOnFormChangeHandler(): void {
    this.portPropertyChangeStream.pipe(untilDestroyed(this)).subscribe(formData => {
      if (this.currentPortID) {
        this.listeningToChange = false;
        this.workflowActionService.setPortProperty(this.currentPortID, cloneDeep(formData));
        this.listeningToChange = true;
      }
    });
  }

  /**
   * This method captures the change in the operator's property via a program instead of user updating the
   *  json schema form in the user interface.
   *
   * For instance, when the input doesn't match the new json schema and the UI needs to remove the
   *  invalid fields, this form will capture those events.
   */
  private registerPortPropertyChangeHandler(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getPortPropertyChangedStream()
      .pipe(
        filter(_ => this.listeningToChange),
        filter(_ => this.currentPortID !== undefined),
        filter(event => isEqual(event.operatorPortID, this.currentPortID)),
        filter(event => !isEqual(this.formData, event.newProperty))
      )
      .pipe(untilDestroyed(this))
      .subscribe(event => (this.formData = cloneDeep(event.newProperty)));
  }

  private setFormlyFormBinding(schema: CustomJSONSchema7) {
    this.formlyFormGroup = new FormGroup({});
    this.formlyOptions = {};
    // convert the json schema to formly config, pass a copy because formly mutates the schema object
    const field = this.formlyJsonschema.toFieldConfig(cloneDeep(schema));
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

  private registerPortDisplayNameChangeHandler(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getPortDisplayNameChangedSubject()
      .pipe(untilDestroyed(this))
      .subscribe(({ operatorID, portID, newDisplayName }) => {
        if (operatorID === this.currentPortID?.operatorID && portID === this.currentPortID?.portID)
          this.formTitle = newDisplayName;
      });
  }
}

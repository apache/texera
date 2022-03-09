import { Component, Input, OnChanges, OnInit, SimpleChanges } from "@angular/core";
import { cloneDeep, isEqual } from "lodash-es";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { DynamicSchemaService } from "../../../service/dynamic-schema/dynamic-schema.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { FormlyJsonschema } from "@ngx-formly/core/json-schema";
import { ExecutionState } from "src/app/workspace/types/execute-workflow.interface";
import { FormGroup } from "@angular/forms";
import { Subject } from "rxjs";
import { FormlyFieldConfig, FormlyFormOptions } from "@ngx-formly/core";
import { CustomJSONSchema7 } from "../../../types/custom-json-schema.interface";
import { createOutputFormChangeEventStream } from "src/app/common/formly/formly-utils";
import { filter } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

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
  selector: "texera-breakpoint-frame",
  templateUrl: "./breakpoint-property-edit-frame.component.html",
  styleUrls: ["./breakpoint-property-edit-frame.component.scss"],
})
export class BreakpointPropertyEditFrameComponent implements OnInit, OnChanges {
  @Input() currentLinkId: string | undefined;

  // whether the editor can be edited
  interactive: boolean = true;

  // the source event stream of form change triggered by library at each user input
  sourceFormChangeEventStream = new Subject<Record<string, unknown>>();

  breakpointChangeStream = createOutputFormChangeEventStream(this.sourceFormChangeEventStream, data =>
    this.checkBreakpoint(data)
  );

  // inputs and two-way bindings to formly component
  formlyFormGroup: FormGroup | undefined;
  formData: any;
  formlyOptions: FormlyFormOptions = {};
  formlyFields: FormlyFieldConfig[] | undefined;
  formTitle: string | undefined;

  constructor(
    private formlyJsonschema: FormlyJsonschema,
    private workflowActionService: WorkflowActionService,
    private autocompleteService: DynamicSchemaService,
    private executeWorkflowService: ExecuteWorkflowService
  ) {}

  ngOnChanges(changes: SimpleChanges): void {
    this.currentLinkId = changes.currentLinkId?.currentValue;
    if (this.currentLinkId) {
      this.showBreakpointEditor(this.currentLinkId);
    }
  }

  ngOnInit(): void {
    // when the operator's property is updated via program instead of user updating the json schema form,
    //  this observable will be responsible in handling these events.
    this.registerOperatorPropertyChangeHandler();

    // handle the form change event on the user interface to actually set the operator property
    this.registerOnFormChangeHandler();
  }

  /**
   * This method handles the form change event
   */
  registerOnFormChangeHandler(): void {
    // currently do nothing upon change, the change is confirmed with the `Add Breakpoint` button
  }

  hasBreakpoint(): boolean {
    if (!this.currentLinkId) {
      return false;
    }
    return this.workflowActionService.getTexeraGraph().getLinkBreakpoint(this.currentLinkId) !== undefined;
  }

  handleAddBreakpoint() {
    if (this.currentLinkId && this.workflowActionService.getTexeraGraph().hasLinkWithID(this.currentLinkId)) {
      this.workflowActionService.setLinkBreakpoint(this.currentLinkId, this.formData);
      if (
        this.executeWorkflowService.getExecutionState().state === ExecutionState.Paused ||
        this.executeWorkflowService.getExecutionState().state === ExecutionState.BreakpointTriggered
      ) {
        this.executeWorkflowService.addBreakpointRuntime(this.currentLinkId, this.formData);
      }
    }
  }

  /**
   * This method handles the link breakpoint remove button click event.
   * It will hide the property editor, clean up currentBreakpointInitialData.
   * Then unhighlight the link and remove it from the workflow.
   */
  handleRemoveBreakpoint() {
    if (this.currentLinkId) {
      // remove breakpoint in texera workflow first, then unhighlight it
      this.workflowActionService.removeLinkBreakpoint(this.currentLinkId);
      this.workflowActionService.getJointGraphWrapper().unhighlightLinks(this.currentLinkId);
    }
    this.clearPropertyEditor();
  }

  showBreakpointEditor(linkID: string): void {
    if (!this.workflowActionService.getTexeraGraph().hasLinkWithID(linkID)) {
      throw new Error("change property editor: link does not exist");
    }
    // set the operator data needed
    this.currentLinkId = linkID;
    const breakpointSchema = this.autocompleteService.getDynamicBreakpointSchema(linkID).jsonSchema;

    this.formTitle = "Breakpoint";
    const breakpoint = this.workflowActionService.getTexeraGraph().getLinkBreakpoint(linkID);
    this.formData = breakpoint !== undefined ? cloneDeep(breakpoint) : {};
    this.setFormlyFormBinding(breakpointSchema);

    const interactive =
      this.executeWorkflowService.getExecutionState().state in
      [
        ExecutionState.Uninitialized,
        ExecutionState.Paused,
        ExecutionState.BreakpointTriggered,
        ExecutionState.Completed,
      ];
    this.setInteractivity(interactive);
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

  /**
   * Hides the form and clears all the data of the current the property editor
   */
  clearPropertyEditor(): void {
    this.currentLinkId = undefined;
    this.formlyFormGroup = undefined;
    this.formData = undefined;
    this.formlyFields = undefined;
    this.formTitle = undefined;
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

  checkBreakpoint(formData: object): boolean {
    // check if the component is displaying breakpoint
    if (!this.currentLinkId) {
      return false;
    }
    // check if the link still exists
    const link = this.workflowActionService.getTexeraGraph().getLinkWithID(this.currentLinkId);
    if (!link) {
      return false;
    }
    // only emit change event if the form data actually changes
    return !isEqual(formData, this.workflowActionService.getTexeraGraph().getLinkBreakpoint(link.linkID));
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
      .getBreakpointChangeStream()
      .pipe(
        filter(_ => this.currentLinkId !== undefined),
        filter(event => event.linkID === this.currentLinkId),
        filter(
          event => !isEqual(this.formData, this.workflowActionService.getTexeraGraph().getLinkBreakpoint(event.linkID))
        )
      )
      .pipe(untilDestroyed(this))
      .subscribe(
        event =>
          (this.formData = cloneDeep(this.workflowActionService.getTexeraGraph().getLinkBreakpoint(event.linkID)))
      );
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
}

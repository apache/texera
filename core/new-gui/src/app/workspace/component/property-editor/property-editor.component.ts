import { OperatorSchema } from './../../types/operator-schema.interface';
import { OperatorPredicate, Breakpoint } from '../../types/workflow-common.interface';
import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { DynamicSchemaService } from '../../service/dynamic-schema/dynamic-schema.service';
import { Component } from '@angular/core';

import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';
import '../../../common/rxjs-operators';

import { cloneDeep, isEqual } from 'lodash';

import { JSONSchema7 } from 'json-schema';
import * as Ajv from 'ajv';

import { FormGroup } from '@angular/forms';
import { FormlyFormOptions, FormlyFieldConfig } from '@ngx-formly/core';
import { FormlyJsonschema } from '@ngx-formly/core/json-schema';
import { ExecuteWorkflowService, FORM_DEBOUNCE_TIME_MS } from '../../service/execute-workflow/execute-workflow.service';
import { ExecutionState, OperatorState } from '../../types/execute-workflow.interface';


/**
 * PropertyEditorComponent is the panel that allows user to edit operator properties.
 *
 * Property Editor uses JSON Schema to automatically generate the form from the JSON Schema of an operator.
 * For example, the JSON Schema of Sentiment Analysis could be:
 *  'properties': {
 *    'attribute': { 'type': 'string' },
 *    'resultAttribute': { 'type': 'string' }
 *  }
 * The automatically generated form will show two input boxes, one titled 'attribute' and one titled 'resultAttribute'.
 * More examples of the operator JSON schema can be found in `mock-operator-metadata.data.ts`
 * More about JSON Schema: Understading JSON Schema - https://spacetelescope.github.io/understanding-json-schema/
 *
 * OperatorMetadataService will fetch metadata about the operators, which includes the JSON Schema, from the backend.
 *
 * We use library `angular2-json-schema-form` to generate form from json schema
 * https://github.com/dschnelldavis/angular2-json-schema-form
 *
 * For more details of comparing different libraries, and the problems of the current library,
 *  see `json-schema-library.md`
 *
 * @author Zuozhi Wang
 */
@Component({
  selector: 'texera-property-editor',
  templateUrl: './property-editor.component.html',
  styleUrls: ['./property-editor.component.scss'],
})
export class PropertyEditorComponent {

  // debounce time for form input in miliseconds
  //  please set this to multiples of 10 to make writing tests easy
  public static formInputDebounceTime: number = FORM_DEBOUNCE_TIME_MS;

  // re-delcare enum for angular template to access it
  public readonly ExecutionState = ExecutionState;

  // operatorID if the component is displaying operator property editor
  public currentOperatorID: string | undefined;

  // the linkID if the component is displaying breakpoint editor
  public currentLinkID: string | undefined;

  // used in HTML template to control if the form is displayed
  public displayForm: boolean = false;

  // whether the editor can be edited
  public interactive: boolean = true;

  // the source event stream of form change triggered by library at each user input
  public sourceFormChangeEventStream = new Subject<object>();

  // the output form change event stream after debouce time and filtering out values
  public operatorPropertyChangeStream = this.createOutputFormChangeEventStream(
    this.sourceFormChangeEventStream, data => this.checkOperatorProperty(data));

  public breakpointChangeStream = this.createOutputFormChangeEventStream(
    this.sourceFormChangeEventStream, data => this.checkBreakpoint(data));

  // inputs and two-way bindings to formly component
  public formlyFormGroup: FormGroup | undefined;
  public formData: any;
  public formlyOptions: FormlyFormOptions | undefined;
  public formlyFields: FormlyFieldConfig[] | undefined;
  public formTitle: string | undefined;

  // show TypeInformation only when operator type is TypeCasting
  public showTypeCastingTypeInformation = false;

  // used to fill in default values in json schema to initialize new operator
  private ajv = new Ajv({ useDefaults: true });



  constructor(
    public formlyJsonschema: FormlyJsonschema,
    public workflowActionService: WorkflowActionService,
    public autocompleteService: DynamicSchemaService,
    public executeWorkflowService: ExecuteWorkflowService,
  ) {
    // listen to the autocomplete event, remove invalid properties, and update the schema displayed on the form
    this.handleOperatorSchemaChange();

    // when the operator's property is updated via program instead of user updating the json schema form,
    //  this observable will be responsible in handling these events.
    this.handleOperatorPropertyChange();

    // handle the form change event on the user interface to actually set the operator property
    this.handleOnFormChange();

    // handle highlight / unhighlight event to show / hide the property editor form
    this.handleHighlightEvents();

    this.handleDisableEditorInteractivity();

  }

  /**
   * Callback function provided to the Angular Json Schema Form library,
   *  whenever the form data is changed, this function is called.
   * It only serves as a bridge from a callback function to RxJS Observable
   * @param formData
   */
  public onFormChanges(event: object): void {
    this.sourceFormChangeEventStream.next(event);
  }

  public hasBreakpoint(): boolean {
    if (! this.currentLinkID) {
      return false;
    }
    return this.workflowActionService.getTexeraGraph().getLinkBreakpoint(this.currentLinkID) !== undefined;
  }

  public handleAddBreakpoint() {
    if (this.currentLinkID && this.workflowActionService.getTexeraGraph().hasLinkWithID(this.currentLinkID)) {
      this.workflowActionService.setLinkBreakpoint(this.currentLinkID, this.formData);
      if (this.executeWorkflowService.getExecutionState().state === ExecutionState.Paused ||
      this.executeWorkflowService.getExecutionState().state === ExecutionState.BreakpointTriggered) {
        this.executeWorkflowService.addBreakpointRuntime(this.currentLinkID, this.formData);
      }
    }
  }

  /**
   * This method handles the link breakpoint remove button click event.
   * It will hide the property editor, clean up currentBreakpointInitialData.
   * Then unhighlight the link and remove it from the workflow.
   */
  public handleRemoveBreakpoint() {
    if (this.currentLinkID) {
      // remove breakpoint in texera workflow first, then unhighlight it
      this.workflowActionService.removeLinkBreakpoint(this.currentLinkID);
      this.workflowActionService.getJointGraphWrapper().unhighlightLinks(this.currentLinkID);
    }
    this.clearPropertyEditor();
  }

  public allowChangeOperatorLogic() {
    this.setInteractivity(true);
  }

  public confirmChangeOperatorLogic() {
    this.setInteractivity(false);
    if (this.currentOperatorID) {
      this.executeWorkflowService.changeOperatorLogic(this.currentOperatorID);
    }
  }

  public setInteractivity(interactive: boolean) {
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
  public clearPropertyEditor(): void {
    // set displayForm to false in the very beginning
    // hide the view first and then make everything null
    this.displayForm = false;
    this.currentOperatorID = undefined;
    this.currentLinkID = undefined;

    this.formlyFormGroup = undefined;
    this.formData = undefined;
    this.formlyFields = undefined;
    this.formTitle = undefined;
  }

  public showBreakpointEditor(linkID: string): void {
    if (!this.workflowActionService.getTexeraGraph().hasLinkWithID(linkID)) {
      throw new Error(`change property editor: link does not exist`);
    }
    // set the operator data needed
    this.currentLinkID = linkID;
    const breakpointSchema = this.autocompleteService.getDynamicBreakpointSchema(linkID).jsonSchema;

    this.formTitle = 'Breakpoint';
    const breakpoint = this.workflowActionService.getTexeraGraph().getLinkBreakpoint(linkID);
    this.formData = breakpoint !== undefined ? cloneDeep(breakpoint) : {};
    this.setFormlyFormBinding(breakpointSchema);

    // show breakpoint editor
    this.displayForm = true;

    const interactive = this.executeWorkflowService.getExecutionState().state === ExecutionState.Uninitialized ||
      this.executeWorkflowService.getExecutionState().state === ExecutionState.Paused ||
      this.executeWorkflowService.getExecutionState().state === ExecutionState.BreakpointTriggered ||
      this.executeWorkflowService.getExecutionState().state === ExecutionState.Completed;
    this.setInteractivity(interactive);
  }

  /**
   * Changes the property editor to use the new operator data.
   * Sets all the data needed by the json schema form and displays the form.
   * @param operator
   */
  public showOperatorPropertyEditor(operator: OperatorPredicate): void {
    // set the operator data needed
    this.currentOperatorID = operator.operatorID;
    const currentOperatorSchema = this.autocompleteService.getDynamicSchema(this.currentOperatorID);
    this.setFormlyFormBinding(currentOperatorSchema.jsonSchema);
    this.formTitle = currentOperatorSchema.additionalMetadata.userFriendlyName;

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

    // set displayForm to true in the end - first initialize all the data then show the view
    this.displayForm = true;

    const interactive = this.executeWorkflowService.getExecutionState().state === ExecutionState.Uninitialized ||
      this.executeWorkflowService.getExecutionState().state === ExecutionState.Completed;
    this.setInteractivity(interactive);
  }

  /**
   * Handles the form change event stream observable,
   *  which corresponds to every event the json schema form library emits.
   *
   * Applies rules that transform the event stream to trigger resonably and less frequently ,
   *  such as debounce time and distince condition.
   *
   * Then modifies the operator property to use the new form data.
   */
  public createOutputFormChangeEventStream(
    formChangeEvent: Observable<object>,
    modelCheck: (formData: object) => boolean
  ): Observable<object> {

    return formChangeEvent
      // set a debounce time to avoid events triggering too often
      //  and to circumvent a bug of the library - each action triggers event twice
      .debounceTime(PropertyEditorComponent.formInputDebounceTime)
      // .do(evt => console.log(evt))
      // don't emit the event until the data is changed
      .distinctUntilChanged()
      // .do(evt => console.log(evt))
      // don't emit the event if form data is same with current actual data
      // also check for other unlikely circumstances (see below)
      .filter(formData => modelCheck(formData))
      // share() because the original observable is a hot observable
      .share();

  }

  private checkOperatorProperty(formData: object): boolean {
    // check if the component is displaying operator property
    if (this.currentOperatorID === undefined) {
      return false;
    }
    // check if the operator still exists, it might be deleted during deboucne time
    const operator = this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorID);
    if (!operator) {
      return false;
    }
    // only emit change event if the form data actually changes
    if (isEqual(formData, operator.operatorProperties)) {
      return false;
    }
    return true;
  }

  private checkBreakpoint(formData: object): boolean {
    // check if the component is displaying breakpoint
    if (!this.currentLinkID) {
      return false;
    }
    // check if the link still exists
    const link = this.workflowActionService.getTexeraGraph().getLinkWithID(this.currentLinkID);
    if (!link) {
      return false;
    }
    // only emit change event if the form data actually changes
    if (isEqual(formData, this.workflowActionService.getTexeraGraph().getLinkBreakpoint(link.linkID))) {
      return false;
    }
    return true;
  }

  private handleDisableEditorInteractivity(): void {
    this.executeWorkflowService.getExecutionStateStream().subscribe(event => {
      if (this.currentOperatorID) {
        if (event.current.state === ExecutionState.Completed || event.current.state === ExecutionState.Failed) {
          this.setInteractivity(true);
        } else {
          this.setInteractivity(false);
        }
      }
    });
  }

  /**
   * This method handles the schema change event from autocomplete. It will get the new schema
   *  propagated from autocomplete and check if the operators' properties that users input
   *  previously are still valid. If invalid, it will remove these fields and triggered an event so
   *  that the user interface will be updated through handleOperatorPropertyChange() method.
   *
   * If the operator that experiences schema changed is the same as the operator that is currently
   *  displaying on the property panel, this handler will update the current operator schema
   *  to the new schema.
   */
  private handleOperatorSchemaChange(): void {
    this.autocompleteService.getOperatorDynamicSchemaChangedStream().subscribe(
      event => {
        if (event.operatorID === this.currentOperatorID) {
          const currentOperatorSchema = this.autocompleteService.getDynamicSchema(this.currentOperatorID);
          const operator = this.workflowActionService.getTexeraGraph().getOperator(event.operatorID);
          if (!operator) {
            throw new Error(`operator ${event.operatorID} does not exist`);
          }
          this.setFormlyFormBinding(currentOperatorSchema.jsonSchema);
        }
      }
    );
  }

  /**
   * This method captures the change in operator's property via program instead of user updating the
   *  json schema form in the user interface.
   *
   * For instance, when the input doesn't matching the new json schema and the UI needs to remove the
   *  invalid fields, this form will capture those events.
   */
  private handleOperatorPropertyChange(): void {
    this.workflowActionService.getTexeraGraph().getOperatorPropertyChangeStream()
      .filter(event => this.currentOperatorID !== undefined)
      .filter(operatorChanged => operatorChanged.operator.operatorID === this.currentOperatorID)
      .filter(operatorChanged => !isEqual(this.formData, operatorChanged.operator.operatorProperties))
      .subscribe(operatorChanged => {
        this.formData = cloneDeep(operatorChanged.operator.operatorProperties);

      });
    this.workflowActionService.getTexeraGraph().getBreakpointChangeStream()
      .filter(event => this.currentLinkID !== undefined)
      .filter(event => event.linkID === this.currentLinkID)
      .filter(event => !isEqual(this.formData, this.workflowActionService.getTexeraGraph().getLinkBreakpoint(event.linkID)))
      .subscribe(event => {
        this.formData = cloneDeep(this.workflowActionService.getTexeraGraph().getLinkBreakpoint(event.linkID));

      });
  }

  /**
   * This method handles the form change event and set the operator property
   *  in the texera graph.
   */
  private handleOnFormChange(): void {
    this.operatorPropertyChangeStream.subscribe(formData => {
      // set the operator property to be the new form data
      if (this.currentOperatorID) {
        const operator = this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorID);

        this.workflowActionService.setOperatorProperty(this.currentOperatorID, formData);
        this.workflowActionService.setOperatorProperty(this.currentOperatorID, cloneDeep(formData));
      }
    });
  }

  /**
   * This method changes the property editor according to how operators are highlighted on the workflow editor.
   *
   * Displays the form of the highlighted operator if only one operator is highlighted;
   * Displays the form of the link breakpoint if only one link is highlighted;
   * hides the form if no operator/link is highlighted or multiple operators and/or groups and/or links are highlighted.
   */
  private handleHighlightEvents() {
    Observable.merge(
      this.workflowActionService.getJointGraphWrapper().getJointOperatorHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorUnhighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointGroupHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointGroupUnhighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getLinkHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getLinkUnhighlightStream()
    ).subscribe(() => {
      const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
      const highlightedGroups = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs();
      const highlightLinks = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedLinkIDs();

      if (highlightedOperators.length === 1 && highlightedGroups.length === 0 && highlightLinks.length === 0) {
        const operator = this.workflowActionService.getTexeraGraph().getOperator(highlightedOperators[0]);
        this.clearPropertyEditor();
        this.showOperatorPropertyEditor(operator);
      } else if (highlightLinks.length === 1 && highlightedGroups.length === 0 && highlightedOperators.length === 0) {
        this.clearPropertyEditor();
        this.showBreakpointEditor(highlightLinks[0]);
      } else {
        this.clearPropertyEditor();
      }
    });
  }

  private setFormlyFormBinding(schema: JSONSchema7) {
    // intercept JsonSchema -> FormlySchema process, adding custom options
    const jsonSchemaMapIntercept = (mappedField: FormlyFieldConfig, mapSource: JSONSchema7): FormlyFieldConfig => {
      // if the title contains "password", then make the field type also to be password
      if (mapSource?.title?.toLowerCase()?.includes('password')) {
        if (mappedField.templateOptions) {
          mappedField.templateOptions.type = 'password';
        }
      }
      // if the title is boolean expression (for Mysql source), then make the field to textarea with 5 rows
      if (mapSource?.title?.toLowerCase() === 'boolean expression') {
        if (mappedField.type) {
          mappedField.type = 'textarea';
        }
        if (mappedField.templateOptions) {
          mappedField.templateOptions.rows = 5;
        }
      }
      // if the title is python script (for Python UDF), then make this field a custom template 'codearea'
      if (mapSource?.description?.toLowerCase() === 'input your code here') {
        if (mappedField.type) {
          mappedField.type = 'codearea';
        }
      }
      return mappedField;
    };

    this.formlyFormGroup = new FormGroup({});
    this.formlyOptions = {};
    const field = this.formlyJsonschema.toFieldConfig(schema, { map: jsonSchemaMapIntercept });
    field.hooks = {
      onInit: (fieldConfig) => {
        if (!this.interactive) {
          fieldConfig?.form?.disable();
        }
      }
    };

    this.formlyFields = [field];
  }


}

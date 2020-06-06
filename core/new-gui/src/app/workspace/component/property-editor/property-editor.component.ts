import { OperatorPredicate } from '../../types/workflow-common.interface';
import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { DynamicSchemaService } from '../../service/dynamic-schema/dynamic-schema.service';
import { Component, ChangeDetectorRef } from '@angular/core';

import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';
import '../../../common/rxjs-operators';

import { cloneDeep, isEqual } from 'lodash';

import { JSONSchema7 } from 'json-schema';

import { FormGroup } from '@angular/forms';
import { FormlyFormOptions } from '@ngx-formly/core';
import { FormlyJsonschema } from '@ngx-formly/core/json-schema';
import { environment } from 'src/environments/environment';

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
  public static formInputDebounceTime: number = 150;

  // used in HTML template to control if the form is displayed
  public displayForm: boolean = false;

  // inputs and two-way bindings to formly component
  public formlyFormGroup: FormGroup = new FormGroup({});
  public formlyOptions: FormlyFormOptions = {};

  public formData: any;
  public jsonSchema: JSONSchema7 | undefined;

  // the source event stream of form change triggered by library at each user input
  public formChanged = new Subject<any>();
  // // the output form change event stream after debouce time
  // public outputFormChangeEventStream = this.createOutputFormChangeEventStream(this.formChanged);
  // whether listen to the model change and update the form
  public listenModelChange: boolean = true;


  // the operatorID corresponds to the operator property editor
  public currentOperatorID: string | undefined;

  // the linkID corresponds to the breakpoint editor
  public currentLinkID: string | undefined;


  constructor(
    public formlyJsonschema: FormlyJsonschema,
    public workflowActionService: WorkflowActionService,
    public autocompleteService: DynamicSchemaService,
    public ref: ChangeDetectorRef
  ) {
    // listen to the autocomplete event, remove invalid properties, and update the schema displayed on the form
    this.handleOperatorSchemaChange();

    // when the operator's property is updated via program instead of user updating the json schema form,
    //  this observable will be responsible in handling these events.
    this.handleOperatorPropertyChange();

    // handle highlight / unhighlight event to show / hide the property editor form
    this.handleOperatorHighlightEvents();

    this.updateOperatorPropertyOnFormChange();

    if (environment.linkBreakpointEnabled) {
      // handle link highlight / unhighlight event to show / hide the breakpoint property editor form
      this.handleLinkHighlight();

      this.updateBreakpointPropertyOnFormChange();

      // // handle the breakpoint form change event on the user interface to set breakpoint property
      // this.handleOnBreakpointPropertyChange();
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
    this.formData = undefined;
    this.jsonSchema = undefined;
  }

  /**
   * Handles the operator highlight / unhighlight events.
   *
   * When operators are highlighted / unhighlighted,
   *   -> displays the form of the highlighted operator if only one operator is highlighted
   *   -> hides the form otherwise
   */
  public handleOperatorHighlightEvents() {
    Observable.merge(
      this.workflowActionService.getJointGraphWrapper().getJointCellHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointCellUnhighlightStream()
    ).subscribe(() => {
      // Displays the form of the highlighted operator if only one operator is highlighted;
      // hides the form if no operator is highlighted or multiple operators are highlighted.
      const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
      if (highlightedOperators.length === 1) {
        const operator = this.workflowActionService.getTexeraGraph().getOperator(highlightedOperators[0]);
        this.clearPropertyEditor();
        this.showOperatorPropertyEditor(operator);
      } else {
        this.clearPropertyEditor();
      }
    });
  }

  /**
   * Changes the property editor to use the new operator data.
   * Sets all the data needed by the json schema form and displays the form.
   * @param operator
   */
  public showOperatorPropertyEditor(operator: OperatorPredicate): void {
    // set the operator data needed
    this.currentOperatorID = operator.operatorID;
    this.jsonSchema = this.autocompleteService.getDynamicSchema(this.currentOperatorID).jsonSchema;

    /**
     * Important: make a deep copy of the initial property data object.
     * this is to avoid angular two-way binding directly reference the object in service
     */
    this.formData = cloneDeep(operator.operatorProperties);

    // set displayForm to true in the end to show the view
    this.displayForm = true;

    // manually trigger a change detection - formly does not emit formChanged when it fills default value
    // and this can cause an inconsistency between the model in component and service
    this.ref.detectChanges();
    this.formChanged.next(this.formData);
  }

  public showBreakpointPropertyEditor(linkID: string): void {
    if (!this.workflowActionService.getTexeraGraph().hasLinkWithID(linkID)) {
      throw new Error(`change property editor: link does not exist`);
    }
    // set the operator data needed
    this.currentLinkID = linkID;
    this.jsonSchema = this.autocompleteService.getDynamicBreakpointSchema(linkID).jsonSchema;
    this.formData = this.workflowActionService.getTexeraGraph().getLinkBreakpoint(linkID);

    // show breakpoint editor
    this.displayForm = true;
  }

  // Handle link highlight event
  // On highlight -> clean up current Property editor and display breakpoint editor
  public handleLinkHighlight() {
    this.workflowActionService.getJointGraphWrapper().getLinkHighlightStream()
      .subscribe(linkID => {
        // TODO: fix this, this should not be handled here
        // this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()
        //       .forEach(operatorID => this.workflowActionService.getJointGraphWrapper().unhighlightOperator(operatorID));
        this.clearPropertyEditor();
        this.showBreakpointPropertyEditor(linkID.linkID);
      });
    this.workflowActionService.getJointGraphWrapper().getLinkUnhighlightStream()
      .filter(linkID => this.currentLinkID !== undefined && this.currentLinkID === linkID.linkID)
      .subscribe(linkID => this.clearPropertyEditor());
  }

  public updateOperatorPropertyOnFormChange() {
    this.formChanged
      .filter(formData => this.currentOperatorID !== undefined)
      .map(formData => ({operatorID: this.currentOperatorID as string, formData}))
      // set a debounce time to avoid events triggering too often
      //  and to circumvent a bug of the library - each action triggers event twice
      .debounceTime(PropertyEditorComponent.formInputDebounceTime)
      // don't emit the event until the data is changed
      .distinctUntilChanged()
      .subscribe(event => {
        // check if the operator still exists, as it can be deleted during deboucne time
        if (!this.workflowActionService.getTexeraGraph().hasOperator(event.operatorID)) {
          return;
        }
        // set the operator property to be the new form data
        this.listenModelChange = false;
        this.workflowActionService.setOperatorProperty(event.operatorID, event.formData);
        this.listenModelChange = true;
      });
  }

  public updateBreakpointPropertyOnFormChange() {
    this.formChanged
      .filter(formData => this.currentLinkID !== undefined)
      .map(formData => ({linkID: this.currentLinkID as string, formData}))
      // set a debounce time to avoid events triggering too often
      //  and to circumvent a bug of the library - each action triggers event twice
      .debounceTime(PropertyEditorComponent.formInputDebounceTime)
      // don't emit the event until the data is changed
      .distinctUntilChanged()
      .subscribe(event => {
        // check if the operator still exists, as it can be deleted during deboucne time
        if (!this.workflowActionService.getTexeraGraph().getLinkBreakpoint(event.linkID)) {
          return;
        }
        // set the operator property to be the new form data
        this.listenModelChange = false;
        this.workflowActionService.setLinkBreakpoint(event.linkID, event.formData);
        this.listenModelChange = true;
      });
  }

  // /**
  //  * This method handles the link breakpoint remove button click event.
  //  * It will hide the property editor, clean up currentBreakpointInitialData.
  //  * Then unhighlight the link and remove it from the workflow.
  //  */
  // public handleLinkBreakpointRemove() {
  //   if (this.currentLinkID) {
  //     // remove breakpoint in texera workflow first, then unhighlight it
  //     this.workflowActionService.removeLinkBreakpoint(this.currentLinkID);
  //     this.workflowActionService.getJointGraphWrapper().unhighlightLink(this.currentLinkID);
  //   }
  //   this.currentLinkID = undefined;
  // }

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
    this.autocompleteService.getOperatorDynamicSchemaChangedStream()
      .filter(event => event.operatorID === this.currentOperatorID)
      .subscribe(
        event => this.jsonSchema = this.currentOperatorID ?
          this.autocompleteService.getDynamicSchema(this.currentOperatorID).jsonSchema : undefined
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
      .filter(event => this.listenModelChange)
      .filter(operatorChanged => operatorChanged.operator.operatorID === this.currentOperatorID)
      .filter(operatorChanged => !isEqual(this.formData, operatorChanged.operator.operatorProperties))
      .subscribe(operatorChanged => {
        this.formData = cloneDeep(operatorChanged.operator.operatorProperties);
      });
  }

}

import { OperatorSchema } from './../../types/operator-schema.interface';
import { OperatorPredicate } from '../../types/workflow-common.interface';
import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { OperatorMetadataService } from './../../service/operator-metadata/operator-metadata.service';
import { Component, OnInit } from '@angular/core';

import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';
import '../../../common/rxjs-operators';

// all lodash import should follow this parttern
// import `functionName` from `lodash-es/functionName`
// to import only the function that we use
import cloneDeep from 'lodash-es/cloneDeep';
import isEqual from 'lodash-es/isEqual';


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
 * There are also many other libraries available. Here's the metrics to compare them:
 *  if it's written for Angular 2+
 *    (if it's not for Angular 2+, then if it's in pure Javascript,
 *      we want to avoid libraries for other frameworks, such as Angular 1 or React)
 *  if it supports Typescript
 *    (if the library isn't written for Angular 2+. Libraries for Angular 2 are written in Typescript)
 *  popularity, how many stars on github
 *  maintenance and activeness, if the library author is actively maintaining it
 *  user-friendliness and looks of the form it generated
 *
 * Here are the options:
 *
 * There are only 2 libraries written for Angular 2+:
 *  - `dschnelldavis/angular2-json-schema-form` (our choice) https://github.com/dschnelldavis/angular2-json-schema-form
 *  - `makinacorpus/ngx-schema-form`: https://github.com/makinacorpus/ngx-schema-form
 * Comparing two libraries:
 *  popularity: about the same (200+ stars)
 *  maintenance: both are *very* poorly maintained - hasn't updated for months and author hardly reply on Github Issues
 *  user-friendliness and look: our choice generates prettier looks and it allows switching themes
 *
 * Other Libraries:
 *  * `json-schema-form/angular-schema-form`: https://github.com/json-schema-form/angular-schema-form
 *    + very popular: 2000+ stars
 *    + authors are actively maintaining it
 *    + form looks good
 *    - written for Angular 1, possibly very hard to port
 *    - although authors showed intent to write an Angular 2 version (2 years ago), the progress is very slow
 *  * `jdorn/json-editor`: https://github.com/jdorn/json-editor
 *    + very popular: 4000+ stars
 *    + form looks okay
 *    - written in pure Javascript
 *    - original author no longer maintains it
 *    - primary fork maintained by community: https://github.com/json-editor/json-editor
 *  * `joshfire/jsonform`: https://github.com/joshfire/jsonform
 *    + popular: 1000+ stars
 *    - written in pure Javascript
 *    - last update is 5 years ago
 *  * `gitana/alpaca`: https://github.com/gitana/alpaca
 *    - popuarl: 900+ stars
 *    - written in jQuery
 *    - not actively maintained: 300+ issues left open
 *
 * In conclusion, among all the potential libraries, there's only 2 for Angular 2+, most of them are not actively maintained.
 *
 * If our current choice cannot satisfy our need or blocked our way of updating Angular,
 *  we are open to:
 *  switch to another library:
 *    - `makinacorpus/ngx-schema-form`: recently added a PR to support Angular 6
 *    - `json-schema-form/angular-schema-form`: spend time integrating Angular 1 library into our app
 *  or make a fork of the current library:
 *    - many people have forked the library to meet their need
 *    https://github.com/dschnelldavis/angular2-json-schema-form/pull/230#issuecomment-383591628
 *
 */
@Component({
  selector: 'texera-property-editor',
  templateUrl: './property-editor.component.html',
  styleUrls: ['./property-editor.component.scss'],
})
export class PropertyEditorComponent {

  // the operatorID corresponds to the property editor's current operator
  public currentOperatorID: string | undefined;
  // a *copy* of the operator property as the initial input to the json schema form
  // see details of why making a copy below at where the copy is made
  public currentOperatorInitialData: object | undefined;
  // the operator schema of the current operator
  public currentOperatorSchema: OperatorSchema | undefined;
  // used in HTML template to control if the form is displayed
  public displayForm: boolean = false;

  // the form layout passed to angular json schema library to hide *submit* button
  public formLayout: object = PropertyEditorComponent.generateFormLayout();

  // the observable event stream of the property change is triggered in the form
  public jsonSchemaOnFormChangeStream = new Subject<object>();
  // the current operator schema list, used to find the operator schema of current operator
  public operatorSchemaList: ReadonlyArray<OperatorSchema> = [];


  constructor(
    private operatorMetadataService: OperatorMetadataService,
    private workflowActionService: WorkflowActionService
  ) {
    this.operatorMetadataService.getOperatorMetadata().subscribe(
      value => { this.operatorSchemaList = value.operators; }
    );

    this.handleHighlightEvents();
    this.handleFormChangeEventStream();
  }

  /**
   * Callback function provided to the Angular Json Schema Form library,
   *  whenever the form data is changed, this function is called.
   * It only serves as a bridge from a callback function to RxJS Observable
   * @param formData
   */
  public onFormChanges(formData: object): void {
    this.jsonSchemaOnFormChangeStream.next(formData);
  }

  /**
   * Hides the form and clears all the data of the current the property editor
   */
  public clearPropertyEditor(): void {
    // set displayForm to false in the very beginning
    // hide the view first and then make everything null
    this.displayForm = false;

    this.currentOperatorID = undefined;
    this.currentOperatorInitialData = undefined;
    this.currentOperatorSchema = undefined;

  }

  /**
   * Changes the property editor to use the new operator data.
   * Sets all the data needed by the json schema form and displays the form.
   * @param operator
   */
  public changePropertyEditor(operator: OperatorPredicate | undefined): void {
    if (! operator) {
      throw new Error(`change property editor: operator is undefined`);
    }

    // set displayForm to false first to hide the view while constructing data
    this.displayForm = false

    // set the operator data needed
    this.currentOperatorID = operator.operatorID;
    this.currentOperatorSchema = this.operatorSchemaList.find(schema => schema.operatorType === operator.operatorType);
    if (! this.currentOperatorSchema) {
      throw new Error(`operator schema for operator type ${operator.operatorType} doesn't exist`)
    }
    /**
     * Make a deep copy of the initial property data object.
     * It's important to make a deep copy. If it's a reference to the operator's property object,
     *  form change event -> property object change -> input to form change -> form change event
     *  although the it falls into an infinite loop of tirggering events.
     * Making a copy prevents that property object change triggers the input to the form changes.
     *
     * Although currently other methods also prevent this to happen, it's still good to explicitly make a copy.
     *  - now the operator property object is immutable, meaning a new property object is construct to replace the old one,
     *      instead of directly mutating the same object reference
     *  - now the formChange event handler checks if the new formData is equal to the current operator data,
     *      which prevents the
     */
    this.currentOperatorInitialData = cloneDeep(operator.operatorProperties);

    // set displayForm to true in the end - first initialize all the data then show the view
    this.displayForm = true;
  }

  /**
   * Handles the highlight / unhighlight events.
   * On highlight -> display the form of the highlighted operator
   * On unhighlight -> hides the form
   */
  private handleHighlightEvents() {
    this.workflowActionService.getJointGraphWrapper().getJointCellHighlightStream()
      .filter(value => value.operatorID !== this.currentOperatorID)
      .map(value => this.workflowActionService.getTexeraGraph().getOperator(value.operatorID))
      .subscribe(
        operator => this.changePropertyEditor(operator)
      );

    this.workflowActionService.getJointGraphWrapper().getJointCellUnhighlightStream()
      .filter(value => value.operatorID === this.currentOperatorID)
      .subscribe(value => this.clearPropertyEditor());
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
  private handleFormChangeEventStream(): void {

    this.jsonSchemaOnFormChangeStream
      // set a debounce time to avoid events triggering too often
      //  and to circumvent a bug of the library - each action triggers event twice
      .debounceTime(175)
      // don't emit the event until the data is changed
      .distinctUntilChanged()
      // don't emit the event if form data is same with current actual data
      // also check for other unlikely circumstances (see below)
      .filter(formData => {
        // check if the current operator ID still exists
        // the user could un-select this operator during deboucne time
        if (!this.currentOperatorID) {
          return false;
        }
        // check if the operator still exists
        // the operator could've been deleted during deboucne time
        const operator = this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorID);
        if (!operator) {
          return false;
        }
        // don't emit event if the form data is equal to actual current property
        // this is to circumvent the library's behavior
        // when the form is initialized, the change event is triggered for the inital data
        // however, the operator property is not changed and shouldn't emit this event
        return ! isEqual(formData, operator.operatorProperties);
      })
      .subscribe(
        formData => {
          // set the operator property to be the new form data
          if (this.currentOperatorID) {
            this.workflowActionService.setOperatorProperty(this.currentOperatorID, formData);
          }
        }
      );
  }

  /**
   * Generates a form layout used by the json schema form library
   *  to hide the *submit* button.
   * ttps://github.com/json-schema-form/angular-schema-form/blob/master/docs/index.md#form-definitions
   */
  private static generateFormLayout(): object {
    return [
      '*',
      { type: 'submit', condition: 'false' }
    ];
  }

}

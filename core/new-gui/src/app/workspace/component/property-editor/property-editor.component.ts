import { OperatorSchema } from './../../types/operator-schema.interface';
import { OperatorPredicate } from '../../types/workflow-common.interface';
import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { DynamicSchemaService } from '../../service/dynamic-schema/dynamic-schema.service';
import { Component } from '@angular/core';

import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';
import '../../../common/rxjs-operators';

// all lodash import should follow this parttern
// import `functionName` from `lodash-es/functionName`
// to import only the function that we use
import cloneDeep from 'lodash-es/cloneDeep';
import isEqual from 'lodash-es/isEqual';
import { IndexableObject } from '../../types/result-table.interface';


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

  // the source event stream of form change triggered by library at each user input
  public sourceFormChangeEventStream = new Subject<object>();

  // the output form change event stream after debouce time and filtering out values
  public outputFormChangeEventStream = this.createOutputFormChangeEventStream(this.sourceFormChangeEventStream);

  // the current operator schema list, used to find the operator schema of current operator
  public operatorSchemaList: ReadonlyArray<OperatorSchema> = [];

   // the map of property description (key = property name, value = property description)
  public propertyDescription: Map<String, String> = new Map();

   // boolean to display the property description button
  public hasPropertyDescription: boolean = false;

  // the operator data need to be stored if the Json Schema changes, else the currently modified changes will be lost
  public cachedFormData: object | undefined;

  constructor(
    private workflowActionService: WorkflowActionService,
    private autocompleteService: DynamicSchemaService
  ) {
    // subscribe to operator schema information (with source tables names added to source operators' table name properties)
    // this.autocompleteService.getSourceTableAddedOperatorMetadataObservable().subscribe(
    //   metadata => { this.operatorSchemaList = metadata.operators; }
    // );

    // listen to the autocomplete event, remove invalid properties, and update the schema displayed on the form
    this.handleOperatorSchemaChange();

    // when the operator's property is updated via program instead of user updating the json schema form,
    //  this observable will be responsible in handling these events.
    this.handleOperatorPropertyChange();

    // handle the form change event on the user interface to actually set the operator property
    this.handleOnFormChange();

    // handle highlight / unhighlight event to show / hide the property editor form
    this.handleHighlightEvents();

  }

  /**
   * Callback function provided to the Angular Json Schema Form library,
   *  whenever the form data is changed, this function is called.
   * It only serves as a bridge from a callback function to RxJS Observable
   * @param formData
   */
  public onFormChanges(formData: object): void {
    this.sourceFormChangeEventStream.next(formData);
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
    if (!operator) {
      throw new Error(`change property editor: operator is undefined`);
    }

    // set displayForm to false first to hide the view while constructing data
    this.displayForm = false;

    // set the operator data needed
    this.currentOperatorID = operator.operatorID;
    this.currentOperatorSchema = this.autocompleteService.getDynamicSchema(this.currentOperatorID);
    if (!this.currentOperatorSchema) {
      throw new Error(`operator schema for operator type ${operator.operatorType} doesn't exist`);
    }

    // handler to show operator detail description button or not
    this.handleOperatorPropertyDescription(this.currentOperatorSchema);

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
    // when operator in the property editor changes, the cachedFormData should also be changed
    this.cachedFormData = this.currentOperatorInitialData;
    // set displayForm to true in the end - first initialize all the data then show the view
    this.displayForm = true;
  }

  /**
   * Handles the highlight / unhighlight events.
   * On highlight -> display the form of the highlighted operator
   * On unhighlight -> hides the form
   */
  public handleHighlightEvents() {
    this.workflowActionService.getJointGraphWrapper().getJointCellHighlightStream()
      .filter(value => value.operatorID !== this.currentOperatorID)
      .map(value => this.workflowActionService.getTexeraGraph().getOperator(value.operatorID))
      .subscribe(
        operator => this.changePropertyEditor(operator)
      );

    this.workflowActionService.getJointGraphWrapper().getJointCellUnhighlightStream()
      .filter(value => value.operatorID === this.currentOperatorID)
      .subscribe(() => this.clearPropertyEditor());
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
  public createOutputFormChangeEventStream(originalSourceFormChangeEvent: Observable<object>): Observable<object> {

    return originalSourceFormChangeEvent
      // set a debounce time to avoid events triggering too often
      //  and to circumvent a bug of the library - each action triggers event twice
      .debounceTime(PropertyEditorComponent.formInputDebounceTime)
      // don't emit the event until the data is changed
      .distinctUntilChanged()
      // don't emit the event if form data is same with current actual data
      // also check for other unlikely circumstances (see below)
      .filter(formData => {
        // check if the current operator ID still exists
        // the user could un-select this operator during debounce time
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
        if (isEqual(formData, operator.operatorProperties)) {
          return false;
        }

        // this checks whether formData and cachedFormData will have the same appearance when rendered in the form
        if (this.secondCheckPropertyEqual(formData as IndexableObject, this.cachedFormData as IndexableObject)) {
          return false;
        }

        return true;
      })
      // share() because the original observable is a hot observable
      .share();

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
          this.currentOperatorSchema = this.autocompleteService.getDynamicSchema(this.currentOperatorID);
        }
      }
    );
  }

  /**
   * This function is a handler for displaying property description option on the property panel
   *
   * The if-else block will prevent undeclared property description to be displayed on the UI
   *
   * @param currentOperatorSchema
   */
  private handleOperatorPropertyDescription(currentOperatorSchema: OperatorSchema): void {
    if (currentOperatorSchema.additionalMetadata.propertyDescription !== undefined) {
      this.propertyDescription = new Map(Object.entries(currentOperatorSchema.additionalMetadata.propertyDescription));
      this.hasPropertyDescription = true;
    } else {
      this.propertyDescription = new Map();
      this.hasPropertyDescription = false;
    }
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
      .filter(operatorChanged => operatorChanged.operator.operatorID === this.currentOperatorID)
      .filter(operatorChanged => !isEqual(this.cachedFormData, operatorChanged.operator.operatorProperties))
      .subscribe(operatorChanged => {
        this.currentOperatorInitialData = cloneDeep(operatorChanged.operator.operatorProperties);
        this.cachedFormData = this.currentOperatorInitialData;
      });
  }


  /**
   * This method is serve as the second check to determine if the form data is equal to the
   *  cached form data that might be changed by system instead of user changing in property panel.
   *
   * This method handles the edge case where isEqual() thinks an empty array does not equal to undefined
   *  in the form properties. For instance, in isEqual(), if
   *
   *  formData = {attributes: Array(0) or []}
   *  cachedFormData = {}
   *
   * isEqual() will return false, while both of these look the same when it is rendered in the property panel.
   *
   * This method is mainly for checking whether 2 properties will have the same appearance when it is rendered
   *  in the JsonSchemaForm.
   *
   * @param property1 property from the current form
   * @param property2 property from the current cached form
   */
  private secondCheckPropertyEqual(property1: IndexableObject, property2: IndexableObject): boolean {
    let isPropertiesEqual = true;

    const propertyOneKeys = Object.keys(property1);
    const propertyTwoKeys = Object.keys(property2);

    // keys exist in both properties
    const keyIntersections = propertyOneKeys.filter(key => propertyTwoKeys.includes(key));

    // check whether the values with these keys are equal
    keyIntersections.forEach(key => {
      if (!isEqual(property1[key], property2[key])) {
        isPropertiesEqual = false;
      }
    });

    if (!isPropertiesEqual) { return isPropertiesEqual; }

    // difference between properties
    const keysDifference = propertyOneKeys
      .filter(key => !propertyTwoKeys.includes(key))
      .concat(propertyTwoKeys.filter(key => !propertyOneKeys.includes(key)));

    /**
     * This part will check all the key-value pairs existing only in one
     *  of the 2 properties. If the value is list and the length is not 0,
     *  it means 2 properties are different. If length = 0 for an Array,
     *  it will be the same as having undefined. This property holds same
     *  for object type. If the key-value pair is not an object or array,
     *  it means it is a regular data type and set isPropertiesEqual to false
     *  immediately.
     */
    keysDifference.forEach(key => {
      const value1 = property1[key];
      const value2 = property2[key];
      if (value1) {
        if (Array.isArray(value1)) {
          if (value1.length !== 0) { isPropertiesEqual = false; }
        } else if (typeof value1 === 'object') {
          if (Object.keys(value1).length !== 0) { isPropertiesEqual = false; }
        } else { isPropertiesEqual = false; }
      } else {
        if (Array.isArray(value2)) {
          if (value2.length !== 0) { isPropertiesEqual = false; }
        } else if (typeof value2 === 'object') {
          if (Object.keys(value2).length !== 0) { isPropertiesEqual = false; }
        } else { isPropertiesEqual = false; }
      }
    });

    return isPropertiesEqual;
  }

  /**
   * This method handles the form change event and set the operator property
   *  in the texera graph.
   */
  private handleOnFormChange(): void {
    this.outputFormChangeEventStream
      .subscribe(formData => {
      // set the operator property to be the new form data
      if (this.currentOperatorID) {
        this.cachedFormData = formData;
        this.workflowActionService.setOperatorProperty(this.currentOperatorID, formData);
      }
    });
  }

  /**
   * Generates a form layout used by the json schema form library
   *  to hide the *submit* button.
   * https://github.com/json-schema-form/angular-schema-form/blob/master/docs/index.md#form-definitions
   */
  private static generateFormLayout(): object {
    return [
      '*',
      { type: 'submit', condition: 'false' }
    ];
  }

}

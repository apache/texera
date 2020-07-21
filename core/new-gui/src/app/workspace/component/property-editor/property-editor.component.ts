import { OperatorSchema } from './../../types/operator-schema.interface';
import { OperatorPredicate } from '../../types/workflow-common.interface';
import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { DynamicSchemaService } from '../../service/dynamic-schema/dynamic-schema.service';
import { Component, ChangeDetectorRef } from '@angular/core';

import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';
import '../../../common/rxjs-operators';

import { cloneDeep, isEqual} from 'lodash';

import { JSONSchema7 } from 'json-schema';

import { FormGroup } from '@angular/forms';
import { FormlyFormOptions, FormlyFieldConfig } from '@ngx-formly/core';
import { FormlyJsonschema } from '@ngx-formly/core/json-schema';

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

  // the operator schemas of the current operator
  public currentOperatorSchema: OperatorSchema | undefined;

  // used in HTML template to control if the form is displayed
  public displayForm: boolean = false;

  // the source event stream of form change triggered by library at each user input
  public sourceFormChangeEventStream = new Subject<object>();

  // the output form change event stream after debouce time and filtering out values
  public outputFormChangeEventStream = this.createOutputFormChangeEventStream(this.sourceFormChangeEventStream);

  // inputs and two-way bindings to formly component
  public formlyFormGroup: FormGroup | undefined;
  public formData: any;
  public formlyOptions: FormlyFormOptions | undefined;
  public formlyFields: FormlyFieldConfig[] | undefined;

  constructor(
    private formlyJsonschema: FormlyJsonschema,
    private workflowActionService: WorkflowActionService,
    private autocompleteService: DynamicSchemaService,
    private ref: ChangeDetectorRef
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
    this.currentOperatorSchema = undefined;
    this.formlyFormGroup = undefined;
    this.formData = undefined;
    this.formlyFields = undefined;
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

    this.convertJsonSchemaToNGXField(this.currentOperatorSchema.jsonSchema);

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
    this.formData = cloneDeep(operator.operatorProperties);

    // set displayForm to true in the end - first initialize all the data then show the view
    this.displayForm = true;

    // manually trigger a change detection to force formly to process the form
    // this is because formly does not emit an onChanges event for filling default values
    // and this might cause an inconsistency between the operator property in componenet and the service
    this.ref.detectChanges();
    this.sourceFormChangeEventStream.next(this.formData);
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
          const operator = this.workflowActionService.getTexeraGraph().getOperator(event.operatorID);
          if (! operator) {
            throw new Error(`operator ${event.operatorID} does not exist`);
          }
          this.convertJsonSchemaToNGXField(this.currentOperatorSchema.jsonSchema);
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
      .filter(operatorChanged => operatorChanged.operator.operatorID === this.currentOperatorID)
      .filter(operatorChanged => !isEqual(this.formData, operatorChanged.operator.operatorProperties))
      .subscribe(operatorChanged => {
        this.formData = cloneDeep(operatorChanged.operator.operatorProperties);
      });
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
        this.workflowActionService.setOperatorProperty(this.currentOperatorID, formData);
      }
    });
  }

  /**
   * Handles the operator highlight / unhighlight events.
   *
   * When operators are highlighted / unhighlighted,
   *   -> displays the form of the highlighted operator if only one operator is highlighted
   *   -> hides the form otherwise
   */
  private handleHighlightEvents() {
    this.workflowActionService.getJointGraphWrapper().getJointCellHighlightStream()
      .subscribe(() => this.changePropertyEditorOnHighlightEvents());
    this.workflowActionService.getJointGraphWrapper().getJointCellUnhighlightStream()
      .subscribe(() => this.changePropertyEditorOnHighlightEvents());
  }

  /**
   * This method changes the property editor according to how operators are highlighted on the workflow editor.
   *
   * Displays the form of the highlighted operator if only one operator is highlighted;
   * hides the form if no operator is highlighted or multiple operators are highlighted.
   */
  private changePropertyEditorOnHighlightEvents() {
    const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
    if (highlightedOperators.length === 1) {
      const operator = this.workflowActionService.getTexeraGraph().getOperator(highlightedOperators[0]);
      this.changePropertyEditor(operator);
    } else {
      this.clearPropertyEditor();
    }
  }


  private convertJsonSchemaToNGXField(schema: JSONSchema7) {
    this.formlyFormGroup = new FormGroup({});
    this.formlyOptions = {};

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
      return mappedField;
    };

    const field = this.formlyJsonschema.toFieldConfig(schema, {map: jsonSchemaMapIntercept});
    this.formlyFields = [field];
  }

}

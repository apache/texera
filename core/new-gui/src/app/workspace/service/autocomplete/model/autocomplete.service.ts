import { Injectable } from '@angular/core';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';

import { AppSettings } from '../../../../common/app-setting';
import { SourceTableNamesAPIResponse, AutocompleteSucessResult } from '../../../types/autocomplete.interface';

import { AutocompleteUtils } from '../util/autocomplete.utils';
import { OperatorMetadata, OperatorSchema } from '../../../types/operator-schema.interface';
import { OperatorMetadataService } from '../../operator-metadata/operator-metadata.service';
import { WorkflowActionService } from '../../workflow-graph/model/workflow-action.service';
import { Observable } from 'rxjs/Observable';
import '../../../../common/rxjs-operators';

import { combineLatest } from 'rxjs/observable/combineLatest';
import { Subject } from 'rxjs/Subject';
import isEqual from 'lodash-es/isEqual';

import { ExecuteWorkflowService } from '../../execute-workflow/execute-workflow.service';
import { OperatorPredicate } from '../../../types/workflow-common.interface';

export const SOURCE_TABLE_NAMES_ENDPOINT = 'resources/table-metadata';
export const AUTOMATED_SCHEMA_PROPAGATION_ENDPOINT = 'queryplan/autocomplete';

import * as Ajv from 'ajv';

/**
 * Autocomplete service is used for the purpose of automated schema propagation. The service does
 * following tasks: -
 *  1. Originally the source operators have tableName property which has no options. This service fetches
 *    source table names from backend and adds them as an enum array to the tableName property.
 *  2. The autocomplete API at backend is invoked using invokeSchemaPropagationAPI() method.
 */
@Injectable()
export class AutocompleteService {
  // the input schema of operators in the current workflow as returned by the autocomplete API.
  // This map will be used to get the propagated operator schema.
  // This is an example state of dynamicSchemaMap:
  // {
  //   'operatorID2' : newAutoCompleteGenerated_OperatorSchema,
  //   'operatorID3' : newAutoCompleteGenerated_OperatorSchema2
  // }
  private dynamicSchemaMap: Map<string, OperatorSchema> = new Map<string, OperatorSchema> ();

  // this stream is used to capture the event when json schema is changed
  private operatorSchemaChangedStream: Subject<string> = new Subject<string> ();

  // the operator schema list with source table names added in source operators
  private operatorSchemaList: ReadonlyArray<OperatorSchema> = [];

  // contacts the **backend** and gets the source tables at the server
  private sourceTableNamesObservable = this.httpClient
  .get<SourceTableNamesAPIResponse>(`${AppSettings.getApiEndpoint()}/${SOURCE_TABLE_NAMES_ENDPOINT}`)
  .map(response => AutocompleteUtils.processSourceTableAPIResponse(response))
  .shareReplay(1);

  constructor(private httpClient: HttpClient,
              private workflowActionService: WorkflowActionService,
              private operatorMetadataService: OperatorMetadataService) {

    this.getSourceTableAddedOperatorMetadataObservable().subscribe(
      metadata => { this.operatorSchemaList = metadata.operators; }
    );

    // property change event
    this.handleTexeraGraphPropertyChangeEvent();
    this.handleTexeraGraphLinkChangeEvent();

    // add / delete event
    this.handleOperatorAddEvent();
    this.handleOperatorDeleteEvent();
  }

  /**
   * This function returns the observable for operatorMetadata modified using the function addSourceTableNamesToMetadata. An important
   * part to note here is that the function uses an combineLatest which fires first when it receives one value from each observable
   * and thereafter a new value from any observable leads to refiring of the observables with the latest value fired by all the observables.
   * Thus, any new value in either of the observables will cause recomputation of the modifed operator schema.
   */
  public getSourceTableAddedOperatorMetadataObservable(): Observable<OperatorMetadata> {
    return  combineLatest(this.getSourceTablesNamesObservable(), this.operatorMetadataService.getOperatorMetadata())
            .map(([tableNames, operatorMetadata]) =>
            AutocompleteUtils.addSourceTableNamesToMetadata(operatorMetadata, tableNames));
  }

  /**
   * Returns the observable which outputs a string everytime the autocomplete API is
   * invoked and response is received successfully.
   */
  public getOperatorSchemaChangedStream(): Observable<string> {
    return this.operatorSchemaChangedStream.asObservable();
  }

  /**
   * Based on the operatorID, get the current dynamic operator schema that is created through autocomplete
   * @param operatorID The ID of an operator
   */
  public getDynamicSchema(operator: Readonly<OperatorPredicate>): OperatorSchema {
    const dynamicSchema = this.dynamicSchemaMap.get(operator.operatorID);
    if (dynamicSchema !== undefined) {
      return dynamicSchema;
    }
    const operatorSchema = this.operatorSchemaList.find(schema => schema.operatorType === operator.operatorType);
    if (!operatorSchema) {
      throw new Error(`operator schema for operator type ${operator.operatorType} doesn't exist`);
    }
    return  operatorSchema;
  }

  /**
   * Modifies the schema of the operator according to autocomplete information obtained from the backend (if autcomplete info
   * contains the operator id).
   * @param operatorSchema the original operator schema
   * @param inputs new input attributes for the operator schema
   */
  public generateAutoCompleteSchemaForOperator(operatorSchema: Readonly<OperatorSchema> | undefined,
      inputs: string[]) {
    if (!operatorSchema) {
      throw new Error(`operator schema doesn't exist`);
    }

    return AutocompleteUtils.addInputSchemaToOperatorSchema(operatorSchema, inputs);
  }

  /**
   * Used for automated propagation of input schema in workflow.
   *
   * When users are in the process of building a workflow, Texera can propagate schema forwards so
   * that users can easily set the properties of the next operator. For eg: If there are two operators Source:Scan and KeywordSearch and
   * a link is created between them, the attributed of the table selected in Source can be propagated to the KeywordSearch operator.
   */
  public invokeAutocompleteAPI(): void {
    // get the current workflow graph
    const workflowPlan = this.workflowActionService.getTexeraGraph();

    // create a Logical Plan based on the workflow graph
    const body = ExecuteWorkflowService.getLogicalPlanRequest(workflowPlan);
    const requestURL = `${AppSettings.getApiEndpoint()}/${AUTOMATED_SCHEMA_PROPAGATION_ENDPOINT}`;

    // make a http post request to the API endpoint with the logical plan object
    this.httpClient.post<AutocompleteSucessResult>(
      requestURL,
      JSON.stringify(body),
      { headers: { 'Content-Type': 'application/json' } })
      .subscribe(
        // backend will either respond an execution result or an error will occur
        // handle both cases
        response => this.handleExecuteResult(response),
        errorResponse => this.handleExecuteError(errorResponse)
      );
  }

    /**
   * This method uses `Another JSON Schema Validator` library to check if the data passed
   *  into the method satisfy the constraint set by the Json Schema for an operator
   *
   * https://github.com/epoberezkin/ajv
   *
   * @param schema json schema of an operator
   * @param data data to check
   */
  public validateJsonSchema(operator: OperatorPredicate, schema: OperatorSchema, data: object): void {
    const ajv = new Ajv({ schemaId : 'auto' });
    // only supports version 4 json schema currently
    ajv.addMetaSchema(require('ajv/lib/refs/json-schema-draft-04.json'));
    const valid = ajv.validate(schema.jsonSchema, data);
    if (!valid) {
      this.handleInvalidSchema(operator, ajv.errors);
    }
  }

  /**
   * This method handles the scenario when calling ajv.validate() on operator properties using
   *  new json schema return invalid. This method will fetch all the fields that contain errors,
   *  currently only 'attributes' and 'attribute', and create a new object that does not
   *  contains these fields generating errors.
   *
   * This method will call 'workflowActionService.setOperatorProperty()' to update the operator's property
   *
   * @param operator operator that does not satisfy the new schema
   * @param schemaError the schema error returned by Ajv
   */
  private handleInvalidSchema(operator: OperatorPredicate, schemaError: Ajv.ErrorObject[] | undefined): void {
    if (!schemaError) {
      throw new Error(`schema error does not exist for operator ${operator.operatorID}`);
    }
    const attribute_list_key_in_schemajson = 'attributes';
    const single_attribute_in_schemajson = 'attribute';
    const propertiesToRemove: Array<String> = [];
    schemaError.forEach(error => {
      const errorPath: string = error.dataPath;
      if (errorPath.includes(attribute_list_key_in_schemajson)) {
        propertiesToRemove.push(attribute_list_key_in_schemajson);
      } else if (errorPath.includes(single_attribute_in_schemajson)) {
        propertiesToRemove.push(single_attribute_in_schemajson);
      }
    });

    // create new object that does not include the key-value pairs to remove
    // property = key-value pair in the properties. Ex: {'matchingType' : 'scan'}
    const newProperties = Object.entries(operator.operatorProperties)
      .filter(property => !(propertiesToRemove.includes(property[0])))
      .reduce((newObject, property) => {
        return {
          ...newObject,
          [property[0]]: property[1]
        };
      }, {});

    // update the operator's properties
    this.workflowActionService.setOperatorProperty(operator.operatorID, newProperties);
  }

  /**
   * Handles valid execution result from the backend.
   * Updates the current autocompleted schema by checking
   *  1. does the input attributes existing in the dynamic schema map still exist in the new result?
   *      If not, revert the dynamic schema to the default operator schema
   *  2. does the input attributes coming from the new result differs from that of dynamic schema map?
   *      If it does differ, update dynamic schema to use the new schema generated by using the new
   *      input attributes passed from the backend response.
   * Both of these scenario will pass operator changed and its new schema to `operatorSchemaChangedStream`.
   *  At another end, it will check if the original data is valid for the newly generated operator Schema.
   *  If invalid, the properties of that operator inside the workflow will be cleared. Also, if the operator
   *  changed is the current operator in the property panel, the operator schema is the property panel will
   *  be reloaded.
   *
   * @param response response from the backend containing a map of operatorID to input attributes for autocomplete
   */
  private handleExecuteResult(response: AutocompleteSucessResult): void {
    this.dynamicSchemaMap.forEach((value, operatorID, map) => {
      const currentPredicate = this.workflowActionService.getTexeraGraph().getOperator(operatorID);
      if (!currentPredicate) {
        throw new Error(`operator predicate for operator ID ${operatorID} doesn't exist in handleExecuteResult`);
      }
      const operatorSchema = this.operatorSchemaList.find(schema => schema.operatorType === currentPredicate.operatorType);
      if (!operatorSchema) {
        throw new Error(`operator schema for operator type ${currentPredicate.operatorType} doesn't exist in handleExecuteResult`);
      }
      if (!response.result[operatorID]) {
        // case1: if the old attributes does not exist in the new attributes, update the dynamic schema to use
        //  default schema from the operatorSchemaList
        if (!isEqual(operatorSchema, this.dynamicSchemaMap.get(operatorID))) {
          this.dynamicSchemaMap.set(operatorID, operatorSchema);
          this.operatorSchemaChangedStream.next(operatorID);
        }
      } else {
        // case2: if the a newSchema is different from the dynamic Schema, update the dynamic schema
        const newSchema = this.generateAutoCompleteSchemaForOperator(operatorSchema, response.result[operatorID]);
        if (!isEqual(newSchema, this.dynamicSchemaMap.get(operatorID))) {
          this.dynamicSchemaMap.set(operatorID, newSchema);
          this.operatorSchemaChangedStream.next(operatorID);
        }
      }
    });
  }

    /**
   * Handler function for unsuccessful schema propagation API call.
   *
   * Logs the error message to console.
   *
   * @param errorResponse
   */
  private handleExecuteError(errorResponse: HttpErrorResponse): void {
    // error logged in console in different error scenarios
    const displayedErrorMessage = AutocompleteUtils.processErrorResponse(errorResponse);
    console.log(displayedErrorMessage.message);
  }

  /**
   * returns the observable which produces array of source table names
   */
  private getSourceTablesNamesObservable(): Observable<ReadonlyArray<string>> {
    return this.sourceTableNamesObservable;
  }

  /**
   * Handles any kind of changes in the links of the joint graph and invokes the autocomplete API.
   * There are 2 kinds of change streams exposed by texera graph wrapper - link add, link delete.
   */
  private handleTexeraGraphLinkChangeEvent(): void {
    Observable.merge(this.workflowActionService.getTexeraGraph().getLinkAddStream(),
      this.workflowActionService.getTexeraGraph().getLinkDeleteStream())
    .subscribe(() => this.invokeAutocompleteAPI());
  }


  /**
   * Handles any kind of changes in the operator properties. Whenever an operator's porperty is changed, for instance
   * a tableName is added to a source or spanList attribute name is added to Keyword Search, we invoke the auto-complete
   * API.
   */
  private handleTexeraGraphPropertyChangeEvent(): void {
    this.workflowActionService.getTexeraGraph().getOperatorPropertyChangeStream()
      .subscribe(() => this.invokeAutocompleteAPI());
  }

  /**
   * Handles the operator add event by adding the operator schema into the dynamic schema map.
   */
  private handleOperatorAddEvent(): void {
    this.workflowActionService.getTexeraGraph().getOperatorAddStream()
      .subscribe(operator => {
        const operatorSchema = this.operatorSchemaList.find(schema => schema.operatorType === operator.operatorType);
        if (!operatorSchema) {
          throw new Error(`operator schema for operator type ${operator.operatorType} doesn't exist in handleOperatorAddEvent`);
        }
        this.dynamicSchemaMap.set(operator.operatorID, operatorSchema);
      });
  }

  /**
   * Handles the operator delete event by removing the operator schema from the dynamic schema map.
   */
  private handleOperatorDeleteEvent(): void {
    this.workflowActionService.getTexeraGraph().getOperatorDeleteStream()
      .subscribe(operator => {
        if (this.dynamicSchemaMap.has(operator.deletedOperator.operatorID)) {
          this.dynamicSchemaMap.delete(operator.deletedOperator.operatorID);
        }
      });
  }

}

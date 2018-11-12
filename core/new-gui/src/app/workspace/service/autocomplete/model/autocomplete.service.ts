import { Injectable } from '@angular/core';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';

import { AppSettings } from '../../../../common/app-setting';
import { SourceTableNamesAPIResponse, AutocompleteSucessResult } from '../../../types/autocomplete.interface';

import { AutocompleteUtils } from '../util/autocomplete.utils';
import { OperatorPredicate } from '../../../types/workflow-common.interface';
import { OperatorMetadata, OperatorSchema } from '../../../types/operator-schema.interface';
import { OperatorMetadataService } from '../../operator-metadata/operator-metadata.service';
import { WorkflowActionService } from '../../workflow-graph/model/workflow-action.service';
import { Observable } from 'rxjs/Observable';
import '../../../../common/rxjs-operators';

import { combineLatest } from 'rxjs/observable/combineLatest';
import { Subject } from 'rxjs/Subject';
import { JSONSchema4 } from 'json-schema';
import { ExecuteWorkflowService } from '../../execute-workflow/execute-workflow.service';

export const SOURCE_TABLE_NAMES_ENDPOINT = 'resources/table-metadata';
export const AUTOMATED_SCHEMA_PROPAGATION_ENDPOINT = 'queryplan/autocomplete';

/**
 * Autocomplete service is used for the purpose of automated schema propagation. The service does
 * following tasks: -
 *  1. Originally the source operators have tableName property which has no options. This service fetches
 *    source table names from backend and adds them as an enum array to the tableName property.
 *  2. The autocomplete API at backend is invoked using invokeSchemaPropagationAPI() method.
 */
@Injectable()
export class AutocompleteService {
  // the input schema of operators in the current workflow as returned by the autocomplete API
  public operatorInputSchemaMap: JSONSchema4 = {};

  private autocompleteAPIExecutedStream = new Subject<string>();

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

    this.handleTexeraGraphPropertyChangeEvent();
    this.handleTexeraGraphLinkChangeEvent();
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
  public getAutocompleteAPIExecutedStream(): Observable<string> {
    return this.autocompleteAPIExecutedStream.asObservable();
  }

  /**
   * Modifies the schema of the operator according to autocomplete information obtained from the backend (if autcomplete info
   * contains the operator id).
   * @param operator the operator whose property is to be displayed in the property editor
   */
  public findAutocompletedSchemaForOperator(operator: Readonly<OperatorPredicate>|undefined): OperatorSchema {
    if (!operator) {
      throw new Error(`autcomplete service:findAutocompletedSchemaForOperator - operator is undefined`);
    }

    const operatorSchema = this.operatorSchemaList.find(schema => schema.operatorType === operator.operatorType);
    if (!operatorSchema) {
      throw new Error(`operator schema for operator type ${operator.operatorType} doesn't exist`);
    }

    if (!(operator.operatorID in this.operatorInputSchemaMap)) {
      return operatorSchema;
    }

    return AutocompleteUtils.addInputSchemaToOperatorSchema(operatorSchema, this.operatorInputSchemaMap[operator.operatorID]);
  }

  /**
   * Used for automated propagation of input schema in workflow.
   *
   * When users are in the process of building a workflow, Texera can propagate schema forwards so
   * that users can easily set the properties of the next operator. For eg: If there are two operators Source:Scan and KeywordSearch and
   * a link is created between them, the attributed of the table selected in Source can be propagated to the KeywordSearch operator.
   */
  public invokeAutocompleteAPI(reloadCurrentOperatorSchema: boolean): void {
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
        response => this.handleExecuteResult(response, reloadCurrentOperatorSchema),
        errorResponse => this.handleExecuteError(errorResponse)
      );
  }

  /**
   * Handles valid execution result from the backend.
   * Updates the current autocompleted schema. A value is inserted into 'autocompleteAPIExecutedStream' only
   * if the current operator schema is to be reloaded (which is the case when a link is connected/deleted/disconnected)
   * someplace in the workflow. However, if the operator's property is being changed, the autocomplete API is to be called,
   * but the operator schema doesn't have to be reloaded.
   *
   * @param response
   */
  private handleExecuteResult(response: AutocompleteSucessResult, reloadCurrentOperatorSchema: boolean): void {
    this.operatorInputSchemaMap = response.result;
    if (reloadCurrentOperatorSchema) {
      this.autocompleteAPIExecutedStream.next('Autocomplete response success');
    }
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
    .subscribe(() => this.invokeAutocompleteAPI(true));
  }


  /**
   * Handles any kind of changes in the operator properties. Whenever an operator's porperty is changed, for instance
   * a tableName is added to a source or spanList attribute name is added to Keyword Search, we invoke the auto-complete
   * API.
   */
  private handleTexeraGraphPropertyChangeEvent(): void {
    this.workflowActionService.getTexeraGraph().getOperatorPropertyChangeStream()
      .subscribe(() => this.invokeAutocompleteAPI(false));
  }

}

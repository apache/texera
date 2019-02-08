import { AppSettings } from './../../../../common/app-setting';
import { environment } from '../../../../../environments/environment';
import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { OperatorSchema } from '../../../types/operator-schema.interface';
import { DynamicSchemaService } from './../dynamic-schema.service';
import { ExecuteWorkflowService } from './../../execute-workflow/execute-workflow.service';
import { WorkflowActionService } from './../../workflow-graph/model/workflow-action.service';


// endpoint for schema propagation
export const SCHEMA_PROPAGATION_ENDPOINT = 'queryplan/autocomplete';
// By contract, property name name for texera table input attribute (column names)
export const attributeInJsonSchema = 'attribute';
export const attributeListInJsonSchema = 'attributes';


/**
 * Schema Propagation Service provides autocomplete functionaility for attribute property of operators.
 * When user creates and connects operators in workflow, the backend can propagate the schema information,
 * so that an operator knows its input attributes (column names).
 *
 * The input box for the attribute can be changed to be a drop-down menu to select the available attributes.
 *
 * By contract, property name `attribute` and `attributes` indicate the field is a column of the operator's input,
 *  and schema propagation can provide autocomplete for the column names.
 */
@Injectable({
  providedIn: 'root'
})
export class SchemaPropagationService {

  constructor(
    private httpClient: HttpClient,
    private workflowActionService: WorkflowActionService,
    private dynamicSchemaService: DynamicSchemaService
  ) {
    // do nothing if schema propagation is not enabled
    if (!environment.schemaPropagationEnabled) {
      return;
    }

    // invoke schema propagation API when: link is added/deleted,
    // or any property of any operator is changed
    Observable
      .merge(
        this.workflowActionService.getTexeraGraph().getLinkAddStream(),
        this.workflowActionService.getTexeraGraph().getLinkDeleteStream(),
        this.workflowActionService.getTexeraGraph().getOperatorPropertyChangeStream())
      .flatMap(() => this.invokeSchemaPropagationAPI())
      .filter(response => response.code === 0)
      .subscribe(response => this._applySchemaPropagationResult(response.result));
  }

  /**
   * Apply the schema propagation result to an operator.
   * The schema propagation result contains the input attributes of operators.
   *
   * If an operator is not in the result, then:
   * 1. the operator's input attributes cannot be inferred. In this case, the operator dynamic schema is unchanged.
   * 2. the operator is a source operator. In this case, we need to fill in the attributes using the selected table.
   *
   * @param schemaPropagationResult
   * @param operatorID
   */
  private _applySchemaPropagationResult(schemaPropagationResult: { [key: string]: string[] }): void {
    // for each operator, try to apply schema propagation result
    Array.from(this.dynamicSchemaService.getDynamicSchemaMap().keys()).forEach(operatorID => {
      // if operator input attributes are in the result, set them in dynamic schema
      if (schemaPropagationResult[operatorID]) {
        const currentDynamicSchema = this.dynamicSchemaService.getDynamicSchema(operatorID);
        const newSchema = SchemaPropagationService.setOperatorInputAttrs(currentDynamicSchema, schemaPropagationResult[operatorID]);

        this.dynamicSchemaService.setDynamicSchema(operatorID, newSchema);
      }

    });
  }

  /**
   * Used for automated propagation of input schema in workflow.
   *
   * When users are in the process of building a workflow, Texera can propagate schema forwards so
   * that users can easily set the properties of the next operator. For eg: If there are two operators Source:Scan and KeywordSearch and
   * a link is created between them, the attributed of the table selected in Source can be propagated to the KeywordSearch operator.
   */
  private invokeSchemaPropagationAPI(): Observable<SchemaPropagationResponse> {
    // create a Logical Plan based on the workflow graph
    const body = ExecuteWorkflowService.getLogicalPlanRequest(this.workflowActionService.getTexeraGraph());
    // make a http post request to the API endpoint with the logical plan object
    return this.httpClient.post<SchemaPropagationResponse>(
      `${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`,
      JSON.stringify(body),
      { headers: { 'Content-Type': 'application/json' } });
  }

  public static setOperatorInputAttrs(operatorSchema: OperatorSchema, inputAttributes: ReadonlyArray<string> | undefined): OperatorSchema {
    // If the inputSchema is empty, just return the original operator metadata.
    if (!inputAttributes || inputAttributes.length === 0) {
      return operatorSchema;
    }

    // TODO: Join operators have two inputs - inner and outer. Autocomplete API currently returns all attributes
    //       in a single array. So, we can't differentiate between inner and outer. Therefore, autocomplete isn't applicable
    //       to Join yet.

    let newJsonSchema = operatorSchema.jsonSchema;
    newJsonSchema = DynamicSchemaService.mutateProperty(newJsonSchema, attributeInJsonSchema,
      () => ({ type: 'string', enum: inputAttributes.slice() }));

    newJsonSchema = DynamicSchemaService.mutateProperty(newJsonSchema, attributeListInJsonSchema,
      () => ({ type: 'array', items: { type: 'string', enum: inputAttributes.slice() } }));

    return {
      ...operatorSchema,
      jsonSchema: newJsonSchema
    };
  }

}

/**
 * The backend interface of the return object of a successful execution
 * of autocomplete API
 *
 * An example data format for AutocompleteSucessResult will look like:
 * {
 *  code: 0,
 *  result: {
 *    'operatorID1' : ['attribute1','attribute2','attribute3'],
 *    'operatorID2' : ['name', 'text', 'follower_count']
 *  }
 * }
 */
export interface SchemaPropagationResponse extends Readonly<{
  code: 0,
  result: {
    [key: string]: string[]
  }
}> { }

/**
 * The backend interface of the return object of a failed execution of
 * autocomplete API
 */
export interface SchemaPropagationError extends Readonly<{
  code: -1,
  message: string
}> { }

export type SchemaPropagationResult = SchemaPropagationResponse | SchemaPropagationError;

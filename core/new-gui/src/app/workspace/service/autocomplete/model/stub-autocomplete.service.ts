import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';

import { OperatorMetadataService } from '../../operator-metadata/operator-metadata.service';
import { AutocompleteUtils } from '../util/autocomplete.utils';
import { mockSourceTableAPIResponse, mockAutocompleteAPISchemaSuggestionResponse,
  mockAutocompleteAPIEmptyResponse } from './mock-autocomplete-service.data';
import { mockWorkflowPlan_scan_sentiment_result } from '../../execute-workflow/mock-workflow-plan';
import { OperatorMetadata, OperatorSchema } from '../../../types/operator-schema.interface';
import { OperatorPredicate } from '../../../types/workflow-common.interface';

import { combineLatest } from 'rxjs/observable/combineLatest';
import { merge } from 'rxjs/observable/merge';
import { distinctUntilChanged } from 'rxjs/operators';
import { Subject } from 'rxjs/Subject';

import { JSONSchema4 } from 'json-schema';
import { WorkflowActionService } from '../../workflow-graph/model/workflow-action.service';

export const SOURCE_OPERATORS_REQUIRING_TABLENAMES: ReadonlyArray<string> = ['KeywordSource', 'RegexSource', 'WordCountIndexSource',
                                                                            'DictionarySource', 'FuzzyTokenSource', 'ScanSource'];

/**
 * This class is mostly a copy of the original Autocomplete Service class except its sourceTableNamesObservable data member
 * and invokeAutocompleteAPI() method. The code in this section could be reduced by simply extending Autocomplete Service
 * and overriding the necessary functions. However, that would lead to HttpClient being introduced as a Provider in all
 * components that use Autocomplete Service which in turn may lead to confusion among developers about why it hasn't been
 * overridden by StubHttpClient. So, we are bearing a bit of extra code to make the tests free of unnecessary dependencies.
 */
@Injectable()
export class StubAutocompleteService {

  // the input schema of operators in the current workflow as returned by the mock autocomplete API
  public operatorInputSchemaMap: JSONSchema4 = {};

  public autocompleteAPIExecutedStream = new Subject<string>();

  // the operator schema list with source table names added in source operators
  private operatorSchemaList: ReadonlyArray<OperatorSchema> = [];

  // mocks the source tables at the server
  private sourceTableNamesObservable = Observable.of(mockSourceTableAPIResponse)
                                          .map(response => AutocompleteUtils.processSourceTableAPIResponse(response))
                                          .shareReplay(1);

  constructor(private operatorMetadataService: OperatorMetadataService, private workflowActionService: WorkflowActionService) {
    this.getSourceTableAddedOperatorMetadataObservable().subscribe(
      metadata => { this.operatorSchemaList = metadata.operators; }
    );

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
            .map(([tableNames, operatorMetadata]) => AutocompleteUtils.addSourceTableNamesToMetadata(operatorMetadata, tableNames));
  }

  /**
   * Returns the observable which outputs a string everytime the autocomplete API is
   * invoked and response is received successfully.
   */
  public getAutocompleteAPIExecutedStream(): Observable<string> {
    return this.autocompleteAPIExecutedStream.asObservable();
  }

  public findAutocompletedSchemaForOperator(operator: Readonly<OperatorPredicate>|undefined): OperatorSchema|undefined {
    if (!operator) {
      throw new Error(`autcomplete service:findAutocompletedSchemaForOperator - operator is undefined`);
    }

    const operatorSchema = this.operatorSchemaList.find(schema => schema.operatorType === operator.operatorType);
    if (!(operator.operatorID in this.operatorInputSchemaMap)) {
      return operatorSchema;
    }

    if (!operatorSchema) {
      throw new Error(`operator schema for operator type ${operator.operatorType} doesn't exist`);
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
    if (this.workflowActionService.getTexeraGraph() === mockWorkflowPlan_scan_sentiment_result) {
      this.operatorInputSchemaMap = mockAutocompleteAPISchemaSuggestionResponse.result;
    } else {
      this.operatorInputSchemaMap = mockAutocompleteAPIEmptyResponse.result;
    }

    if (reloadCurrentOperatorSchema) {
      this.autocompleteAPIExecutedStream.next('Autocomplete response success');
    }
  }

  /**
   * returns the observable which produces array of source table names
   */
  private getSourceTablesNamesObservable(): Observable<ReadonlyArray<string>> {
    return this.sourceTableNamesObservable;
  }

  /**
   * Handles any kind of changes in the links of the joint graph and invokes the autocomplete API.
   * There are 3 kinds of change streams exposed by joint graph wrapper - link add, link delete
   * and link change.
   */
  private handleTexeraGraphLinkChangeEvent(): void {
    merge(this.workflowActionService.getJointGraphWrapper().getJointLinkCellAddStream(),
      this.workflowActionService.getJointGraphWrapper().getJointLinkCellDeleteStream(),
      this.workflowActionService.getJointGraphWrapper().getJointLinkCellChangeStream())
    .subscribe(() => this.invokeAutocompleteAPI(true));
  }
}

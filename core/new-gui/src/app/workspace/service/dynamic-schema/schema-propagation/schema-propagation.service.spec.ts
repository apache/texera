import { mockKeywordSearchSchema } from './../../operator-metadata/mock-operator-metadata.data';
import { AppSettings } from './../../../../common/app-setting';
import { TestBed, inject } from '@angular/core/testing';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { LoggerModule, NgxLoggerLevel } from 'ngx-logger';

import { StubOperatorMetadataService } from '../../operator-metadata/stub-operator-metadata.service';
import { OperatorMetadataService } from '../../operator-metadata/operator-metadata.service';
import { DynamicSchemaService } from './../dynamic-schema.service';
import { JointUIService } from './../../joint-ui/joint-ui.service';
import { WorkflowActionService } from './../../workflow-graph/model/workflow-action.service';
import { UndoRedoService } from './../../undo-redo/undo-redo.service';
import { SchemaPropagationService, SCHEMA_PROPAGATION_ENDPOINT } from './schema-propagation.service';
import { mockScanPredicate, mockPoint, mockSentimentPredicate, mockScanSentimentLink } from '../../workflow-graph/model/mock-workflow-data';
import {
  mockSchemaPropagationResponse, mockSchemaPropagationOperatorID, mockEmptySchemaPropagationResponse
} from './mock-schema-propagation.data';
import { mockAggregationSchema } from '../../operator-metadata/mock-operator-metadata.data';
import { OperatorPredicate } from '../../../types/workflow-common.interface';
import { environment } from '../../../../../environments/environment';

/* tslint:disable: no-non-null-assertion */
describe('SchemaPropagationService', () => {

  let httpClient: HttpClient;
  let httpTestingController: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [
        HttpClientTestingModule,
        LoggerModule.forRoot(undefined)
      ],
      providers: [
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        JointUIService,
        WorkflowActionService,
        UndoRedoService,
        DynamicSchemaService,
        SchemaPropagationService
      ]
    });

    httpClient = TestBed.get(HttpClient);
    httpTestingController = TestBed.get(HttpTestingController);
    environment.schemaPropagationEnabled = true;
  });

  it('should be created', inject([SchemaPropagationService], (service: SchemaPropagationService) => {
    expect(service).toBeTruthy();
  }));

  it('should invoke schema propagation API when a link is added/deleted', () => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const schemaPropagationService: SchemaPropagationService = TestBed.get(SchemaPropagationService);
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockSentimentPredicate, mockPoint);
    workflowActionService.addLink(mockScanSentimentLink);

    // since resetAttributeOfOperator is called every time when the schema changes, and resetAttributeOfOperator
    //  will change operator property, 2 requests will be sent to auto-complete API endpoint every time
    const req1 = httpTestingController.match(
      request => request.method === 'POST'
    );
    expect(req1[0].request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req1[0].flush(mockSchemaPropagationResponse);

    const req2 = httpTestingController.match(
      request => request.method === 'POST'
    );
    expect(req2[0].request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req2[0].flush(mockSchemaPropagationResponse);

    httpTestingController.verify();


    workflowActionService.deleteLinkWithID(mockScanSentimentLink.linkID);

    const req3 = httpTestingController.match(
      request => request.method === 'POST'
    );
    expect(req3[0].request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req3[0].flush(mockEmptySchemaPropagationResponse);

    const req4 = httpTestingController.match(
      request => request.method === 'POST'
    );
    expect(req4[0].request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req4[0].flush(mockEmptySchemaPropagationResponse);

    httpTestingController.verify();

  });

  it('should invoke schema propagation API when a operator property is changed', () => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const schemaPropagationService: SchemaPropagationService = TestBed.get(SchemaPropagationService);

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.setOperatorProperty(mockScanPredicate.operatorID, { tableName: 'test' });

    const req1 = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    expect(req1.request.method).toEqual('POST');
    req1.flush(mockSchemaPropagationResponse);
    httpTestingController.verify();
  });

  it('should handle error responses from server gracefully', () => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const schemaPropagationService: SchemaPropagationService = TestBed.get(SchemaPropagationService);

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.setOperatorProperty(mockScanPredicate.operatorID, { tableName: 'test' });

    // return error response from server
    const req1 = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    expect(req1.request.method).toEqual('POST');
    req1.error(new ErrorEvent('network error'));
    httpTestingController.verify();

    // verify that after the error response, schema propagation service still reacts to events normally
    workflowActionService.setOperatorProperty(mockScanPredicate.operatorID, { tableName: 'newTable' });

    const req2 = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    expect(req2.request.method).toEqual('POST');
    req2.flush(mockSchemaPropagationResponse);
    httpTestingController.verify();

  });

  it('should modify `attribute` of operator schema', () => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const dynamicSchemaService: DynamicSchemaService = TestBed.get(DynamicSchemaService);
    const schemaPropagationService: SchemaPropagationService = TestBed.get(SchemaPropagationService);

    const mockOperator = {
      ...mockSentimentPredicate,
      operatorID: mockSchemaPropagationOperatorID
    };

    workflowActionService.addOperator(mockOperator, mockPoint);
    // change operator property to trigger invoking schema propagation API
    workflowActionService.setOperatorProperty(mockOperator.operatorID, { testAttr: 'test' });

    // flush mock response
    const req1 = httpTestingController.match(
      request => request.method === 'POST'
    );
    expect(req1[0].request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req1[0].flush(mockSchemaPropagationResponse);

    const req2 = httpTestingController.match(
      request => request.method === 'POST'
    );
    expect(req2[0].request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req2[0].flush(mockSchemaPropagationResponse);

    httpTestingController.verify();

    const schema = dynamicSchemaService.getDynamicSchema(mockSentimentPredicate.operatorID);
    const attributeInSchema = schema.jsonSchema!.properties!['attribute'];
    expect(attributeInSchema).toEqual({
      type: 'string',
      enum: mockSchemaPropagationResponse.result[mockOperator.operatorID]
    });

  });

  it('should restore `attribute` to original schema if input attributes no longer exists', () => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const dynamicSchemaService: DynamicSchemaService = TestBed.get(DynamicSchemaService);
    const schemaPropagationService: SchemaPropagationService = TestBed.get(SchemaPropagationService);

    const mockOperator = {
      ...mockSentimentPredicate,
      operatorID: mockSchemaPropagationOperatorID
    };

    workflowActionService.addOperator(mockOperator, mockPoint);
    // change operator property to trigger invoking schema propagation API
    workflowActionService.setOperatorProperty(mockOperator.operatorID, { testAttr: 'test' });

    // flush mock response
    const req1 = httpTestingController.match(
      request => request.method === 'POST'
    );
    expect(req1[0].request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req1[0].flush(mockSchemaPropagationResponse);

    const req2 = httpTestingController.match(
      request => request.method === 'POST'
    );
    expect(req2[0].request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req2[0].flush(mockSchemaPropagationResponse);

    httpTestingController.verify();

    const schema = dynamicSchemaService.getDynamicSchema(mockSentimentPredicate.operatorID);
    const attributeInSchema = schema.jsonSchema!.properties!['attribute'];
    expect(attributeInSchema).toEqual({
      type: 'string',
      enum: mockSchemaPropagationResponse.result[mockOperator.operatorID]
    });

    // change operator property to trigger invoking schema propagation API
    workflowActionService.setOperatorProperty(mockOperator.operatorID, { testAttr: 'test' });

    // flush mock response, however, this time response is empty, which means input attrs no longer exists

    const req3 = httpTestingController.match(
      request => request.method === 'POST'
    );
    expect(req3[0].request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req3[0].flush(mockEmptySchemaPropagationResponse);

    const req4 = httpTestingController.match(
      request => request.method === 'POST'
    );
    expect(req4[0].request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req4[0].flush(mockEmptySchemaPropagationResponse);

    httpTestingController.verify();

    // verify that schema is restored to original value
    const restoredSchema = dynamicSchemaService.getDynamicSchema(mockSentimentPredicate.operatorID);
    const restoredAttributeInSchema = restoredSchema.jsonSchema!.properties!['attribute'];
    expect(restoredAttributeInSchema).toEqual({
      type: 'string'
    });
  });

  it('should modify `attributes` of operator schema', () => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const dynamicSchemaService: DynamicSchemaService = TestBed.get(DynamicSchemaService);
    const schemaPropagationService: SchemaPropagationService = TestBed.get(SchemaPropagationService);

    const mockKeywordSearchOperator: OperatorPredicate = {
      operatorID: mockSchemaPropagationOperatorID,
      operatorType: mockKeywordSearchSchema.operatorType,
      operatorProperties: {},
      inputPorts: [],
      outputPorts: [],
      showAdvanced: true
    };

    workflowActionService.addOperator(mockKeywordSearchOperator, mockPoint);
    // change operator property to trigger invoking schema propagation API
    workflowActionService.setOperatorProperty(mockKeywordSearchOperator.operatorID, { testAttr: 'test' });

    // flush mock response
    const req1 = httpTestingController.match(
      request => request.method === 'POST'
    );
    expect(req1[0].request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req1[0].flush(mockSchemaPropagationResponse);

    const req2 = httpTestingController.match(
      request => request.method === 'POST'
    );
    expect(req2[0].request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req2[0].flush(mockSchemaPropagationResponse);

    httpTestingController.verify();

    const schema = dynamicSchemaService.getDynamicSchema(mockSentimentPredicate.operatorID);
    const attributeInSchema = schema.jsonSchema!.properties!['attributes'];
    expect(attributeInSchema).toEqual({
      type: 'array',
      items: {
        type: 'string',
        enum: mockSchemaPropagationResponse.result[mockKeywordSearchOperator.operatorID]
      }
    });
  });

  it('should modify nested deep `attribute` of operator schema', () => {

    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const dynamicSchemaService: DynamicSchemaService = TestBed.get(DynamicSchemaService);
    const schemaPropagationService: SchemaPropagationService = TestBed.get(SchemaPropagationService);

    // to match the operator ID of mockSchemaPropagationResponse
    const mockAggregationPredicate: OperatorPredicate = {
      operatorID: mockSchemaPropagationOperatorID,
      operatorType: mockAggregationSchema.operatorType,
      operatorProperties: {},
      inputPorts: [],
      outputPorts: [],
      showAdvanced: true
    };

    workflowActionService.addOperator(mockAggregationPredicate, mockPoint);
    // change operator property to trigger invoking schema propagation API
    workflowActionService.setOperatorProperty(mockAggregationPredicate.operatorID, { testAttr: 'test' });

    // flush mock response
    const req1 = httpTestingController.match(
      request => request.method === 'POST'
    );
    expect(req1[0].request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req1[0].flush(mockSchemaPropagationResponse);

    const req2 = httpTestingController.match(
      request => request.method === 'POST'
    );
    expect(req2[0].request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req2[0].flush(mockSchemaPropagationResponse);

    httpTestingController.verify();

    const schema = dynamicSchemaService.getDynamicSchema(mockSentimentPredicate.operatorID);

    expect(schema.jsonSchema!.properties).toEqual({
      listOfAggregations: {
        type: 'array',
        items: {
          type: 'object',
          properties: {
            attribute: {
              type: 'string',
              enum: mockSchemaPropagationResponse.result[mockAggregationPredicate.operatorID]
            },
            aggregator: {
              type: 'string',
              enum: ['min', 'max', 'average', 'sum', 'count']
            },
            resultAttribute: { type: 'string' }
          }
        }
      }
    });

  });

});

import { UserService } from './../../../../common/service/user/user.service';
import { UserFileService } from './../../../../common/service/user/user-file/user-file.service';
import { AppSettings } from './../../../../common/app-setting';
import { TestBed, inject } from '@angular/core/testing';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';

import { StubOperatorMetadataService } from '../../operator-metadata/stub-operator-metadata.service';
import { OperatorMetadataService, OPERATOR_METADATA_ENDPOINT } from '../../operator-metadata/operator-metadata.service';
import { mockOperatorMetaData, mockKeywordSourceSchema } from '../../operator-metadata/mock-operator-metadata.data';

import { DynamicSchemaService } from './../dynamic-schema.service';
import { JointUIService } from './../../joint-ui/joint-ui.service';
import { WorkflowActionService } from './../../workflow-graph/model/workflow-action.service';
import { UndoRedoService } from './../../undo-redo/undo-redo.service';

import { SourceTablesService, SOURCE_TABLE_NAMES_ENDPOINT } from './source-tables.service';
import { mockSourceTableAPIResponse, mockTableTwitter, mockTablePromed } from './mock-source-tables.data';
import { mockScanPredicate, mockPoint } from '../../workflow-graph/model/mock-workflow-data';
import { OperatorPredicate } from '../../../types/workflow-common.interface';
import { environment } from '../../../../../environments/environment';
import { Subject } from 'rxjs';
import { UserFile } from 'src/app/common/type/user-file';
import { StubUserService } from 'src/app/common/service/user/stub-user.service';

/* tslint:disable: no-non-null-assertion */
describe('SourceTablesService', () => {

  let httpClient: HttpClient;
  let httpTestingController: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: UserService, useClass: StubUserService },
        JointUIService,
        WorkflowActionService,
        UndoRedoService,
        DynamicSchemaService,
        SourceTablesService,
        UserFileService,
      ]
    });

    httpClient = TestBed.get(HttpClient);
    httpTestingController = TestBed.get(HttpTestingController);
    environment.sourceTableEnabled = true;
  });

  it('should fetch source tables from backend API', () => {
    const sourceTablesService: SourceTablesService = TestBed.get(SourceTablesService);

    const req = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SOURCE_TABLE_NAMES_ENDPOINT}`);
    expect(req.request.method).toEqual('GET');
    req.flush(mockSourceTableAPIResponse);

    httpTestingController.verify();

    const tableSchemaMap = sourceTablesService.getTableSchemaMap();

    expect(tableSchemaMap !== undefined && tableSchemaMap.size > 0);

  });

  it('should modify tableName of the scan operator schema', () => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const dynamicSchemaService: DynamicSchemaService = TestBed.get(DynamicSchemaService);
    const sourceTablesService: SourceTablesService = TestBed.get(SourceTablesService);

    const req = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SOURCE_TABLE_NAMES_ENDPOINT}`);
    req.flush(mockSourceTableAPIResponse);
    httpTestingController.verify();

    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    const dynamicSchema = dynamicSchemaService.getDynamicSchema(mockScanPredicate.operatorID);

    expect(dynamicSchema.jsonSchema.properties!['tableName']).toEqual({
      type: 'string',
      enum: [
        mockTablePromed.tableName, mockTableTwitter.tableName
      ],
      uniqueItems: true
    });

  });

  it('should modify fileName of the file source operator schema', () => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const dynamicSchemaService: DynamicSchemaService = TestBed.get(DynamicSchemaService);

    const userFileService: UserFileService = TestBed.get(UserFileService);
    const userFilesChanged = new Subject<ReadonlyArray<UserFile> | undefined>();
    spyOn(userFileService, 'getUserFilesChangedEvent').and.returnValue(userFilesChanged.asObservable());

    const sourceTablesService: SourceTablesService = TestBed.get(SourceTablesService);

    const mockFileSourcePredicate: OperatorPredicate = {
      operatorID: '1',
      operatorType: 'FileSource',
      operatorProperties: {
      },
      inputPorts: [],
      outputPorts: ['output-0'],
      showAdvanced: true
    };

    workflowActionService.addOperator(mockFileSourcePredicate, mockPoint);

    const dynamicSchema = dynamicSchemaService.getDynamicSchema(mockScanPredicate.operatorID);
    expect(dynamicSchema.jsonSchema.properties!['fileName']).toEqual({
      type: 'string'
    });

    userFilesChanged.next([
      { id: 1, name: 'file1', path: 'path', description: '', size: 100 },
      { id: 2, name: 'file2', path: 'pat2', description: '', size: 200 }
    ]);
    const dynamicSchemaAfter = dynamicSchemaService.getDynamicSchema(mockScanPredicate.operatorID);
    expect(dynamicSchemaAfter.jsonSchema.properties!['fileName']).toEqual({
      type: 'string',
      enum: [
        'file1', 'file2'
      ],
      uniqueItems: true
    });

  });

  it('should modify the attribute of the scan operator after table is selected', () => {
    // construct the source table service and flush the source table responses
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const dynamicSchemaService: DynamicSchemaService = TestBed.get(DynamicSchemaService);
    const sourceTablesService: SourceTablesService = TestBed.get(SourceTablesService);

    const req = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SOURCE_TABLE_NAMES_ENDPOINT}`);
    req.flush(mockSourceTableAPIResponse);
    httpTestingController.verify();

    const mockKeywordSourcePredicate: OperatorPredicate = {
      operatorID: '1',
      operatorType: mockKeywordSourceSchema.operatorType,
      operatorProperties: {},
      inputPorts: [],
      outputPorts: ['output-0'],
      showAdvanced: true
    };

    // add keyword source operator and select a table name, this should trigger the change of "attributes" property
    workflowActionService.addOperator(mockKeywordSourcePredicate, mockPoint);
    workflowActionService.setOperatorProperty(mockKeywordSourcePredicate.operatorID, { tableName: mockTableTwitter.tableName });

    // check "attributes" is changed with autocomplete attribute names of the selected table
    const dynamicSchema = dynamicSchemaService.getDynamicSchema(mockKeywordSourcePredicate.operatorID);
    expect(dynamicSchema.jsonSchema.properties!['attributes']).toEqual({
      type: 'array',
      items: {
        type: 'string',
        enum: mockTableTwitter.schema.attributes.map(attr => attr.attributeName),
        uniqueItems: true
      }
    });

  });


});

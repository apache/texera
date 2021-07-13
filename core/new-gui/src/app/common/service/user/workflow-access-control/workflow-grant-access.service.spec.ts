import { TestBed } from '@angular/core/testing';
import { Workflow, WorkflowContent } from '../../../type/workflow';
import { jsonCast } from '../../../util/storage';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import {
  WORKFLOW_ACCESS_GRANT_URL,
  WORKFLOW_ACCESS_LIST_URL,
  WORKFLOW_ACCESS_REVOKE_URL,
  WorkflowGrantAccessService
} from './workflow-grant-access.service';
import { AppSettings } from '../../../app-setting';

describe('WorkflowGrantAccessService', () => {

  const TestWorkflow: Workflow = {
    wid: 28,
    name: 'project 1',
    content: jsonCast<WorkflowContent>(' {"operators":[],"operatorPositions":{},"links":[],"groups":[],"breakpoints":{}}'),
    creationTime: 1,
    lastModifiedTime: 2
  };

  const username = 'Jim';
  const accessType = 'read';

  let service: WorkflowGrantAccessService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        WorkflowGrantAccessService
      ],
      imports: [
        HttpClientTestingModule
      ]
    });
    service = TestBed.get(WorkflowGrantAccessService);
    httpMock = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
  it('GrantWorkflowAccess works as expected', () => {
    service.grantAccess(TestWorkflow, username, accessType).first().subscribe();
    console.log(httpMock);
    const req = httpMock.expectOne(
      `${AppSettings.getApiEndpoint()}/${WORKFLOW_ACCESS_GRANT_URL}/${TestWorkflow.wid}/${username}/${accessType}`);
    console.log(req.request);
    expect(req.request.method).toEqual('POST');
    req.flush({code: 0, message: ''});
  });

  it('GetSharedAccess works as expected', () => {
    service.retrieveGrantedList(TestWorkflow).first().subscribe();
    const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${WORKFLOW_ACCESS_LIST_URL}/${TestWorkflow.wid}`);
    expect(req.request.method).toEqual('GET');
    req.flush({code: 0, message: ''});
  });

  it('RemoveAccess works as expected', () => {
    service.revokeAccess(TestWorkflow, username).first().subscribe();
    const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${WORKFLOW_ACCESS_REVOKE_URL}/${TestWorkflow.wid}/${username}`);
    expect(req.request.method).toEqual('POST');
    req.flush({code: 0, message: ''});
  });

});

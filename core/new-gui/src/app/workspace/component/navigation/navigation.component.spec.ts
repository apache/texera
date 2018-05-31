import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NavigationComponent } from './navigation.component';
import { ExecuteWorkflowService } from './../../service/execute-workflow/execute-workflow.service';
import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';

import { CustomNgMaterialModule } from '../../../common/custom-ng-material.module';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { StubOperatorMetadataService } from '../../service/operator-metadata/stub-operator-metadata.service';
import { OperatorMetadataService } from '../../service/operator-metadata/operator-metadata.service';
import { JointUIService } from '../../service/joint-ui/joint-ui.service';

import { Observable } from 'rxjs/Observable';
import { MOCK_RESULT_DATA } from '../../service/execute-workflow/mock-result-data';
import { HttpClient } from 'selenium-webdriver/http';
import { StubExecuteWorkflowService } from '../../service/execute-workflow/stub-execute-workflow.service';


describe('NavigationComponent', () => {
  let component: NavigationComponent;
  let fixture: ComponentFixture<NavigationComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NavigationComponent ],
      providers: [
        ExecuteWorkflowService,
        WorkflowActionService,
        JointUIService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: ExecuteWorkflowService, useClass: StubExecuteWorkflowService }
      ],
      imports: [ CustomNgMaterialModule, BrowserAnimationsModule ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NavigationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

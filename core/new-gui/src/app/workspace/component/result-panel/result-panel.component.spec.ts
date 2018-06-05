import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ResultPanelComponent } from './result-panel.component';
import { ExecuteWorkflowService } from "./../../service/execute-workflow/execute-workflow.service";
import { StubExecuteWorkflowService } from "./../../service/execute-workflow/stub-execute-workflow.service";
import { CustomNgMaterialModule } from "./../../../common/custom-ng-material.module";

import { WorkflowActionService } from "./../../service/workflow-graph/model/workflow-action.service";
import { JointUIService } from "./../../service/joint-ui/joint-ui.service";
import { OperatorMetadataService } from "./../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "./../../service/operator-metadata/stub-operator-metadata.service";
import { NgbModule } from '@ng-bootstrap/ng-bootstrap';


describe('ResultPanelComponent', () => {
  let component: ResultPanelComponent;
  let fixture: ComponentFixture<ResultPanelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ResultPanelComponent ],
      imports: [
        NgbModule.forRoot(),
        CustomNgMaterialModule
      ],
      providers: [
        WorkflowActionService,
        JointUIService,
        { provide: ExecuteWorkflowService, useClass: StubExecuteWorkflowService },
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService }
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ResultPanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

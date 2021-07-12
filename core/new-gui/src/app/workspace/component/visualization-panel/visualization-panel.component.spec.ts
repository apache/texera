import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzModalModule, NzModalService } from 'ng-zorro-antd/modal';
import { ExecuteWorkflowService } from '../../service/execute-workflow/execute-workflow.service';
import { JointUIService } from '../../service/joint-ui/joint-ui.service';
import { OperatorMetadataService } from '../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../../service/operator-metadata/stub-operator-metadata.service';
import { UndoRedoService } from '../../service/undo-redo/undo-redo.service';
import { WorkflowActionService } from '../../service/workflow-graph/model/workflow-action.service';
import { WorkflowUtilService } from '../../service/workflow-graph/util/workflow-util.service';
import { VisualizationPanelComponent } from './visualization-panel.component';
import { WorkflowResultService, OperatorResultService } from '../../service/workflow-result/workflow-result.service';
import { WebDataUpdate } from '../../types/execute-workflow.interface';
import { ChartType } from '../../types/visualization.interface';

describe('VisualizationPanelComponent', () => {
  let component: VisualizationPanelComponent;
  let fixture: ComponentFixture<VisualizationPanelComponent>;
  let workflowResultService: WorkflowResultService;
  const operatorID = 'operator1';
  const testData: WebDataUpdate = {
    mode: { type: 'SetSnapshotMode' },
    chartType: ChartType.BAR,
    table: []
  };

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        NzModalModule,
        NzButtonModule,
        HttpClientTestingModule
      ],
      declarations: [VisualizationPanelComponent],
      providers: [
        JointUIService,
        WorkflowUtilService,
        UndoRedoService,
        WorkflowActionService,
        {provide: OperatorMetadataService, useClass: StubOperatorMetadataService},
        WorkflowResultService,
        ExecuteWorkflowService
      ]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(VisualizationPanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();

    workflowResultService = TestBed.get(WorkflowResultService);
    const operatorResultService: OperatorResultService = new OperatorResultService(operatorID);
    operatorResultService.handleResultUpdate(testData);

    spyOn(workflowResultService, 'getResultService').and.returnValue(operatorResultService);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have button', () => {
    component.operatorID = operatorID;

    // fixture.detectChanges() doesn't call ngOnChanges in tests because of Angular bug
    component.ngOnChanges();
    fixture.detectChanges();

    console.log(component.displayVisualizationPanel);

    const element: HTMLElement = fixture.nativeElement;
    const button = element.querySelector('button');
    expect(button).toBeTruthy();
  });

  it('should open dialog', () => {
    // make button appear
    component.operatorID = operatorID;

    // fixture.detectChanges() doesn't call ngOnChanges in tests because of Angular bug
    component.ngOnChanges();
    fixture.detectChanges();

    const element: HTMLElement = fixture.nativeElement;
    const button = element.querySelector('button');

    const modalService = TestBed.get(NzModalService);
    const createSpy = spyOn(modalService, 'create');

    // click button
    button?.click();
    expect(createSpy).toHaveBeenCalled();
  });
});

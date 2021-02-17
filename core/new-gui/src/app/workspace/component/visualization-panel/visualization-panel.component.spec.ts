import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { VisualizationPanelComponent } from './visualization-panel.component';
import { NzModalModule, NzModalService } from 'ng-zorro-antd/modal';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { WorkflowStatusService } from '../../service/workflow-status/workflow-status.service';
import { ResultObject } from '../../types/execute-workflow.interface';

describe('VisualizationPanelComponent', () => {
  let component: VisualizationPanelComponent;
  let fixture: ComponentFixture<VisualizationPanelComponent>;
  let workflowStatusService: WorkflowStatusService;

  const testData: Record<string, ResultObject> = {'operator1': {operatorID: 'operator1', chartType: 'bar', table: [], totalRowCount: 0, }};

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        NzModalModule,
        NzButtonModule
      ],
      declarations: [ VisualizationPanelComponent ],
      providers: [ WorkflowStatusService ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(VisualizationPanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();

    workflowStatusService = TestBed.get(WorkflowStatusService);
    spyOn(workflowStatusService, 'getCurrentResult').and.returnValue(testData);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have button', () => {
    component.operatorID = 'operator1';

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
    component.operatorID = 'operator1';

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

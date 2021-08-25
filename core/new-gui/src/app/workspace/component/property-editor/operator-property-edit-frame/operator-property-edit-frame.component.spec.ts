import { async, ComponentFixture, discardPeriodicTasks, fakeAsync, TestBed, tick } from '@angular/core/testing';

import { OperatorPropertyEditFrameComponent } from './operator-property-edit-frame.component';
import { WorkflowActionService } from '../../../service/workflow-graph/model/workflow-action.service';
import { DynamicSchemaService } from '../../../service/dynamic-schema/dynamic-schema.service';
import { SchemaPropagationService } from '../../../service/dynamic-schema/schema-propagation/schema-propagation.service';
import { environment } from '../../../../../environments/environment';
import { ArrayTypeComponent } from '../../../../common/formly/array.type';
import { ObjectTypeComponent } from '../../../../common/formly/object.type';
import { MultiSchemaTypeComponent } from '../../../../common/formly/multischema.type';
import { NullTypeComponent } from '../../../../common/formly/null.type';
import { JointUIService } from '../../../service/joint-ui/joint-ui.service';
import { WorkflowUtilService } from '../../../service/workflow-graph/util/workflow-util.service';
import { UndoRedoService } from '../../../service/undo-redo/undo-redo.service';
import { OperatorMetadataService } from '../../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../../../service/operator-metadata/stub-operator-metadata.service';
import { ExecuteWorkflowService, FORM_DEBOUNCE_TIME_MS } from '../../../service/execute-workflow/execute-workflow.service';
import { FormlyJsonschema } from '@ngx-formly/core/json-schema';
import { LoggerConfig, NGXLogger, NGXLoggerHttpService, NGXMapperService } from 'ngx-logger';
import { CommonModule, DatePipe } from '@angular/common';
import { BrowserModule, By } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { FormlyModule } from '@ngx-formly/core';
import { TEXERA_FORMLY_CONFIG } from '../../../../common/formly/formly-config';
import { FormlyMaterialModule } from '@ngx-formly/material';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { mockPoint, mockResultPredicate, mockScanPredicate } from '../../../service/workflow-graph/model/mock-workflow-data';
import { mockScanSourceSchema } from '../../../service/operator-metadata/mock-operator-metadata.data';
import { JSONSchema7 } from 'json-schema';
import { configure } from 'rxjs-marbles';

const { marbles } = configure({ run: false });
describe('OperatorPropertyEditFrameComponent', () => {
  let component: OperatorPropertyEditFrameComponent;
  let fixture: ComponentFixture<OperatorPropertyEditFrameComponent>;
  let workflowActionService: WorkflowActionService;
  let dynamicSchemaService: DynamicSchemaService;
  let schemaPropagationService: SchemaPropagationService;
  environment.schemaPropagationEnabled = true;


  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [
        OperatorPropertyEditFrameComponent,
        ArrayTypeComponent,
        ObjectTypeComponent,
        MultiSchemaTypeComponent,
        NullTypeComponent
      ],
      providers: [
        JointUIService,
        WorkflowActionService,
        WorkflowUtilService,
        UndoRedoService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        DynamicSchemaService,
        ExecuteWorkflowService,
        FormlyJsonschema,
        SchemaPropagationService,
        NGXLogger,
        NGXLoggerHttpService,
        LoggerConfig,
        DatePipe,
        NGXMapperService
        // { provide: HttpClient, useClass: {} }
      ],
      imports: [
        CommonModule,
        BrowserModule,
        BrowserAnimationsModule,
        NgbModule,
        FormsModule,
        FormlyModule.forRoot(TEXERA_FORMLY_CONFIG),
        // formly ng zorro module has a bug that doesn't display field description,
        // FormlyNgZorroAntdModule,
        // use formly material module instead
        FormlyMaterialModule,
        ReactiveFormsModule,
        HttpClientTestingModule
      ]
    })
      .compileComponents();
  }));

  beforeEach(() => {

    workflowActionService = TestBed.inject(WorkflowActionService);
    dynamicSchemaService = TestBed.inject(DynamicSchemaService);
    schemaPropagationService = TestBed.inject(SchemaPropagationService);
    // fixture.detectChanges();

  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });


  /**
   * test if the property editor correctly receives the operator highlight stream,
   *  get the operator data (id, property, and metadata), and then display the form.
   */
  it('should change the content of property editor from an empty panel correctly', () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();
    console.log(workflowActionService.getTexeraGraph());

    // check if the changePropertyEditor called after the operator
    //  is highlighted has correctly updated the variables
    const predicate = mockScanPredicate;

    // add and highlight an operator
    workflowActionService.addOperator(predicate, mockPoint);
    jointGraphWrapper.highlightOperators(predicate.operatorID);

    fixture = TestBed.createComponent(OperatorPropertyEditFrameComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    // check variables are set correctly
    expect(component.currentOperatorId).toEqual(predicate.operatorID);
    expect(component.formData).toEqual(predicate.operatorProperties);

    // check HTML form are displayed
    const formTitleElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-title'));
    const jsonSchemaFormElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));
    // check the panel title
    expect((formTitleElement.nativeElement as HTMLElement).innerText).toEqual(
      mockScanSourceSchema.additionalMetadata.userFriendlyName);

    // check if the form has the all the json schema property names
    Object.entries(mockScanSourceSchema.jsonSchema.properties as any).forEach((entry) => {
      const propertyTitle = (entry[1] as JSONSchema7).title;
      if (propertyTitle) {
        expect((jsonSchemaFormElement.nativeElement as HTMLElement).innerHTML).toContain(propertyTitle);
      }
      const propertyDescription = (entry[1] as JSONSchema7).description;
      if (propertyDescription) {
        expect((jsonSchemaFormElement.nativeElement as HTMLElement).innerHTML).toContain(propertyDescription);
      }
    });

  });


  /**
   * test if the property editor correctly receives the operator highlight stream
   *  and displays the operator's data when it's the only highlighted operator.
   */
  it('should switch the content of property editor to the highlighted operator correctly when only one operator is highlighted', () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add and highlight two operators, then unhighlight one of them
    workflowActionService.addOperatorsAndLinks([{ op: mockScanPredicate, pos: mockPoint },
      { op: mockResultPredicate, pos: mockPoint }], []);
    jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID, mockResultPredicate.operatorID);
    jointGraphWrapper.unhighlightOperators(mockResultPredicate.operatorID);

    // assert that only one operator is highlighted on the graph
    const predicate = mockScanPredicate;
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([predicate.operatorID]);

    fixture = TestBed.createComponent(OperatorPropertyEditFrameComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();

    // check if the changePropertyEditor called after the operator
    //  is unhighlighted has correctly updated the variables

    // check variables are set correctly
    expect(component.currentOperatorId).toEqual(predicate.operatorID);
    expect(component.formData).toEqual(predicate.operatorProperties);


    // check HTML form are displayed
    const formTitleElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-title'));
    const jsonSchemaFormElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));

    // check the panel title
    expect((formTitleElement.nativeElement as HTMLElement).innerText).toEqual(
      mockScanSourceSchema.additionalMetadata.userFriendlyName);

    // check if the form has the all the json schema property names
    Object.keys(mockScanSourceSchema.jsonSchema.properties as any).forEach((propertyName) => {
      expect((jsonSchemaFormElement.nativeElement as HTMLElement).innerHTML).toContain(propertyName);
    });
  });


  it('should change Texera graph property when the form is edited by the user', fakeAsync(() => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add an operator and highlight the operator so that the
    //  variables in property editor component is set correctly
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);

    fixture = TestBed.createComponent(OperatorPropertyEditFrameComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();

    // stimulate a form change by the user
    const formChangeValue = { tableName: 'twitter_sample' };
    component.onFormChanges(formChangeValue);

    // maintain a counter of how many times the event is emitted
    let emitEventCounter = 0;
    component.operatorPropertyChangeStream.subscribe(() => emitEventCounter++);

    // fakeAsync enables tick, which waits for the set property debounce time to finish
    tick(FORM_DEBOUNCE_TIME_MS + 10);

    // then get the operator, because operator is immutable, the operator before the tick
    //   is a different object reference from the operator after the tick
    const operator = workflowActionService.getTexeraGraph().getOperator(mockScanPredicate.operatorID);
    if (!operator) {
      throw new Error(`operator ${mockScanPredicate.operatorID} is undefined`);
    }

    discardPeriodicTasks();

    expect(operator.operatorProperties).toEqual(formChangeValue);
    expect(emitEventCounter).toEqual(1);

  }));

  xit('should debounce the user form input to avoid emitting event too frequently', marbles(m => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add an operator and highlight the operator so that the
    //  variables in property editor component is set correctly
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);

    // prepare the form user input event stream
    // simulate user types in `table` character by character
    const formUserInputMarbleString = '-a-b-c-d-e';
    const formUserInputMarbleValue = {
      a: { tableName: 't' },
      b: { tableName: 'ta' },
      c: { tableName: 'tab' },
      d: { tableName: 'tabl' },
      e: { tableName: 'table' }
    };
    const formUserInputEventStream = m.hot(formUserInputMarbleString, formUserInputMarbleValue);

    // prepare the expected output stream after debounce time
    const formChangeEventMarbleStrig =
      // wait for the time of last marble string starting to emit
      '-'.repeat(formUserInputMarbleString.length - 1) +
      // then wait for debounce time (each tick represents 10 ms)
      '-'.repeat(FORM_DEBOUNCE_TIME_MS / 10) +
      'e-';
    const formChangeEventMarbleValue = {
      e: { tableName: 'table' } as object
    };
    const expectedFormChangeEventStream = m.hot(formChangeEventMarbleStrig, formChangeEventMarbleValue);

    m.bind();

    // TODO: FIX THIS
    // const actualFormChangeEventStream = component.createOutputFormChangeEventStream(formUserInputEventStream);
    // formUserInputEventStream.subscribe();

    // m.expect(actualFormChangeEventStream).toBeObservable(expectedFormChangeEventStream);

  }));

  it('should not emit operator property change event if the new property is the same as the old property', fakeAsync(() => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add an operator and highlight the operator so that the
    //  variables in property editor component is set correctly
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    const mockOperatorProperty = { tableName: 'table' };
    // set operator property first before displaying the operator property in property panel
    workflowActionService.setOperatorProperty(mockScanPredicate.operatorID, mockOperatorProperty);
    jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);

    fixture = TestBed.createComponent(OperatorPropertyEditFrameComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();

    // stimulate a form change with the same property
    component.onFormChanges(mockOperatorProperty);

    // maintain a counter of how many times the event is emitted
    let emitEventCounter = 0;
    component.operatorPropertyChangeStream.subscribe(() => emitEventCounter++);

    // fakeAsync enables tick, which waits for the set property debounce time to finish
    tick(FORM_DEBOUNCE_TIME_MS + 10);

    discardPeriodicTasks();

    // assert that the form change event doesn't emit any time
    // because the form change value is the same
    expect(emitEventCounter).toEqual(0);

  }));

});

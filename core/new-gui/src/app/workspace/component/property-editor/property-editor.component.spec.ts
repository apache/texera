import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { By } from '@angular/platform-browser';
import { MaterialDesignFrameworkModule } from 'angular6-json-schema-form';
import { async, ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';

import { PropertyEditorComponent } from './property-editor.component';

import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { UndoRedoService } from './../../service/undo-redo/undo-redo.service';
import { OperatorMetadataService } from './../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from './../../service/operator-metadata/stub-operator-metadata.service';
import { JointUIService } from './../../service/joint-ui/joint-ui.service';

import { mockScanSourceSchema, mockViewResultsSchema,
         mockBreakpointSchema } from './../../service/operator-metadata/mock-operator-metadata.data';

import { configure } from 'rxjs-marbles';
const { marbles } = configure({ run: false });

import { mockResultPredicate, mockScanPredicate, mockPoint,
         mockScanResultLink, mockScanSentimentLink, mockSentimentResultLink,
         mockSentimentPredicate} from '../../service/workflow-graph/model/mock-workflow-data';
import { CustomNgMaterialModule } from '../../../common/custom-ng-material.module';
import { DynamicSchemaService } from '../../service/dynamic-schema/dynamic-schema.service';
import { environment } from './../../../../environments/environment';

import { NgbModule } from '@ng-bootstrap/ng-bootstrap';

/* tslint:disable:no-non-null-assertion */

describe('PropertyEditorComponent', () => {
  let component: PropertyEditorComponent;
  let fixture: ComponentFixture<PropertyEditorComponent>;
  let workflowActionService: WorkflowActionService;
  let dynamicSchemaService: DynamicSchemaService;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [PropertyEditorComponent],
      providers: [
        JointUIService,
        WorkflowActionService,
        UndoRedoService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        DynamicSchemaService,
        // { provide: HttpClient, useClass: StubHttpClient }
      ],
      imports: [
        CustomNgMaterialModule,
        BrowserAnimationsModule,
        MaterialDesignFrameworkModule,
        NgbModule
      ]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PropertyEditorComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.get(WorkflowActionService);
    dynamicSchemaService = TestBed.get(DynamicSchemaService);

    fixture.detectChanges();

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

    // check if the changePropertyEditor called after the operator
    //  is highlighted has correctly updated the variables
    const predicate = mockScanPredicate;

    // add and highlight an operator
    workflowActionService.addOperator(predicate, mockPoint);
    jointGraphWrapper.highlightOperator(predicate.operatorID);

    fixture.detectChanges();

    // check variables are set correctly
    expect(component.currentOperatorID).toEqual(predicate.operatorID);
    expect(component.currentOperatorSchema).toEqual(mockScanSourceSchema);
    expect(component.currentOperatorInitialData).toEqual(predicate.operatorProperties);
    expect(component.displayForm).toBeTruthy();

    // check HTML form are displayed
    const formTitleElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-title'));
    const jsonSchemaFormElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));

    // check the panel title
    expect((formTitleElement.nativeElement as HTMLElement).innerText).toEqual(
      mockScanSourceSchema.additionalMetadata.userFriendlyName);

    // check if the form has the all the json schema property names
    Object.keys(mockScanSourceSchema.jsonSchema.properties!).forEach((propertyName) => {
      expect((jsonSchemaFormElement.nativeElement as HTMLElement).innerHTML).toContain(propertyName);
    });

  });


  it('should switch the content of property editor to another operator from the former operator correctly', () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add two operators
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockResultPredicate, mockPoint);

    // highlight the first operator
    jointGraphWrapper.highlightOperator(mockScanPredicate.operatorID);
    fixture.detectChanges();

    // check the variables
    expect(component.currentOperatorID).toEqual(mockScanPredicate.operatorID);
    expect(component.currentOperatorSchema).toEqual(mockScanSourceSchema);
    expect(component.currentOperatorInitialData).toEqual(mockScanPredicate.operatorProperties);
    expect(component.displayForm).toBeTruthy();

    // highlight the second operator
    jointGraphWrapper.highlightOperator(mockResultPredicate.operatorID);
    fixture.detectChanges();

    expect(component.currentOperatorID).toEqual(mockResultPredicate.operatorID);
    expect(component.currentOperatorSchema).toEqual(mockViewResultsSchema);
    expect(component.currentOperatorInitialData).toEqual(mockResultPredicate.operatorProperties);
    expect(component.displayForm).toBeTruthy();


    // check HTML form are displayed
    const formTitleElementAfterChange = fixture.debugElement.query(By.css('.texera-workspace-property-editor-title'));
    const jsonSchemaFormElementAfterChange = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));

    // check the panel title
    expect((formTitleElementAfterChange.nativeElement as HTMLElement).innerText).toEqual(
      mockViewResultsSchema.additionalMetadata.userFriendlyName);

    // check if the form has the all the json schema property names
    Object.keys(mockViewResultsSchema.jsonSchema.properties!).forEach((propertyName) => {
      expect((jsonSchemaFormElementAfterChange.nativeElement as HTMLElement).innerHTML).toContain(propertyName);
    });


  });

  /**
   * test if the property editor correctly receives the operator unhighlight stream
   *  and displays the operator's data when it's the only highlighted operator.
   */
  it('should switch the content of property editor to the highlighted operator correctly when only one operator is highlighted', () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add and highlight two operators, then unhighlight one of them
    workflowActionService.addOperatorsAndLinks([{op: mockScanPredicate, pos: mockPoint},
      {op: mockResultPredicate, pos: mockPoint}], []);
    jointGraphWrapper.highlightOperators([mockScanPredicate.operatorID, mockResultPredicate.operatorID]);
    jointGraphWrapper.unhighlightOperator(mockResultPredicate.operatorID);

    // assert that only one operator is highlighted on the graph
    const predicate = mockScanPredicate;
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([predicate.operatorID]);

    fixture.detectChanges();

    // check if the changePropertyEditor called after the operator
    //  is unhighlighted has correctly updated the variables

    // check variables are set correctly
    expect(component.currentOperatorID).toEqual(predicate.operatorID);
    expect(component.currentOperatorSchema).toEqual(mockScanSourceSchema);
    expect(component.currentOperatorInitialData).toEqual(predicate.operatorProperties);
    expect(component.displayForm).toBeTruthy();

    // check HTML form are displayed
    const formTitleElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-title'));
    const jsonSchemaFormElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));

    // check the panel title
    expect((formTitleElement.nativeElement as HTMLElement).innerText).toEqual(
      mockScanSourceSchema.additionalMetadata.userFriendlyName);

    // check if the form has the all the json schema property names
    Object.keys(mockScanSourceSchema.jsonSchema.properties!).forEach((propertyName) => {
      expect((jsonSchemaFormElement.nativeElement as HTMLElement).innerHTML).toContain(propertyName);
    });
  });

  /**
   * test if the property editor correctly receives the operator unhighlight stream
   *  and clears all the operator data, and hide the form.
   */
  it('should clear and hide the property editor panel correctly when no operator is highlighted', () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add and highlight an operator
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    jointGraphWrapper.highlightOperator(mockScanPredicate.operatorID);

    // unhighlight the operator
    jointGraphWrapper.unhighlightOperator(mockScanPredicate.operatorID);
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([]);

    fixture.detectChanges();

    // check if the clearPropertyEditor called after the operator
    //  is unhighlighted has correctly updated the variables
    expect(component.currentOperatorID).toBeFalsy();
    expect(component.currentOperatorSchema).toBeFalsy();
    expect(component.currentOperatorInitialData).toBeFalsy();
    expect(component.displayForm).toBeFalsy();

    // check HTML form are not displayed
    const formTitleElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-title'));
    const jsonSchemaFormElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));

    expect(formTitleElement).toBeFalsy();
    expect(jsonSchemaFormElement).toBeFalsy();
  });

  it('should clear and hide the property editor panel correctly when multiple operators are highlighted', () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add and highlight two operators
    workflowActionService.addOperatorsAndLinks([{op: mockScanPredicate, pos: mockPoint},
      {op: mockResultPredicate, pos: mockPoint}], []);
    jointGraphWrapper.highlightOperators([mockScanPredicate.operatorID, mockResultPredicate.operatorID]);

    // assert that multiple operators are highlighted
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toContain(mockResultPredicate.operatorID);
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toContain(mockScanPredicate.operatorID);

    // expect that the property editor is cleared
    expect(component.currentOperatorID).toBeFalsy();
    expect(component.currentOperatorSchema).toBeFalsy();
    expect(component.currentOperatorInitialData).toBeFalsy();
    expect(component.displayForm).toBeFalsy();

    // check HTML form are not displayed
    const formTitleElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-title'));
    const jsonSchemaFormElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));

    expect(formTitleElement).toBeFalsy();
    expect(jsonSchemaFormElement).toBeFalsy();
  });

  it('should change Texera graph property when the form is edited by the user', fakeAsync(() => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add an operator and highlight the operator so that the
    //  variables in property editor component is set correctly
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    jointGraphWrapper.highlightOperator(mockScanPredicate.operatorID);

    // stimulate a form change by the user
    const formChangeValue = { tableName: 'twitter_sample' };
    component.onFormChanges(formChangeValue);

    // maintain a counter of how many times the event is emitted
    let emitEventCounter = 0;
    component.outputFormChangeEventStream.subscribe(() => emitEventCounter++);

    // fakeAsync enables tick, which waits for the set property debounce time to finish
    tick(PropertyEditorComponent.formInputDebounceTime + 10);

    // then get the opeator, because operator is immutable, the operator before the tick
    //   is a different object reference from the operator after the tick
    const operator = workflowActionService.getTexeraGraph().getOperator(mockScanPredicate.operatorID);
    if (!operator) {
      throw new Error(`operator ${mockScanPredicate.operatorID} is undefined`);
    }
    expect(operator.operatorProperties).toEqual(formChangeValue);
    expect(emitEventCounter).toEqual(1);
  }));

  it('should debounce the user form input to avoid emitting event too frequently', marbles(m => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add an operator and highlight the operator so that the
    //  variables in property editor component is set correctly
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    jointGraphWrapper.highlightOperator(mockScanPredicate.operatorID);

    // prepare the form user input event stream
    // simulate user types in `table` character by character
    const formUserInputMarbleString = '-a-b-c-d-e';
    const formUserInputMarbleValue = {
      a: { tableName: 't' },
      b: { tableName: 'ta' },
      c: { tableName: 'tab' },
      d: { tableName: 'tabl' },
      e: { tableName: 'table' },
    };
    const formUserInputEventStream = m.hot(formUserInputMarbleString, formUserInputMarbleValue);

    // prepare the expected output stream after debounce time
    const formChangeEventMarbleStrig =
      // wait for the time of last marble string starting to emit
      '-'.repeat(formUserInputMarbleString.length - 1) +
      // then wait for debounce time (each tick represents 10 ms)
      '-'.repeat(PropertyEditorComponent.formInputDebounceTime / 10) +
      'e-';
    const formChangeEventMarbleValue = {
      e: { tableName: 'table' } as object
    };
    const expectedFormChangeEventStream = m.hot(formChangeEventMarbleStrig, formChangeEventMarbleValue);


    m.bind();

    const actualFormChangeEventStream = component.updateOperatorPropertyOnFormChange(formUserInputEventStream);
    formUserInputEventStream.subscribe();

    m.expect(actualFormChangeEventStream).toBeObservable(expectedFormChangeEventStream);

  }));

  it('should not emit operator property change event if the new property is the same as the old property', fakeAsync(() => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add an operator and highlight the operator so that the
    //  variables in property editor component is set correctly
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    const mockOperatorProperty = { tableName: 'table' };
    // set operator property first before displaying the operator property in property panel
    workflowActionService.setOperatorProperty(mockScanPredicate.operatorID, mockOperatorProperty);
    jointGraphWrapper.highlightOperator(mockScanPredicate.operatorID);


    // stimulate a form change with the same property
    component.onFormChanges(mockOperatorProperty);

    // maintain a counter of how many times the event is emitted
    let emitEventCounter = 0;
    component.outputFormChangeEventStream.subscribe(() => emitEventCounter++);

    // fakeAsync enables tick, which waits for the set property debounce time to finish
    tick(PropertyEditorComponent.formInputDebounceTime + 10);

    // assert that the form change event doesn't emit any time
    // because the form change value is the same
    expect(emitEventCounter).toEqual(0);

  }));


  it(`should display property description button when property description is provided, when clicked,
  should display the tooltip window on the GUI`, () => {
    expect(component.hasPropertyDescription).toBeFalsy();
    expect(component.propertyDescription.size).toEqual(0);

    let buttonState = fixture.debugElement.query(By.css('.propertyDescriptionButton'));
    expect(buttonState).toBeFalsy();

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    component.changePropertyEditor(mockScanPredicate);
    fixture.detectChanges();
    buttonState = fixture.debugElement.query(By.css('.propertyDescriptionButton'));

    expect(buttonState).toBeTruthy();

    let tooltipWindow = fixture.debugElement.query(By.css('.tooltip'));
    expect(tooltipWindow).toBeFalsy();

    buttonState.triggerEventHandler('click', null);
    fixture.detectChanges();

    tooltipWindow = fixture.debugElement.query(By.css('.tooltip'));
    expect(tooltipWindow).toBeTruthy();

    expect(component.hasPropertyDescription).toBeTruthy();
    expect(component.propertyDescription.size).toBeGreaterThan(0);
  });

 it('should not display property description button when property description is undefined', () => {

    expect(component.hasPropertyDescription).toBeFalsy();
    expect(component.propertyDescription.size).toEqual(0);

    let buttonState = fixture.debugElement.query(By.css('.propertyDescriptionButton'));
    expect(buttonState).toBeFalsy();
    workflowActionService.addOperator(mockResultPredicate, mockPoint);

    component.changePropertyEditor(mockResultPredicate);
    fixture.detectChanges();
    buttonState = fixture.debugElement.query(By.css('.propertyDescriptionButton'));

    expect(buttonState).toBeFalsy();

    expect(component.hasPropertyDescription).toBeFalsy();
    expect(component.propertyDescription.size).toEqual(0);
  });

  describe('when linkBreakpoint is enabled', () => {
    beforeAll(() => {
      environment.linkBreakpointEnabled = true;
    });

    afterAll(() => {
      environment.linkBreakpointEnabled = false;
    });

    it('should change the content of property editor from an empty panel to breakpoint editor correctly', () => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      workflowActionService.addLink(mockScanResultLink);

      jointGraphWrapper.highlightLink(mockScanResultLink.linkID);

      fixture.detectChanges();

      // check variables are set correctly
      expect(component.currentLinkID!.linkID).toEqual(mockScanResultLink.linkID);
      expect(component.currentLinkBreakpointSchema).toEqual(mockBreakpointSchema);
      expect(component.currentBreakpointInitialData).toEqual(mockScanResultLink.breakpointProperties);
      expect(component.displayBreakpointEditor).toBeTruthy();

      // check HTML form are displayed
      const jsonSchemaFormElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));

      // check if the form has the all the json schema property names
      Object.keys(mockBreakpointSchema.jsonSchema.properties!).forEach((propertyName) => {
        expect((jsonSchemaFormElement.nativeElement as HTMLElement).innerHTML).toContain(propertyName);
      });
    });

    it('should switch the content of property editor to another link-breakpoint from the former link-breakpoint correctly', () => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockSentimentPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      workflowActionService.addLink(mockScanSentimentLink);
      workflowActionService.addLink(mockSentimentResultLink);

      // highlight the first link
      jointGraphWrapper.highlightLink(mockScanSentimentLink.linkID);
      fixture.detectChanges();

      // check the variables
      expect(component.currentLinkID!.linkID).toEqual(mockScanSentimentLink.linkID);
      expect(component.currentLinkBreakpointSchema).toEqual(mockBreakpointSchema);
      expect(component.currentBreakpointInitialData).toEqual(mockScanSentimentLink.breakpointProperties);
      expect(component.displayBreakpointEditor).toBeTruthy();

      // highlight the second link
      jointGraphWrapper.highlightLink(mockSentimentResultLink.linkID);
      fixture.detectChanges();

      expect(component.currentLinkID!.linkID).toEqual(mockSentimentResultLink.linkID);
      expect(component.currentLinkBreakpointSchema).toEqual(mockBreakpointSchema);
      expect(component.currentBreakpointInitialData).toEqual(mockSentimentResultLink.breakpointProperties);
      expect(component.displayBreakpointEditor).toBeTruthy();

      // check HTML form are displayed
      const jsonSchemaFormElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));

      // check if the form has the all the json schema property names
      Object.keys(mockBreakpointSchema.jsonSchema.properties!).forEach((propertyName) => {
        expect((jsonSchemaFormElement.nativeElement as HTMLElement).innerHTML).toContain(propertyName);
      });
    });

    it('should switch the content of property editor between link-breakpoint and a operator correctly', () => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      workflowActionService.addLink(mockScanResultLink);

      // highlight the operator
      jointGraphWrapper.highlightOperator(mockScanPredicate.operatorID);
      fixture.detectChanges();

      // check the variables
      expect(component.currentOperatorID).toEqual(mockScanPredicate.operatorID);
      expect(component.currentOperatorSchema).toEqual(mockScanSourceSchema);
      expect(component.currentOperatorInitialData).toEqual(mockScanPredicate.operatorProperties);
      expect(component.displayForm).toBeTruthy();
      expect(component.displayBreakpointEditor).toBeFalsy();

      // highlight the link
      jointGraphWrapper.highlightLink(mockScanResultLink.linkID);
      fixture.detectChanges();

      // check the variables
      expect(component.currentLinkID!.linkID).toEqual(mockScanResultLink.linkID);
      expect(component.currentLinkBreakpointSchema).toEqual(mockBreakpointSchema);
      expect(component.currentBreakpointInitialData).toEqual(mockScanResultLink.breakpointProperties);
      expect(component.displayBreakpointEditor).toBeTruthy();

      expect(component.currentOperatorID).toBeUndefined();
      expect(component.currentOperatorSchema).toBeUndefined();
      expect(component.currentOperatorInitialData).toBeUndefined();
      expect(component.displayForm).toBeFalsy();

      // highlight the operator again
      jointGraphWrapper.highlightOperator(mockScanPredicate.operatorID);
      fixture.detectChanges();

      // check the variables
      expect(component.currentOperatorID).toEqual(mockScanPredicate.operatorID);
      expect(component.currentOperatorSchema).toEqual(mockScanSourceSchema);
      expect(component.currentOperatorInitialData).toEqual(mockScanPredicate.operatorProperties);
      expect(component.displayForm).toBeTruthy();

      expect(component.currentLinkID).toBeUndefined();
      expect(component.currentLinkBreakpointSchema).toBeUndefined();
      expect(component.currentBreakpointInitialData).toBeUndefined();
      expect(component.displayBreakpointEditor).toBeFalsy();
    });

    it('should clear and hide the property editor panel correctly on unhighlighting an link', () => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      workflowActionService.addLink(mockScanResultLink);

      jointGraphWrapper.highlightLink(mockScanResultLink.linkID);
      fixture.detectChanges();

      expect(component.currentLinkID!.linkID).toEqual(mockScanResultLink.linkID);
      expect(component.currentLinkBreakpointSchema).toEqual(mockBreakpointSchema);
      expect(component.currentBreakpointInitialData).toEqual(mockScanResultLink.breakpointProperties);
      expect(component.displayBreakpointEditor).toBeTruthy();

      // unhighlight the highlighted link
      jointGraphWrapper.unhighlightLink(mockScanResultLink.linkID);
      fixture.detectChanges();

      expect(component.currentLinkID).toBeUndefined();
      expect(component.currentLinkBreakpointSchema).toBeUndefined();
      expect(component.currentBreakpointInitialData).toBeUndefined();
      expect(component.displayBreakpointEditor).toBeFalsy();

      // check HTML form are not displayed
      const formTitleElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-title'));
      const jsonSchemaFormElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));

      expect(formTitleElement).toBeFalsy();
      expect(jsonSchemaFormElement).toBeFalsy();
    });

    it('should clear and hide the property editor panel correctly on clicking the remove button on breakpoint editor', () => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

      let buttonState = fixture.debugElement.query(By.css('.breakpointRemoveButton'));
      expect(buttonState).toBeFalsy();

      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      workflowActionService.addLink(mockScanResultLink);

      jointGraphWrapper.highlightLink(mockScanResultLink.linkID);
      fixture.detectChanges();

      buttonState = fixture.debugElement.query(By.css('.breakpointRemoveButton'));
      expect(buttonState).toBeTruthy();

      buttonState.triggerEventHandler('click', null);
      fixture.detectChanges();

      expect(component.currentLinkID).toBeUndefined();
      expect(component.currentLinkBreakpointSchema).toBeUndefined();
      expect(component.currentBreakpointInitialData).toBeUndefined();
      expect(component.displayBreakpointEditor).toBeFalsy();

      // check HTML form are not displayed
      const formTitleElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-title'));
      const jsonSchemaFormElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));

      expect(formTitleElement).toBeFalsy();
      expect(jsonSchemaFormElement).toBeFalsy();
    });

    it('should change Texera graph link-breakpoint property correctly when the breakpoint form is edited by the user', fakeAsync(() => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

      // add a link and highligh the link so that the
      //  variables in property editor component is set correctly
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      workflowActionService.addLink(mockScanResultLink);
      jointGraphWrapper.highlightLink(mockScanResultLink.linkID);
      fixture.detectChanges();

      // stimulate a form change by the user
      const formChangeValue = { attribute: 'age' };
      component.onFormChanges(formChangeValue);

      // maintain a counter of how many times the event is emitted
      let emitEventCounter = 0;
      component.outputBreakpointChangeEventStream.subscribe(() => emitEventCounter++);

      // fakeAsync enables tick, which waits for the set property debounce time to finish
      tick(PropertyEditorComponent.formInputDebounceTime + 10);

      // then get the opeator, because operator is immutable, the operator before the tick
      //   is a different object reference from the operator after the tick
      const link = workflowActionService.getTexeraGraph().getLinkWithID(mockScanResultLink.linkID);
      if (!link) {
        throw new Error(`link ${mockScanResultLink.linkID} is undefined`);
      }
      expect(link.breakpointProperties).toEqual(formChangeValue);
      expect(emitEventCounter).toEqual(1);
    }));

    it('should remove Texera graph link-breakpoint property correctly when the breakpoint remove button is clicked', fakeAsync(() => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

      // add a link and highligh the link so that the
      //  variables in property editor component is set correctly
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      workflowActionService.addLink(mockScanResultLink);
      jointGraphWrapper.highlightLink(mockScanResultLink.linkID);
      fixture.detectChanges();

      // stimulate a form change by the user
      const formChangeValue = { attribute: 'age' };
      component.onFormChanges(formChangeValue);

      // fakeAsync enables tick, which waits for the set property debounce time to finish
      tick(PropertyEditorComponent.formInputDebounceTime + 10);

      // then get the opeator, because operator is immutable, the operator before the tick
      //   is a different object reference from the operator after the tick
      let link = workflowActionService.getTexeraGraph().getLinkWithID(mockScanResultLink.linkID);
      if (!link) {
        throw new Error(`link ${mockScanResultLink.linkID} is undefined`);
      }
      expect(link.breakpointProperties).toEqual(formChangeValue);

      // simulate button click
      const buttonState = fixture.debugElement.query(By.css('.breakpointRemoveButton'));
      expect(buttonState).toBeTruthy();

      buttonState.triggerEventHandler('click', null);
      fixture.detectChanges();

      link = workflowActionService.getTexeraGraph().getLinkWithID(mockScanResultLink.linkID);
      if (!link) {
        throw new Error(`link ${mockScanResultLink.linkID} is undefined`);
      }
      const emptyProperty = {};
      expect(link.breakpointProperties).toEqual(emptyProperty);
    }));

    it('should debounce the user breakpoint form input to avoid emitting event too frequently', marbles(m => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

      // add a link and highligh the link so that the
      //  variables in property editor component is set correctly
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      workflowActionService.addLink(mockScanResultLink);
      jointGraphWrapper.highlightLink(mockScanResultLink.linkID);
      fixture.detectChanges();

      // prepare the form user input event stream
      // simulate user types in `table` character by character
      const formUserInputMarbleString = '-a-b-c-d-e';
      const formUserInputMarbleValue = {
        a: { tableName: 'p' },
        b: { tableName: 'pr' },
        c: { tableName: 'pri' },
        d: { tableName: 'pric' },
        e: { tableName: 'price' },
      };
      const formUserInputEventStream = m.hot(formUserInputMarbleString, formUserInputMarbleValue);

      // prepare the expected output stream after debounce time
      const formChangeEventMarbleStrig =
        // wait for the time of last marble string starting to emit
        '-'.repeat(formUserInputMarbleString.length - 1) +
        // then wait for debounce time (each tick represents 10 ms)
        '-'.repeat(PropertyEditorComponent.formInputDebounceTime / 10) +
        'e-';
      const formChangeEventMarbleValue = {
        e: { tableName: 'price' } as object
      };
      const expectedFormChangeEventStream = m.hot(formChangeEventMarbleStrig, formChangeEventMarbleValue);


      m.bind();

      const actualFormChangeEventStream = component.createoutputBreakpointChangeEventStream(formUserInputEventStream);
      formUserInputEventStream.subscribe();

      m.expect(actualFormChangeEventStream).toBeObservable(expectedFormChangeEventStream);
    }));

    it('should not emit breakpoint property change event if the new property is the same as the old property', fakeAsync(() => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();


      // add a link and highligh the link so that the
      //  variables in property editor component is set correctly
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      workflowActionService.addLink(mockScanResultLink);
      const mockBreakpointProperty = { attribute: 'price'};
      workflowActionService.setLinkBreakpoint(mockScanResultLink.linkID, mockBreakpointProperty);
      jointGraphWrapper.highlightLink(mockScanResultLink.linkID);
      fixture.detectChanges();

      // stimulate a form change with the same property
      component.onFormChanges(mockBreakpointProperty);

      // maintain a counter of how many times the event is emitted
      let emitEventCounter = 0;
      component.outputBreakpointChangeEventStream.subscribe(() => emitEventCounter++);

      // fakeAsync enables tick, which waits for the set property debounce time to finish
      tick(PropertyEditorComponent.formInputDebounceTime + 10);

      // assert that the form change event doesn't emit any time
      // because the form change value is the same
      expect(emitEventCounter).toEqual(0);
    }));


  });
});

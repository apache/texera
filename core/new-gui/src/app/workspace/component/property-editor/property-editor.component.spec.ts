import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { PropertyEditorComponent } from './property-editor.component';
import { environment } from '../../../../environments/environment';
import {
  mockPoint,
  mockResultPredicate,
  mockScanPredicate,
  mockScanResultLink,
  mockScanSentimentLink,
  mockSentimentPredicate,
  mockSentimentResultLink
} from '../../service/workflow-graph/model/mock-workflow-data';
import { By } from '@angular/platform-browser';
import { WorkflowActionService } from '../../service/workflow-graph/model/workflow-action.service';
import { OperatorPropertyEditFrameComponent } from './operator-property-edit-frame/operator-property-edit-frame.component';
import * as Ajv from 'ajv';
import { cloneDeep } from 'lodash';
import { mockBreakpointSchema, mockViewResultsSchema } from '../../service/operator-metadata/mock-operator-metadata.data';
import { JSONSchema7 } from 'json-schema';
import { BreakpointPropertyEditFrameComponent } from './breakpoint-property-edit-frame/breakpoint-property-edit-frame.component';
import { assertType } from '../../../common/util/assert';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { OperatorMetadataService } from '../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../../service/operator-metadata/stub-operator-metadata.service';


fdescribe('PropertyEditorComponent', () => {
  let component: PropertyEditorComponent;
  let fixture: ComponentFixture<PropertyEditorComponent>;
  let workflowActionService: WorkflowActionService;
  environment.schemaPropagationEnabled = true;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService }
      ]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PropertyEditorComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.inject(WorkflowActionService);
    fixture.detectChanges();

  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  /**
   * test if the property editor correctly receives the operator unhighlight stream
   *  and clears all the operator data, and hide the form.
   */
  it('should clear and hide the property editor panel correctly when no operator is highlighted', () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add and highlight an operator
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);

    fixture.detectChanges();

    expect(component.frameComponent instanceof OperatorPropertyEditFrameComponent);

    // unhighlight the operator
    jointGraphWrapper.unhighlightOperators(mockScanPredicate.operatorID);
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([]);

    fixture.detectChanges();

    // check if the clearPropertyEditor called after the operator
    //  is unhighlighted has correctly updated the variables
    expect(component.frameComponent === undefined);

    // check HTML form are not displayed
    const formTitleElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-title'));
    const jsonSchemaFormElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));

    expect(formTitleElement).toBeFalsy();
    expect(jsonSchemaFormElement).toBeFalsy();
  });

  it('should clear and hide the property editor panel correctly when multiple operators are highlighted', () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add and highlight two operators
    workflowActionService.addOperatorsAndLinks([{ op: mockScanPredicate, pos: mockPoint },
      { op: mockResultPredicate, pos: mockPoint }], []);
    jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID, mockResultPredicate.operatorID);

    // assert that multiple operators are highlighted
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toContain(mockResultPredicate.operatorID);
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toContain(mockScanPredicate.operatorID);

    fixture.detectChanges();

    // expect that the property editor is cleared
    expect(component.frameComponent === undefined);

    // check HTML form are not displayed
    const formTitleElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-title'));
    const jsonSchemaFormElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));

    expect(formTitleElement).toBeFalsy();
    expect(jsonSchemaFormElement).toBeFalsy();
  });

  fit('should switch the content of property editor to another operator from the former operator correctly', () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add two operators
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockResultPredicate, mockPoint);

    // highlight the first operator
    jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);

    fixture.detectChanges();

    // check the variables
    expect(component.frameComponent === OperatorPropertyEditFrameComponent);

    jointGraphWrapper.unhighlightOperators(mockScanPredicate.operatorID);
    fixture.detectChanges();

    expect(component.frameComponent === undefined);

    // highlight the second operator
    jointGraphWrapper.highlightOperators(mockResultPredicate.operatorID);
    fixture.detectChanges();

    // result operator has default values, use ajv to fill in default values
    // expected form output should fill in all default values instead of an empty object
    const ajv = new Ajv({ useDefaults: true });
    const expectedResultOperatorProperties = cloneDeep(mockResultPredicate.operatorProperties);
    ajv.validate(mockViewResultsSchema.jsonSchema, expectedResultOperatorProperties);

    expect(component.frameComponent === OperatorPropertyEditFrameComponent);
    const injectedElem = fixture.debugElement.query(
      By.directive(OperatorPropertyEditFrameComponent),
    );
    fixture.detectChanges();
    expect(injectedElem).not.toBeNull();

    // check HTML form are displayed
    const formTitleElementAfterChange = fixture.debugElement.query(By.css('.texera-workspace-property-editor-title'));
    const jsonSchemaFormElementAfterChange = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));

    // check the panel title
    expect((formTitleElementAfterChange.nativeElement as HTMLElement).innerText).toEqual(
      mockViewResultsSchema.additionalMetadata.userFriendlyName);

    // check if the form has the all the json schema property names
    Object.entries(mockViewResultsSchema.jsonSchema.properties as any).forEach((entry) => {
      const propertyTitle = (entry[1] as JSONSchema7).title;
      if (propertyTitle) {
        expect((jsonSchemaFormElementAfterChange.nativeElement as HTMLElement).innerHTML).toContain(propertyTitle);
      }
      const propertyDescription = (entry[1] as JSONSchema7).description;
      if (propertyDescription) {
        expect((jsonSchemaFormElementAfterChange.nativeElement as HTMLElement).innerHTML).toContain(propertyDescription);
      }
    });

  });

  it('should clear and hide the property editor panel correctly on unhighlighting an link', () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    workflowActionService.addLink(mockScanResultLink);

    jointGraphWrapper.highlightLink(mockScanResultLink.linkID);
    fixture.detectChanges();

    expect(component.frameComponent instanceof BreakpointPropertyEditFrameComponent);
    // unhighlight the highlighted link
    jointGraphWrapper.unhighlightLink(mockScanResultLink.linkID);
    fixture.detectChanges();

    expect(component.frameComponent).toBeUndefined();

    // check HTML form are not displayed
    const formTitleElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-title'));
    const jsonSchemaFormElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));

    expect(formTitleElement).toBeFalsy();
    expect(jsonSchemaFormElement).toBeFalsy();
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
    // expect(component.currentLinkID).toEqual(mockScanSentimentLink.linkID);

    // highlight the second link
    jointGraphWrapper.highlightLink(mockSentimentResultLink.linkID);
    fixture.detectChanges();

    // expect(component.currentLinkID).toEqual(mockSentimentResultLink.linkID);

    // check HTML form are displayed
    const jsonSchemaFormElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));

    // check if the form has the all the json schema property names
    Object.values((mockBreakpointSchema.jsonSchema.oneOf as any)[0].properties).forEach((property: unknown) => {
      assertType<{ type: string, title: string }>(property);
      expect((jsonSchemaFormElement.nativeElement as HTMLElement).innerHTML).toContain(property.title);
    });
  });


// xit('should change Texera graph link-breakpoint property correctly when the breakpoint form is edited by the user', fakeAsync(() => {
//   const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

//   // add a link and highlight the link so that the
//   //  variables in property editor component is set correctly
//   workflowActionService.addOperator(mockScanPredicate, mockPoint);
//   workflowActionService.addOperator(mockResultPredicate, mockPoint);
//   workflowActionService.addLink(mockScanResultLink);
//   jointGraphWrapper.highlightLink(mockScanResultLink.linkID);
//   fixture.detectChanges();

//   // stimulate a form change by the user
//   const formChangeValue = { attribute: 'age' };
//   component.onFormChanges(formChangeValue);

//   // maintain a counter of how many times the event is emitted
//   let emitEventCounter = 0;
//   component.outputBreakpointChangeEventStream.subscribe(() => emitEventCounter++);

//   // fakeAsync enables tick, which waits for the set property debounce time to finish
//   tick(PropertyEditorComponent.formInputDebounceTime + 10);

//   // then get the operator, because operator is immutable, the operator before the tick
//   //   is a different object reference from the operator after the tick
//   const link = workflowActionService.getTexeraGraph().getLinkWithID(mockScanResultLink.linkID);
//   if (!link) {
//     throw new Error(`link ${mockScanResultLink.linkID} is undefined`);
//   }
//   expect(link.breakpointProperties).toEqual(formChangeValue);
//   expect(emitEventCounter).toEqual(1);
// }));


// xit('should debounce the user breakpoint form input to avoid emitting event too frequently', marbles(m => {
//   const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

//   // add a link and highlight the link so that the
//   //  variables in property editor component is set correctly
//   workflowActionService.addOperator(mockScanPredicate, mockPoint);
//   workflowActionService.addOperator(mockResultPredicate, mockPoint);
//   workflowActionService.addLink(mockScanResultLink);
//   jointGraphWrapper.highlightLink(mockScanResultLink.linkID);
//   fixture.detectChanges();

//   // prepare the form user input event stream
//   // simulate user types in `table` character by character
//   const formUserInputMarbleString = '-a-b-c-d-e';
//   const formUserInputMarbleValue = {
//     a: { tableName: 'p' },
//     b: { tableName: 'pr' },
//     c: { tableName: 'pri' },
//     d: { tableName: 'pric' },
//     e: { tableName: 'price' },
//   };
//   const formUserInputEventStream = m.hot(formUserInputMarbleString, formUserInputMarbleValue);

//   // prepare the expected output stream after debounce time
//   const formChangeEventMarbleStrig =
//     // wait for the time of last marble string starting to emit
//     '-'.repeat(formUserInputMarbleString.length - 1) +
//     // then wait for debounce time (each tick represents 10 ms)
//     '-'.repeat(PropertyEditorComponent.formInputDebounceTime / 10) +
//     'e-';
//   const formChangeEventMarbleValue = {
//     e: { tableName: 'price' } as object
//   };
//   const expectedFormChangeEventStream = m.hot(formChangeEventMarbleStrig, formChangeEventMarbleValue);

//   m.bind();

//   const actualFormChangeEventStream = component.createoutputBreakpointChangeEventStream(formUserInputEventStream);
//   formUserInputEventStream.subscribe();

//   m.expect(actualFormChangeEventStream).toBeObservable(expectedFormChangeEventStream);
// }));

// xit('should not emit breakpoint property change event if the new property is the same as the old property', fakeAsync(() => {
//   const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

//   // add a link and highligh the link so that the
//   //  variables in property editor component is set correctly
//   workflowActionService.addOperator(mockScanPredicate, mockPoint);
//   workflowActionService.addOperator(mockResultPredicate, mockPoint);
//   workflowActionService.addLink(mockScanResultLink);
//   const mockBreakpointProperty = { attribute: 'price'};
//   workflowActionService.setLinkBreakpoint(mockScanResultLink.linkID, mockBreakpointProperty);
//   jointGraphWrapper.highlightLink(mockScanResultLink.linkID);
//   fixture.detectChanges();

//   // stimulate a form change with the same property
//   component.onFormChanges(mockBreakpointProperty);

//   // maintain a counter of how many times the event is emitted
//   let emitEventCounter = 0;
//   component.outputBreakpointChangeEventStream.subscribe(() => emitEventCounter++);

//   // fakeAsync enables tick, which waits for the set property debounce time to finish
//   tick(PropertyEditorComponent.formInputDebounceTime + 10);

//   // assert that the form change event doesn't emit any time
//   // because the form change value is the same
//   expect(emitEventCounter).toEqual(0);
// }));
});


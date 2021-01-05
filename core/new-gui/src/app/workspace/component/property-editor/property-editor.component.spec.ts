import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { BrowserModule, By } from '@angular/platform-browser';
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
import { FormlyModule } from '@ngx-formly/core';
import { TEXERA_FORMLY_CONFIG } from 'src/app/common/formly/formly-config';
import { FormlyMaterialModule } from '@ngx-formly/material';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { ExecuteWorkflowService } from '../../service/execute-workflow/execute-workflow.service';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { FormlyNgZorroAntdModule } from '@ngx-formly/ng-zorro-antd';
import { AppRoutingModule } from 'src/app/app-routing.module';
import { NgxAceModule } from 'ngx-ace-icy';
import { RouterModule } from '@angular/router';
import { CommonModule } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { FormlyJsonschema } from '@ngx-formly/core/json-schema';
import { ArrayTypeComponent } from 'src/app/common/formly/array.type';
import { ObjectTypeComponent } from 'src/app/common/formly/object.type';
import { MultiSchemaTypeComponent } from 'src/app/common/formly/multischema.type';
import { NullTypeComponent } from 'src/app/common/formly/null.type';
import { JSONSchema7 } from 'json-schema';

import * as Ajv from 'ajv';
import { cloneDeep } from 'lodash';
import { assertType } from 'src/app/common/util/assert';
import { WorkflowUtilService } from '../../service/workflow-graph/util/workflow-util.service';

/* tslint:disable:no-non-null-assertion */

describe('PropertyEditorComponent', () => {
  let component: PropertyEditorComponent;
  let fixture: ComponentFixture<PropertyEditorComponent>;
  let workflowActionService: WorkflowActionService;
  let dynamicSchemaService: DynamicSchemaService;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [
        PropertyEditorComponent,
        ArrayTypeComponent,
        ObjectTypeComponent,
        MultiSchemaTypeComponent,
        NullTypeComponent,
      ],
      providers: [
        JointUIService,
        WorkflowActionService,
        WorkflowUtilService,
        UndoRedoService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        DynamicSchemaService,
        ExecuteWorkflowService,
        FormlyJsonschema
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
        HttpClientTestingModule,
      ]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PropertyEditorComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.inject(WorkflowActionService);
    dynamicSchemaService = TestBed.inject(DynamicSchemaService);
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
    console.log(workflowActionService.getTexeraGraph());

    // check if the changePropertyEditor called after the operator
    //  is highlighted has correctly updated the variables
    const predicate = mockScanPredicate;

    // add and highlight an operator
    workflowActionService.addOperator(predicate, mockPoint);
    jointGraphWrapper.highlightOperators(predicate.operatorID);

    fixture.detectChanges();
    // check variables are set correctly
    expect(component.currentOperatorID).toEqual(predicate.operatorID);
    expect(component.formData).toEqual(predicate.operatorProperties);
    expect(component.displayForm).toBeTruthy();

    // check HTML form are displayed
    const formTitleElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-title'));
    const jsonSchemaFormElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));
    // check the panel title
    expect((formTitleElement.nativeElement as HTMLElement).innerText).toEqual(
      mockScanSourceSchema.additionalMetadata.userFriendlyName);

    // check if the form has the all the json schema property names
    Object.entries(mockScanSourceSchema.jsonSchema.properties!).forEach((entry) => {
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


  it('should switch the content of property editor to another operator from the former operator correctly', () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add two operators
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockResultPredicate, mockPoint);

    // highlight the first operator
    jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);
    fixture.detectChanges();

    // check the variables
    expect(component.currentOperatorID).toEqual(mockScanPredicate.operatorID);
    expect(component.formData).toEqual(mockScanPredicate.operatorProperties);
    expect(component.displayForm).toBeTruthy();

    // highlight the second operator
    jointGraphWrapper.highlightOperators(mockResultPredicate.operatorID);
    fixture.detectChanges();

    // result operator has default values, use ajv to fill in default values
    // expected form output should fill in all default values instead of an empty object
    const ajv = new Ajv({ useDefaults: true });
    const expectedResultOperatorProperties = cloneDeep(mockResultPredicate.operatorProperties);
    ajv.validate(mockViewResultsSchema.jsonSchema, expectedResultOperatorProperties);

    expect(component.currentOperatorID).toEqual(mockResultPredicate.operatorID);
    expect(component.formData).toEqual(expectedResultOperatorProperties);
    expect(component.displayForm).toBeTruthy();


    // check HTML form are displayed
    const formTitleElementAfterChange = fixture.debugElement.query(By.css('.texera-workspace-property-editor-title'));
    const jsonSchemaFormElementAfterChange = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));

    // check the panel title
    expect((formTitleElementAfterChange.nativeElement as HTMLElement).innerText).toEqual(
      mockViewResultsSchema.additionalMetadata.userFriendlyName);

    // check if the form has the all the json schema property names
    Object.entries(mockViewResultsSchema.jsonSchema.properties!).forEach((entry) => {
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

  /**
   * test if the property editor correctly receives the operator unhighlight stream
   *  and displays the operator's data when it's the only highlighted operator.
   */
  it('should switch the content of property editor to the highlighted operator correctly when only one operator is highlighted', () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add and highlight two operators, then unhighlight one of them
    workflowActionService.addOperatorsAndLinks([{op: mockScanPredicate, pos: mockPoint},
      {op: mockResultPredicate, pos: mockPoint}], []);
    jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID, mockResultPredicate.operatorID);
    jointGraphWrapper.unhighlightOperators(mockResultPredicate.operatorID);

    // assert that only one operator is highlighted on the graph
    const predicate = mockScanPredicate;
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([predicate.operatorID]);

    fixture.detectChanges();

    // check if the changePropertyEditor called after the operator
    //  is unhighlighted has correctly updated the variables

    // check variables are set correctly
    expect(component.currentOperatorID).toEqual(predicate.operatorID);
    expect(component.formData).toEqual(predicate.operatorProperties);
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
    jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);

    // unhighlight the operator
    jointGraphWrapper.unhighlightOperators(mockScanPredicate.operatorID);
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([]);

    fixture.detectChanges();

    // check if the clearPropertyEditor called after the operator
    //  is unhighlighted has correctly updated the variables
    expect(component.currentOperatorID).toBeFalsy();
    expect(component.formData).toBeFalsy();
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
    jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID, mockResultPredicate.operatorID);

    // assert that multiple operators are highlighted
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toContain(mockResultPredicate.operatorID);
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toContain(mockScanPredicate.operatorID);

    // expect that the property editor is cleared
    expect(component.currentOperatorID).toBeFalsy();
    expect(component.formData).toBeFalsy();
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
    jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);

    // stimulate a form change by the user
    const formChangeValue = { tableName: 'twitter_sample' };
    component.onFormChanges(formChangeValue);

    // maintain a counter of how many times the event is emitted
    let emitEventCounter = 0;
    component.operatorPropertyChangeStream.subscribe(() => emitEventCounter++);

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


    // stimulate a form change with the same property
    component.onFormChanges(mockOperatorProperty);

    // maintain a counter of how many times the event is emitted
    let emitEventCounter = 0;
    component.operatorPropertyChangeStream.subscribe(() => emitEventCounter++);

    // fakeAsync enables tick, which waits for the set property debounce time to finish
    tick(PropertyEditorComponent.formInputDebounceTime + 10);

    // assert that the form change event doesn't emit any time
    // because the form change value is the same
    expect(emitEventCounter).toEqual(0);

  }));

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
      expect(component.currentLinkID).toEqual(mockScanResultLink.linkID);
      expect(component.formData).toEqual({});

      // check HTML form are displayed
      const jsonSchemaFormElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));
      // check if the form has the all the json schema property names
      Object.values((mockBreakpointSchema.jsonSchema.oneOf as any)[0].properties).forEach((property: unknown) => {
        assertType<{type: string, title: string}>(property);
        expect((jsonSchemaFormElement.nativeElement as HTMLElement).innerHTML).toContain(property.title);
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
      expect(component.currentLinkID).toEqual(mockScanSentimentLink.linkID);

      // highlight the second link
      jointGraphWrapper.highlightLink(mockSentimentResultLink.linkID);
      fixture.detectChanges();

      expect(component.currentLinkID).toEqual(mockSentimentResultLink.linkID);

      // check HTML form are displayed
      const jsonSchemaFormElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));

      // check if the form has the all the json schema property names
      Object.values((mockBreakpointSchema.jsonSchema.oneOf as any)[0].properties).forEach((property: unknown) => {
        assertType<{type: string, title: string}>(property);
        expect((jsonSchemaFormElement.nativeElement as HTMLElement).innerHTML).toContain(property.title);
      });
    });

    it('should switch the content of property editor between link-breakpoint and a operator correctly', () => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      workflowActionService.addLink(mockScanResultLink);

      // highlight the operator
      jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);
      fixture.detectChanges();

      // check the variables
      expect(component.currentOperatorID).toEqual(mockScanPredicate.operatorID);
      expect(component.displayForm).toBeTruthy();

      // highlight the link
      jointGraphWrapper.highlightLink(mockScanResultLink.linkID);
      fixture.detectChanges();

      // check the variables
      expect(component.currentLinkID).toEqual(mockScanResultLink.linkID);

      expect(component.currentOperatorID).toBeUndefined();

      // highlight the operator again
      jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);
      fixture.detectChanges();

      // check the variables
      expect(component.currentOperatorID).toEqual(mockScanPredicate.operatorID);
      expect(component.displayForm).toBeTruthy();

      expect(component.currentLinkID).toBeUndefined();
    });

    it('should clear and hide the property editor panel correctly on unhighlighting an link', () => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      workflowActionService.addLink(mockScanResultLink);

      jointGraphWrapper.highlightLink(mockScanResultLink.linkID);
      fixture.detectChanges();

      expect(component.currentLinkID).toEqual(mockScanResultLink.linkID);
      // unhighlight the highlighted link
      jointGraphWrapper.unhighlightLink(mockScanResultLink.linkID);
      fixture.detectChanges();

      expect(component.currentLinkID).toBeUndefined();

      // check HTML form are not displayed
      const formTitleElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-title'));
      const jsonSchemaFormElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));

      expect(formTitleElement).toBeFalsy();
      expect(jsonSchemaFormElement).toBeFalsy();
    });

    it('should add a breakpoint when clicking add breakpoint', () => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

      // for some reason, all the breakpoint interaction buttons (add, modify, remove) are class 'breakpointRemoveButton' ???
      let buttonState = fixture.debugElement.query(By.css('.breakpointRemoveButton'));
      expect(buttonState).toBeFalsy();

      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      workflowActionService.addLink(mockScanResultLink);

      jointGraphWrapper.highlightLink(mockScanResultLink.linkID);
      fixture.detectChanges();

      // after adding breakpoint, this should be the addbreakpoint button
      buttonState = fixture.debugElement.query(By.css('.breakpointRemoveButton'));
      expect(buttonState).toBeTruthy();

      spyOn(workflowActionService, 'setLinkBreakpoint');
      component.formData = {count: 3};
      buttonState.triggerEventHandler('click', null);
      expect(workflowActionService.setLinkBreakpoint).toHaveBeenCalledTimes(1);


    });

    it('should clear and hide the property editor panel correctly on clicking the remove button on breakpoint editor', () => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

      // for some reason, all the breakpoint interaction buttons (add, modify, remove) are class 'breakpointRemoveButton' ???
      let buttonState = fixture.debugElement.query(By.css('.breakpointRemoveButton'));
      expect(buttonState).toBeFalsy();

      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      workflowActionService.addLink(mockScanResultLink);

      jointGraphWrapper.highlightLink(mockScanResultLink.linkID);
      fixture.detectChanges();

      // simulate adding a breakpoint
      component.formData = {count: 3};
      component.handleAddBreakpoint();
      fixture.detectChanges();

      // after adding breakpoint, this should now be the remove breakpoint button
      buttonState = fixture.debugElement.query(By.css('.breakpointRemoveButton'));
      expect(buttonState).toBeTruthy();

      buttonState.triggerEventHandler('click', null);
      fixture.detectChanges();
      expect(component.currentLinkID).toBeUndefined();
      // check HTML form are not displayed
      const formTitleElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-title'));
      const jsonSchemaFormElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));

      expect(formTitleElement).toBeFalsy();
      expect(jsonSchemaFormElement).toBeFalsy();
    });

    // xit('should change Texera graph link-breakpoint property correctly when the breakpoint form is edited by the user', fakeAsync(() => {
    //   const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    //   // add a link and highligh the link so that the
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

    //   // then get the opeator, because operator is immutable, the operator before the tick
    //   //   is a different object reference from the operator after the tick
    //   const link = workflowActionService.getTexeraGraph().getLinkWithID(mockScanResultLink.linkID);
    //   if (!link) {
    //     throw new Error(`link ${mockScanResultLink.linkID} is undefined`);
    //   }
    //   expect(link.breakpointProperties).toEqual(formChangeValue);
    //   expect(emitEventCounter).toEqual(1);
    // }));

    it('should remove Texera graph link-breakpoint property correctly when the breakpoint remove button is clicked', () => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

      // add a link and highligh the link so that the
      //  variables in property editor component is set correctly
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      workflowActionService.addLink(mockScanResultLink);
      jointGraphWrapper.highlightLink(mockScanResultLink.linkID);
      fixture.detectChanges();

      const formData = { count: 100 };
      // simulate adding a breakpoint
      component.formData = formData;
      component.handleAddBreakpoint();
      fixture.detectChanges();

      // check breakpoint
      let linkBreakpoint = workflowActionService.getTexeraGraph().getLinkBreakpoint(mockScanResultLink.linkID);
      if (!linkBreakpoint) {
        throw new Error(`link ${mockScanResultLink.linkID} is undefined`);
      }
      expect(linkBreakpoint).toEqual(formData);

      // simulate button click
      const buttonState = fixture.debugElement.query(By.css('.breakpointRemoveButton'));
      expect(buttonState).toBeTruthy();

      buttonState.triggerEventHandler('click', null);
      fixture.detectChanges();

      linkBreakpoint = workflowActionService.getTexeraGraph().getLinkBreakpoint(mockScanResultLink.linkID);
      expect(linkBreakpoint).toBeUndefined();
    });

    // xit('should debounce the user breakpoint form input to avoid emitting event too frequently', marbles(m => {
    //   const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    //   // add a link and highligh the link so that the
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

});

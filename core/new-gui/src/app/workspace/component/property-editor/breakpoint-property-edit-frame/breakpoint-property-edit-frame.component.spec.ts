import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BreakpointPropertyEditFrameComponent } from './breakpoint-property-edit-frame.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { environment } from '../../../../../environments/environment';
import {
  mockPoint,
  mockResultPredicate,
  mockScanPredicate,
  mockScanResultLink
} from '../../../service/workflow-graph/model/mock-workflow-data';
import { BrowserModule, By } from '@angular/platform-browser';
import { mockBreakpointSchema } from '../../../service/operator-metadata/mock-operator-metadata.data';
import { assertType } from '../../../../common/util/assert';
import { WorkflowActionService } from '../../../service/workflow-graph/model/workflow-action.service';
import { ArrayTypeComponent } from '../../../../common/formly/array.type';
import { ObjectTypeComponent } from '../../../../common/formly/object.type';
import { MultiSchemaTypeComponent } from '../../../../common/formly/multischema.type';
import { NullTypeComponent } from '../../../../common/formly/null.type';
import { JointUIService } from '../../../service/joint-ui/joint-ui.service';
import { WorkflowUtilService } from '../../../service/workflow-graph/util/workflow-util.service';
import { UndoRedoService } from '../../../service/undo-redo/undo-redo.service';
import { OperatorMetadataService } from '../../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../../../service/operator-metadata/stub-operator-metadata.service';
import { DynamicSchemaService } from '../../../service/dynamic-schema/dynamic-schema.service';
import { ExecuteWorkflowService } from '../../../service/execute-workflow/execute-workflow.service';
import { FormlyJsonschema } from '@ngx-formly/core/json-schema';
import { SchemaPropagationService } from '../../../service/dynamic-schema/schema-propagation/schema-propagation.service';
import { LoggerConfig, NGXLogger, NGXLoggerHttpService, NGXMapperService } from 'ngx-logger';
import { CommonModule, DatePipe } from '@angular/common';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { FormlyModule } from '@ngx-formly/core';
import { TEXERA_FORMLY_CONFIG } from '../../../../common/formly/formly-config';
import { FormlyMaterialModule } from '@ngx-formly/material';

describe('BreakpointPropertyEditFrameComponent', () => {
  let component: BreakpointPropertyEditFrameComponent;
  let fixture: ComponentFixture<BreakpointPropertyEditFrameComponent>;
  let workflowActionService: WorkflowActionService;
  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [
        BreakpointPropertyEditFrameComponent,
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

  });

  it('should create', () => {
    expect(component).toBeTruthy();
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

      fixture = TestBed.createComponent(BreakpointPropertyEditFrameComponent);
      component = fixture.componentInstance;
      fixture.detectChanges();

      // check variables are set correctly
      expect(component.currentLinkID).toEqual(mockScanResultLink.linkID);
      expect(component.formData).toEqual({});

      // check HTML form are displayed
      const jsonSchemaFormElement = fixture.debugElement.query(By.css('.texera-workspace-property-editor-form'));
      // check if the form has the all the json schema property names
      Object.values((mockBreakpointSchema.jsonSchema.oneOf as any)[0].properties).forEach((property: unknown) => {
        assertType<{ type: string, title: string }>(property);
        expect((jsonSchemaFormElement.nativeElement as HTMLElement).innerHTML).toContain(property.title);
      });
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
      fixture = TestBed.createComponent(BreakpointPropertyEditFrameComponent);
      component = fixture.componentInstance;
      fixture.detectChanges();

      // after adding breakpoint, this should be the addbreakpoint button
      buttonState = fixture.debugElement.query(By.css('.breakpointRemoveButton'));
      expect(buttonState).toBeTruthy();

      spyOn(workflowActionService, 'setLinkBreakpoint');
      component.formData = { count: 3 };
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
      fixture = TestBed.createComponent(BreakpointPropertyEditFrameComponent);
      component = fixture.componentInstance;
      fixture.detectChanges();

      // simulate adding a breakpoint
      component.formData = { count: 3 };
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

    it('should remove Texera graph link-breakpoint property correctly when the breakpoint remove button is clicked', () => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

      // add a link and highligh the link so that the
      //  variables in property editor component is set correctly
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      workflowActionService.addLink(mockScanResultLink);
      jointGraphWrapper.highlightLink(mockScanResultLink.linkID);
      fixture = TestBed.createComponent(BreakpointPropertyEditFrameComponent);
      component = fixture.componentInstance;
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
  });
});

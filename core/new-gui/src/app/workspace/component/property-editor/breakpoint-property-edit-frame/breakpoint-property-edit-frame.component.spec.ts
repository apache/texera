import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";

import { BreakpointPropertyEditFrameComponent } from "./breakpoint-property-edit-frame.component";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { environment } from "../../../../../environments/environment";
import {
  mockPoint,
  mockResultPredicate,
  mockScanPredicate,
  mockScanResultLink,
} from "../../../service/workflow-graph/model/mock-workflow-data";
import { By } from "@angular/platform-browser";
import { mockBreakpointSchema } from "../../../service/operator-metadata/mock-operator-metadata.data";
import { assertType } from "../../../../common/util/assert";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { ArrayTypeComponent } from "../../../../common/formly/array.type";
import { ObjectTypeComponent } from "../../../../common/formly/object.type";
import { MultiSchemaTypeComponent } from "../../../../common/formly/multischema.type";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { FormlyModule } from "@ngx-formly/core";
import { TEXERA_FORMLY_CONFIG } from "../../../../common/formly/formly-config";
import { FormlyMaterialModule } from "@ngx-formly/material";
import { SimpleChange } from "@angular/core";

describe("BreakpointPropertyEditFrameComponent", () => {
  let component: BreakpointPropertyEditFrameComponent;
  let fixture: ComponentFixture<BreakpointPropertyEditFrameComponent>;
  let workflowActionService: WorkflowActionService;
  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [
        BreakpointPropertyEditFrameComponent,
        ArrayTypeComponent,
        ObjectTypeComponent,
        MultiSchemaTypeComponent,
      ],
      providers: [
        WorkflowActionService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
      ],
      imports: [
        BrowserAnimationsModule,
        FormsModule,
        FormlyModule.forRoot(TEXERA_FORMLY_CONFIG),
        // formly ng zorro module has a bug that doesn't display field description,
        // FormlyNgZorroAntdModule,
        // use formly material module instead
        FormlyMaterialModule,
        ReactiveFormsModule,
        HttpClientTestingModule,
      ],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BreakpointPropertyEditFrameComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.inject(WorkflowActionService);
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("when linkBreakpoint is enabled", () => {
    beforeAll(() => {
      environment.linkBreakpointEnabled = true;
    });

    afterAll(() => {
      environment.linkBreakpointEnabled = false;
    });

    it("should change the content of property editor from an empty panel to breakpoint editor correctly", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      workflowActionService.addLink(mockScanResultLink);

      component.ngOnChanges({
        currentLinkId: new SimpleChange(undefined, mockScanResultLink.linkID, true),
      });

      fixture.detectChanges();

      // check variables are set correctly
      // expect(component.formData).toEqual({});

      // check HTML form are displayed
      const jsonSchemaFormElement = fixture.debugElement.query(By.css(".texera-workspace-property-editor-form"));
      // check if the form has the all the json schema property names
      Object.values((mockBreakpointSchema.jsonSchema.oneOf as any)[0].properties).forEach((property: unknown) => {
        assertType<{ type: string; title: string }>(property);
        expect((jsonSchemaFormElement.nativeElement as HTMLElement).innerHTML).toContain(property.title);
      });
    });

    it("should add a breakpoint upon clicking add breakpoint button", () => {
      // for some reason, all the breakpoint interaction buttons (add, modify, remove) are class 'breakpointRemoveButton' ???
      let buttonState = fixture.debugElement.query(By.css(".breakpointRemoveButton"));
      expect(buttonState).toBeFalsy();

      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      workflowActionService.addLink(mockScanResultLink);

      component.ngOnChanges({
        currentLinkId: new SimpleChange(undefined, mockScanResultLink.linkID, true),
      });
      fixture.detectChanges();

      // after adding breakpoint, this should be the add breakpoint button
      buttonState = fixture.debugElement.query(By.css(".breakpointRemoveButton"));
      expect(buttonState).toBeTruthy();

      spyOn(workflowActionService, "setLinkBreakpoint");
      component.formData = { count: 3 };
      buttonState.triggerEventHandler("click", null);
      fixture.detectChanges();
      expect(workflowActionService.setLinkBreakpoint).toHaveBeenCalledTimes(1);
    });

    it("should clear and hide the property editor panel correctly upon clicking the remove button on breakpoint editor", () => {
      // for some reason, all the breakpoint interaction buttons (add, modify, remove) are class 'breakpointRemoveButton' ???
      let buttonState = fixture.debugElement.query(By.css(".breakpointRemoveButton"));
      expect(buttonState).toBeFalsy();

      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      workflowActionService.addLink(mockScanResultLink);

      component.ngOnChanges({
        currentLinkId: new SimpleChange(undefined, mockScanResultLink.linkID, true),
      });
      fixture.detectChanges();

      // simulate adding a breakpoint
      component.formData = { count: 3 };
      component.handleAddBreakpoint();
      fixture.detectChanges();

      // after adding breakpoint, this should now be the remove breakpoint button
      buttonState = fixture.debugElement.query(By.css(".breakpointRemoveButton"));
      expect(buttonState).toBeTruthy();

      buttonState.triggerEventHandler("click", null);
      fixture.detectChanges();
      expect(component.currentLinkId).toBeUndefined();
      // check HTML form are not displayed
      const formTitleElement = fixture.debugElement.query(By.css(".texera-workspace-property-editor-title"));
      const jsonSchemaFormElement = fixture.debugElement.query(By.css(".texera-workspace-property-editor-form"));

      expect(formTitleElement).toBeFalsy();
      expect(jsonSchemaFormElement).toBeFalsy();
    });

    it("should remove Texera graph link-breakpoint property correctly upon clicking the breakpoint remove button", () => {
      // add a link and highlight the link so that the
      //  variables in property editor component is set correctly
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      workflowActionService.addLink(mockScanResultLink);

      component.ngOnChanges({
        currentLinkId: new SimpleChange(undefined, mockScanResultLink.linkID, true),
      });
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
      const buttonState = fixture.debugElement.query(By.css(".breakpointRemoveButton"));
      expect(buttonState).toBeTruthy();

      buttonState.triggerEventHandler("click", null);
      fixture.detectChanges();

      linkBreakpoint = workflowActionService.getTexeraGraph().getLinkBreakpoint(mockScanResultLink.linkID);
      expect(linkBreakpoint).toBeUndefined();
    });
  });
});

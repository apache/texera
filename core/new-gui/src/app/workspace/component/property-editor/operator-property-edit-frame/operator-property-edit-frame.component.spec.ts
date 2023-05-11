import { ComponentFixture, discardPeriodicTasks, fakeAsync, TestBed, tick, waitForAsync } from "@angular/core/testing";

import { OperatorPropertyEditFrameComponent } from "./operator-property-edit-frame.component";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { FORM_DEBOUNCE_TIME_MS } from "../../../service/execute-workflow/execute-workflow.service";
import { DatePipe } from "@angular/common";
import { By } from "@angular/platform-browser";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { FormlyModule } from "@ngx-formly/core";
import { TEXERA_FORMLY_CONFIG } from "../../../../common/formly/formly-config";
import { FormlyMaterialModule } from "@ngx-formly/material";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import {
  mockPoint,
  mockResultPredicate,
  mockScanPredicate,
} from "../../../service/workflow-graph/model/mock-workflow-data";
import {
  mockScanSourceSchema,
  mockViewResultsSchema,
} from "../../../service/operator-metadata/mock-operator-metadata.data";
import { JSONSchema7 } from "json-schema";
import { configure } from "rxjs-marbles";
import { SimpleChange } from "@angular/core";
import { cloneDeep } from "lodash-es";

import Ajv from "ajv";
import { COLLAB_DEBOUNCE_TIME_MS } from "../../../../common/formly/collab-wrapper/collab-wrapper/collab-wrapper.component";

const { marbles } = configure({ run: false });
describe("OperatorPropertyEditFrameComponent", () => {
  let component: OperatorPropertyEditFrameComponent;
  let fixture: ComponentFixture<OperatorPropertyEditFrameComponent>;
  let workflowActionService: WorkflowActionService;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [OperatorPropertyEditFrameComponent],
      providers: [
        WorkflowActionService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        DatePipe,
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
    fixture = TestBed.createComponent(OperatorPropertyEditFrameComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.inject(WorkflowActionService);
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  /**
   * test if the property editor correctly receives the operator highlight stream,
   *  get the operator data (id, property, and metadata), and then display the form.
   */
  it("should change the content of property editor from an empty panel correctly", () => {
    // check if the changePropertyEditor called after the operator
    //  is highlighted has correctly updated the variables
    const predicate = {
      ...mockScanPredicate,
      operatorProperties: { tableName: "" },
    };

    // add and highlight an operator
    workflowActionService.addOperator(predicate, mockPoint);

    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, predicate.operatorID, true),
    });
    fixture.detectChanges();
    // check variables are set correctly
    expect(component.formData).toEqual(predicate.operatorProperties);

    // check HTML form are displayed
    const formTitleElement = fixture.debugElement.query(By.css(".texera-workspace-property-editor-title"));
    const jsonSchemaFormElement = fixture.debugElement.query(By.css(".texera-workspace-property-editor-form"));
    // check the panel title
    expect((formTitleElement.nativeElement as HTMLElement).innerText).toEqual(
      mockScanSourceSchema.additionalMetadata.userFriendlyName
    );

    // check if the form has the all the json schema property names
    Object.entries(mockScanSourceSchema.jsonSchema.properties as any).forEach(entry => {
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

  it("should change Texera graph property when the form is edited by the user", fakeAsync(() => {
    // add an operator and highlight the operator so that the
    //  variables in property editor component is set correctly
    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, mockScanPredicate.operatorID, true),
    });
    fixture.detectChanges();
    tick(COLLAB_DEBOUNCE_TIME_MS);

    // stimulate a form change by the user
    const formChangeValue = { tableName: "twitter_sample" };
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

  xit(
    "should debounce the user form input to avoid emitting event too frequently",
    marbles(m => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

      // add an operator and highlight the operator so that the
      //  variables in property editor component is set correctly
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);

      // prepare the form user input event stream
      // simulate user types in `table` character by character
      const formUserInputMarbleString = "-a-b-c-d-e";
      const formUserInputMarbleValue = {
        a: { tableName: "t" },
        b: { tableName: "ta" },
        c: { tableName: "tab" },
        d: { tableName: "tabl" },
        e: { tableName: "table" },
      };
      const formUserInputEventStream = m.hot(formUserInputMarbleString, formUserInputMarbleValue);

      // prepare the expected output stream after debounce time
      const formChangeEventMarbleString =
        // wait for the time of last marble string starting to emit
        "-".repeat(formUserInputMarbleString.length - 1) +
        // then wait for debounce time (each tick represents 10 ms)
        "-".repeat(FORM_DEBOUNCE_TIME_MS / 10) +
        "e-";
      const formChangeEventMarbleValue = {
        e: { tableName: "table" } as object,
      };
      const expectedFormChangeEventStream = m.hot(formChangeEventMarbleString, formChangeEventMarbleValue);

      m.bind();

      // // TODO: FIX THIS
      // const actualFormChangeEventStream = component.operatorPropertyChangeStream;
      // // formUserInputEventStream.subscribe();

      // m.expect(actualFormChangeEventStream).toBeObservable(expectedFormChangeEventStream);
    })
  );

  it("should not emit operator property change event if the new property is the same as the old property", fakeAsync(() => {
    // add an operator and highlight the operator so that the
    //  variables in property editor component is set correctly
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    const mockOperatorProperty = { tableName: "table" };
    // set operator property first before displaying the operator property in property panel
    workflowActionService.setOperatorProperty(mockScanPredicate.operatorID, mockOperatorProperty);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, mockScanPredicate.operatorID, true),
    });
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

  it("should change operator to default values", () => {
    // result operator has default values, use ajv to fill in default values
    // expected form output should fill in all default values instead of an empty object
    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, mockResultPredicate.operatorID, true),
    });
    fixture.detectChanges();
    const ajv = new Ajv({ useDefaults: true });
    const expectedResultOperatorProperties = cloneDeep(mockResultPredicate.operatorProperties);
    ajv.validate(mockViewResultsSchema.jsonSchema, expectedResultOperatorProperties);

    expect(component.formData).toEqual(expectedResultOperatorProperties);
  });

  it("check operator version", () => {
    // check result operator version
    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, mockResultPredicate.operatorID, true),
    });
    fixture.detectChanges();
    expect(component.operatorVersion).toEqual(mockResultPredicate.operatorVersion);

    // check scan opeartor version
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    component.ngOnChanges({
      currentOperatorId: new SimpleChange(undefined, mockScanPredicate.operatorID, true),
    });
    fixture.detectChanges();
    expect(component.operatorVersion).toEqual(mockScanPredicate.operatorVersion);
  });
});

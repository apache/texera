import { HttpClient } from "@angular/common/http";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { fakeAsync, inject, TestBed, tick, flush, discardPeriodicTasks } from "@angular/core/testing";
import { environment } from "../../../../../environments/environment";
import { OperatorMetadataService } from "../../operator-metadata/operator-metadata.service";
import {
  mockPoint,
  mockScanPredicate,
  mockScanSentimentLink,
  mockSentimentPredicate,
} from "../../workflow-graph/model/mock-workflow-data";
import { StubOperatorMetadataService } from "../../operator-metadata/stub-operator-metadata.service";
import { WorkflowActionService } from "../../workflow-graph/model/workflow-action.service";
import { DynamicSchemaService } from "../dynamic-schema.service";
import { AutoAttributeCorrectionService } from "./auto-attribute-correction.service";
import {
  mockSchemaPropagationResponse1,
  mockSchemaPropagationResponse2,
  mockSchemaPropagationResponse3,
  mockSentimentOperatorA,
  mockSentimentOperatorB,
  mockLinkAtoB,
  mockLinkBtoC,
  mockSentimentOperatorC,
  mockSchemaPropagationResponse4,
  mockSchemaPropagationResponse5,
} from "./mock-auto-attribute-correction.data";
import { AppSettings } from "src/app/common/app-setting";
import {
  SchemaPropagationService,
  SCHEMA_PROPAGATION_DEBOUNCE_TIME_MS,
  SCHEMA_PROPAGATION_ENDPOINT,
} from "../schema-propagation/schema-propagation.service";

/* eslint-disable @typescript-eslint/no-non-null-assertion */
describe("AttributeChangePropagationService", () => {
  let httpClient: HttpClient;
  let httpTestingController: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        WorkflowActionService,
        DynamicSchemaService,
        AutoAttributeCorrectionService,
        SchemaPropagationService,
      ],
    });

    httpClient = TestBed.inject(HttpClient);
    httpTestingController = TestBed.inject(HttpTestingController);
    environment.schemaPropagationEnabled = true;
  });

  it("should be created", inject([AutoAttributeCorrectionService], (service: AutoAttributeCorrectionService) => {
    expect(service).toBeTruthy();
  }));

  it("should propagate new attribute name when atteibute is renamed", fakeAsync(() => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const schemaPropagationService: SchemaPropagationService = TestBed.inject(SchemaPropagationService);
    const autoAttributeCorrectionService: AutoAttributeCorrectionService =
      TestBed.inject(AutoAttributeCorrectionService);

    workflowActionService.addOperator(mockSentimentOperatorA, mockPoint);
    workflowActionService.addOperator(mockSentimentOperatorB, mockPoint);
    workflowActionService.addLink(mockLinkAtoB);

    const req1 = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    expect(req1.request.method === "POST");
    expect(req1.request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req1.flush(mockSchemaPropagationResponse1);
    httpTestingController.verify();

    // trigger inputSchemaChangeStream
    workflowActionService.setOperatorProperty(mockSentimentOperatorA.operatorID, { testAttr: "test" });
    tick(SCHEMA_PROPAGATION_DEBOUNCE_TIME_MS);
    const req2 = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    expect(req2.request.method === "POST");
    expect(req2.request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req2.flush(mockSchemaPropagationResponse2);
    httpTestingController.verify();

    expect(
      workflowActionService.getTexeraGraph().getOperator(mockSentimentOperatorB.operatorID).operatorProperties.attribute
    ).toEqual("user_display_name");
    flush();
    discardPeriodicTasks();
  }));

  it("should delete attribute in succeeding operators when attribute is deleted", fakeAsync(() => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const schemaPropagationService: SchemaPropagationService = TestBed.inject(SchemaPropagationService);
    const autoAttributeCorrectionService: AutoAttributeCorrectionService =
      TestBed.inject(AutoAttributeCorrectionService);

    workflowActionService.addOperator(mockSentimentOperatorA, mockPoint);
    workflowActionService.addOperator(mockSentimentOperatorB, mockPoint);
    workflowActionService.addLink(mockLinkAtoB);

    const req1 = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    expect(req1.request.method === "POST");
    expect(req1.request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req1.flush(mockSchemaPropagationResponse1);
    httpTestingController.verify();

    // trigger inputSchemaChangeStream
    workflowActionService.setOperatorProperty(mockSentimentOperatorA.operatorID, { testAttr: "test" });
    tick(SCHEMA_PROPAGATION_DEBOUNCE_TIME_MS + 1);
    const req2 = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    expect(req2.request.method === "POST");
    expect(req2.request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req2.flush(mockSchemaPropagationResponse3);
    httpTestingController.verify();

    expect(
      workflowActionService.getTexeraGraph().getOperator(mockSentimentOperatorB.operatorID).operatorProperties.attribute
    ).toEqual("");

    flush();
    discardPeriodicTasks();
  }));

  it("should propagate new attribute name in three consecutive operators", fakeAsync(() => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const schemaPropagationService: SchemaPropagationService = TestBed.inject(SchemaPropagationService);
    const autoAttributeCorrectionService: AutoAttributeCorrectionService =
      TestBed.inject(AutoAttributeCorrectionService);

    workflowActionService.addOperator(mockSentimentOperatorA, mockPoint);
    workflowActionService.addOperator(mockSentimentOperatorB, mockPoint);
    workflowActionService.addOperator(mockSentimentOperatorC, mockPoint);
    workflowActionService.addLink(mockLinkAtoB);
    httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    workflowActionService.addLink(mockLinkBtoC);

    const req1 = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    expect(req1.request.method === "POST");
    expect(req1.request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req1.flush(mockSchemaPropagationResponse4);
    httpTestingController.verify();

    // trigger inputSchemaChangeStream
    workflowActionService.setOperatorProperty(mockSentimentOperatorA.operatorID, { testAttr: "test" });
    tick(SCHEMA_PROPAGATION_DEBOUNCE_TIME_MS);
    const req2 = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    expect(req2.request.method === "POST");
    expect(req2.request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req2.flush(mockSchemaPropagationResponse5);
    httpTestingController.verify();

    expect(
      workflowActionService.getTexeraGraph().getOperator(mockSentimentOperatorC.operatorID).operatorProperties.attribute
    ).toEqual("screen_display_time");

    flush();
    discardPeriodicTasks();
  }));
});

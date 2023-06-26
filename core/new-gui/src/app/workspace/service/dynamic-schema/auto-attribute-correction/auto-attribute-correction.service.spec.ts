import { HttpClient } from "@angular/common/http";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { discardPeriodicTasks, fakeAsync, flush, inject, TestBed, tick } from "@angular/core/testing";
import { environment } from "../../../../../environments/environment";
import { OperatorMetadataService } from "../../operator-metadata/operator-metadata.service";
import { mockPoint } from "../../workflow-graph/model/mock-workflow-data";
import { StubOperatorMetadataService } from "../../operator-metadata/stub-operator-metadata.service";
import { WorkflowActionService } from "../../workflow-graph/model/workflow-action.service";
import { DynamicSchemaService } from "../dynamic-schema.service";
import { AutoAttributeCorrectionService } from "./auto-attribute-correction.service";
import {
  mockLinkAtoB,
  mockLinkBtoC,
  mockSchemaPropagationResponse1,
  mockSchemaPropagationResponse2,
  mockSchemaPropagationResponse3,
  mockSchemaPropagationResponse4,
  mockSchemaPropagationResponse5,
  mockSentimentOperatorA,
  mockSentimentOperatorB,
  mockSentimentOperatorC,
} from "./mock-auto-attribute-correction.data";
import { AppSettings } from "src/app/common/app-setting";
import {
  SCHEMA_PROPAGATION_DEBOUNCE_TIME_MS,
  SCHEMA_PROPAGATION_ENDPOINT,
  SchemaPropagationService,
} from "../schema-propagation/schema-propagation.service";

describe("AutoAttributeCorrectionService", () => {
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
    TestBed.inject(SchemaPropagationService);
    TestBed.inject(AutoAttributeCorrectionService);
    workflowActionService.addOperator(mockSentimentOperatorA, mockPoint);
    workflowActionService.addOperator(mockSentimentOperatorB, mockPoint);
    workflowActionService.addLink(mockLinkAtoB);

    const req1 = httpTestingController.expectOne(
      `${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}/undefined`
    );
    expect(req1.request.method === "POST");
    expect(req1.request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}/undefined`);
    req1.flush(mockSchemaPropagationResponse1);
    httpTestingController.verify();

    // trigger inputSchemaChangeStream
    workflowActionService.setOperatorProperty(mockSentimentOperatorA.operatorID, { testAttr: "test" });
    tick(SCHEMA_PROPAGATION_DEBOUNCE_TIME_MS);
    const req2 = httpTestingController.expectOne(
      `${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}/undefined`
    );
    expect(req2.request.method === "POST");
    expect(req2.request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}/undefined`);
    req2.flush(mockSchemaPropagationResponse2);
    httpTestingController.verify();
    flush();
    expect(
      workflowActionService.getTexeraGraph().getOperator(mockSentimentOperatorB.operatorID).operatorProperties.attribute
    ).toEqual("user_display_name");
    flush();
    discardPeriodicTasks();
  }));

  it("should delete attribute in succeeding operators when attribute is deleted", fakeAsync(() => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    TestBed.inject(SchemaPropagationService);
    TestBed.inject(AutoAttributeCorrectionService);
    workflowActionService.addOperator(mockSentimentOperatorA, mockPoint);
    workflowActionService.addOperator(mockSentimentOperatorB, mockPoint);
    workflowActionService.addLink(mockLinkAtoB);

    const req1 = httpTestingController.expectOne(
      `${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}/undefined`
    );
    expect(req1.request.method === "POST");
    expect(req1.request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}/undefined`);
    req1.flush(mockSchemaPropagationResponse1);
    httpTestingController.verify();

    // trigger inputSchemaChangeStream
    workflowActionService.setOperatorProperty(mockSentimentOperatorA.operatorID, { testAttr: "test" });
    tick(SCHEMA_PROPAGATION_DEBOUNCE_TIME_MS + 1);
    const req2 = httpTestingController.expectOne(
      `${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}/undefined`
    );
    expect(req2.request.method === "POST");
    expect(req2.request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}/undefined`);
    req2.flush(mockSchemaPropagationResponse3);
    httpTestingController.verify();
    flush();
    expect(
      workflowActionService.getTexeraGraph().getOperator(mockSentimentOperatorB.operatorID).operatorProperties.attribute
    ).toEqual("");

    flush();
    discardPeriodicTasks();
  }));

  it("should propagate new attribute name in three consecutive operators", fakeAsync(() => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    TestBed.inject(SchemaPropagationService);
    TestBed.inject(AutoAttributeCorrectionService);
    workflowActionService.addOperator(mockSentimentOperatorA, mockPoint);
    workflowActionService.addOperator(mockSentimentOperatorB, mockPoint);
    workflowActionService.addOperator(mockSentimentOperatorC, mockPoint);
    workflowActionService.addLink(mockLinkAtoB);
    httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}/undefined`);
    workflowActionService.addLink(mockLinkBtoC);

    const req1 = httpTestingController.expectOne(
      `${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}/undefined`
    );
    expect(req1.request.method === "POST");
    expect(req1.request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}/undefined`);
    req1.flush(mockSchemaPropagationResponse4);
    httpTestingController.verify();

    // trigger inputSchemaChangeStream
    workflowActionService.setOperatorProperty(mockSentimentOperatorA.operatorID, { testAttr: "test" });
    tick(SCHEMA_PROPAGATION_DEBOUNCE_TIME_MS);
    const req2 = httpTestingController.expectOne(
      `${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}/undefined`
    );
    expect(req2.request.method === "POST");
    expect(req2.request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}/undefined`);
    req2.flush(mockSchemaPropagationResponse5);
    httpTestingController.verify();
    flush();
    expect(
      workflowActionService.getTexeraGraph().getOperator(mockSentimentOperatorC.operatorID).operatorProperties.attribute
    ).toEqual("screen_display_time");

    flush();
    discardPeriodicTasks();
  }));
});

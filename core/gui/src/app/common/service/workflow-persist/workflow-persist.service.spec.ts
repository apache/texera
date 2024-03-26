import { TestBed } from "@angular/core/testing";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { WorkflowPersistService } from "./workflow-persist.service";
import { jsonCast } from "../../util/storage";
import { WorkflowContent } from "../../type/workflow";
import { last } from "rxjs/operators";

describe("WorkflowPersistService", () => {
  let service: WorkflowPersistService;
  let httpTestingController: HttpTestingController;
  const testContent =
    "{\"operators\":[{\"operatorID\":\"Limit-operator-a11370eb-940a-4f10-8b36-8b413b2396c9\"," +
    "\"operatorType\":\"Limit\",\"operatorProperties\":{\"limit\":2},\"inputPorts\":[{\"portID\":\"input-0\",\"displayName\":\"\"}]," +
    "\"outputPorts\":[{\"portID\":\"output-0\",\"displayName\":null}],\"showAdvanced\":false}," +
    "{\"operatorID\":\"SimpleSink-operator-e4a77a32-e3c9-4c40-a26d-a1aa103cc914\",\"operatorType\":\"SimpleSink\"," +
    "\"operatorProperties\":{},\"inputPorts\":[{\"portID\":\"input-0\",\"displayName\":\"\"}],\"outputPorts\":[]," +
    "\"showAdvanced\":false},{\"operatorID\":\"MySQLSource-operator-1ee619b1-8884-4564-a136-29ef77dfcc50\"," +
    "\"operatorType\":\"MySQLSource\",\"operatorProperties\":{\"port\":\"default\",\"search\":false,\"progressive\":false," +
    "\"min\":\"auto\",\"max\":\"auto\",\"interval\":1000000000,\"host\":\"localhost\"},\"inputPorts\":[]," +
    "\"outputPorts\":[{\"portID\":\"output-0\",\"displayName\":\"\"}],\"showAdvanced\":false}]," +
    "\"operatorPositions\":{\"Limit-operator-a11370eb-940a-4f10-8b36-8b413b2396c9\":{\"x\":200,\"y\":212}," +
    "\"SimpleSink-operator-e4a77a32-e3c9-4c40-a26d-a1aa103cc914\":{\"x\":392,\"y\":218}," +
    "\"MySQLSource-operator-1ee619b1-8884-4564-a136-29ef77dfcc50\":{\"x\":36,\"y\":214}}," +
    "\"links\":[{\"linkID\":\"link-ea977a06-3ef5-4c80-b31a-4013cfb8321d\"," +
    "\"source\":{\"operatorID\":\"Limit-operator-a11370eb-940a-4f10-8b36-8b413b2396c9\",\"portID\":\"output-0\"}," +
    "\"target\":{\"operatorID\":\"SimpleSink-operator-e4a77a32-e3c9-4c40-a26d-a1aa103cc914\",\"portID\":\"input-0\"}}," +
    "{\"linkID\":\"link-c94e24a6-2c77-40cf-ba22-1a7ffba64b7d\",\"source\":{\"operatorID\":" +
    "\"MySQLSource-operator-1ee619b1-8884-4564-a136-29ef77dfcc50\",\"portID\":\"output-0\"},\"target\":" +
    "{\"operatorID\":\"Limit-operator-a11370eb-940a-4f10-8b36-8b413b2396c9\",\"portID\":\"input-0\"}}]," +
    "\"groups\":[],\"breakpoints\":{}}";
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
    });
    service = TestBed.inject(WorkflowPersistService);
    httpTestingController = TestBed.get(HttpTestingController);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("should send http post request once", () => {
    service
      .createWorkflow(jsonCast<WorkflowContent>(testContent), "testname")
      .pipe(last())
      .subscribe(value => {
        expect(value).toBeTruthy();
      });
    httpTestingController.expectOne(request => request.method === "POST");
  });

  it("should check if workflow content and name returned correctly", () => {
    service
      .createWorkflow(jsonCast<WorkflowContent>(testContent), "testname")
      .pipe(last())
      .subscribe(value => {
        expect(value.workflow.name).toEqual("testname_copy");
        expect(value.workflow.content).toEqual(jsonCast<WorkflowContent>(testContent));
      });
  });
});

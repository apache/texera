import { TestBed } from "@angular/core/testing";
import { WorkflowWebsocketService } from "./workflow-websocket.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { Observable, of } from "rxjs";
import { WorkflowMetadata } from "src/app/dashboard/type/workflow-metadata.interface";

class StubWorkflowActionService {
  constructor() {}
  public getWorkflowMetadata(): WorkflowMetadata {
    return { name: "", wid: 1, creationTime: undefined, lastModifiedTime: undefined };
  }
  public workflowMetaDataChanged(): Observable<void> {
    return of();
  }
}

describe("WorkflowWebsocketService", () => {
  let service: WorkflowWebsocketService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        {
          provide: WorkflowActionService,
          useClass: StubWorkflowActionService,
        },
        WorkflowWebsocketService,
      ],
    });
    service = TestBed.inject(WorkflowWebsocketService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});

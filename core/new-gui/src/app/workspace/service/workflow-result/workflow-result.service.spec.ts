import { TestBed } from "@angular/core/testing";

import { WorkflowResultService } from "./workflow-result.service";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { UndoRedoService } from "../undo-redo/undo-redo.service";
import { JointUIService } from "../joint-ui/joint-ui.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";

describe("WorkflowResultService", () => {
  let service: WorkflowResultService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        WorkflowActionService,
        WorkflowUtilService,
        UndoRedoService,
        JointUIService,
        WorkflowWebsocketService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
      ],
    });
    service = TestBed.inject(WorkflowResultService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});

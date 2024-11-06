import { TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { WorkflowVersionService } from "./workflow-version.service";
import { WorkflowActionService } from "../../../../workspace/service/workflow-graph/model/workflow-action.service";
import { OperatorMetadataService } from "../../../../workspace/service/operator-metadata/operator-metadata.service";


describe("WorkflowVersionService", () => {
  let service: WorkflowVersionService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [WorkflowVersionService, WorkflowActionService, OperatorMetadataService],
    });
    service = TestBed.inject(WorkflowVersionService);
  });

  describe("canRestoreVersion", () => {
    it("should return true when modificationEnabledBeforeTempWorkflow is true", () => {
      // Arrange
      service["modificationEnabledBeforeTempWorkflow"] = true;

      // Act
      const result = service.canRestoreVersion;

      // Assert
      expect(result).toBe(true);
    });

    it("should return false when modificationEnabledBeforeTempWorkflow is undefined", () => {
      // Arrange
      service["modificationEnabledBeforeTempWorkflow"] = undefined;

      // Act
      const result = service.canRestoreVersion;

      // Assert
      expect(result).toBe(false);
    });
  });
});

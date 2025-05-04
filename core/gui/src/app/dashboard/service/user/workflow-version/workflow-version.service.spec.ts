/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { WorkflowVersionService } from "./workflow-version.service";
import { WorkflowActionService } from "src/app/workspace/service/workflow-graph/model/workflow-action.service";
import { OperatorMetadataService } from "src/app/workspace/service/operator-metadata/operator-metadata.service";

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

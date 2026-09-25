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
import * as joint from "jointjs";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { OverboxService } from "./overbox.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
import { mockScanPredicate, mockPoint } from "../workflow-graph/model/mock-workflow-data";

describe("OverboxService", () => {
  beforeEach(() =>
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [...commonTestProviders, { provide: OperatorMetadataService, useClass: StubOperatorMetadataService }],
    })
  );
  it("stores named colored frames in workflow annotations and supports editing", () => {
    const actions = TestBed.inject(WorkflowActionService);
    const service = TestBed.inject(OverboxService);
    actions.addOperator(mockScanPredicate, mockPoint);
    const id = service.create("Limpieza ñ", "#1677ff", [mockScanPredicate.operatorID]);
    const box = actions.getTexeraGraph().getCommentBox(id);
    expect(box.overbox?.name).toBe("Limpieza ñ");
    expect(box.overbox?.width).toBeGreaterThan(0);
    expect(actions.getJointGraph().getCell(id).get("z")).toBe(-1);
    service.update(id, "Transform", "#ff0000");
    expect(actions.getTexeraGraph().getCommentBox(id).overbox?.color).toBe("#ff0000");
    expect(actions.getJointGraph().getCell(id).attr("label/text")).toBe("Transform");
    service.resize(id, 640.4, 360.6);
    expect(actions.getTexeraGraph().getCommentBox(id).overbox).toEqual({
      name: "Transform",
      color: "#ff0000",
      width: 640,
      height: 361,
    });
    expect((actions.getJointGraph().getCell(id) as joint.dia.Element).size()).toEqual({ width: 640, height: 361 });
    actions.highlightCommentBoxes(false, id);
    actions
      .getJointGraphWrapper()
      .setElementPosition(id, 510 - box.commentBoxPosition.x, 320 - box.commentBoxPosition.y);
    const saved = JSON.parse(JSON.stringify(actions.getWorkflowContent())).commentBoxes[0];
    expect(saved.commentBoxPosition).toEqual({ x: 510, y: 320 });
    actions.deleteCommentBox(id);
    actions.addCommentBox(saved);
    expect(actions.getJointGraph().getCell(id).attr("label/text")).toBe("Transform");
    expect(actions.getTexeraGraph().getCommentBox(id).overbox).toEqual(saved.overbox);
  });

  it("rejects blank names, invalid colors and nonmodifiable workflows", () => {
    const service = TestBed.inject(OverboxService);
    expect(() => service.create(" ", "#1677ff", [])).toThrow();
    expect(() => service.create("Name", "invalid", [])).toThrow();
    expect(() => service.resize("missing", Number.NaN, 100)).toThrow("Section dimensions must be finite.");
    const actions = TestBed.inject(WorkflowActionService);
    actions.disableWorkflowModification();
    expect(() => service.create("Name", "#1677ff", [])).toThrow("Workflow is read-only.");
  });
});

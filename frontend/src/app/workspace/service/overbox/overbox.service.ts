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

import { Injectable } from "@angular/core";
import { YType } from "../../types/shared-editing.interface";
import { CommentBox } from "../../types/workflow-common.interface";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { MIN_OVERBOX_HEIGHT, MIN_OVERBOX_WIDTH } from "./overbox-resize-tool";

@Injectable({ providedIn: "root" })
export class OverboxService {
  constructor(
    private actions: WorkflowActionService,
    private util: WorkflowUtilService
  ) {}
  private validate(name: string, color: string): void {
    if (!this.actions.checkWorkflowModificationEnabled()) throw new Error("Workflow is read-only.");
    if (!name.trim() || name.trim().length > 100) throw new Error("Use a name between 1 and 100 characters.");
    if (!/^#[0-9a-f]{6}$/i.test(color)) throw new Error("Choose a valid color.");
  }

  private getStyle(id: string): YType<NonNullable<CommentBox["overbox"]>> {
    const graph = this.actions.getTexeraGraph();
    if (!graph.getCommentBox(id).overbox) throw new Error("Section no longer exists.");
    return graph.getSharedCommentBoxType(id).get("overbox") as unknown as YType<NonNullable<CommentBox["overbox"]>>;
  }
  public create(name: string, color: string, operatorIDs: readonly string[]): string {
    this.validate(name, color);
    const bounds = operatorIDs.map(id => {
      this.actions.getTexeraGraph().assertOperatorExists(id);
      return this.actions.getJointGraph().getCell(id).getBBox();
    });
    const paper = this.actions.getJointGraphWrapper().getMainJointPaper();
    const origin = paper
      ? paper.clientToLocalPoint({
          x: paper.el.getBoundingClientRect().left + 350,
          y: paper.el.getBoundingClientRect().top + 150,
        })
      : { x: 350, y: 150 };
    const x = bounds.length ? Math.min(...bounds.map(b => b.x)) - 24 : origin.x;
    const y = bounds.length ? Math.min(...bounds.map(b => b.y)) - 32 : origin.y;
    const width = bounds.length ? Math.max(...bounds.map(b => b.x + b.width)) - x + 24 : 400;
    const height = bounds.length ? Math.max(...bounds.map(b => b.y + b.height)) - y + 24 : 240;
    const commentBoxID = this.util.getCommentBoxRandomUUID();
    this.actions.addCommentBox({
      commentBoxID,
      comments: [],
      commentBoxPosition: { x, y },
      overbox: { name: name.trim(), color, width, height },
    });
    return commentBoxID;
  }
  public update(id: string, name: string, color: string): void {
    this.validate(name, color);
    const graph = this.actions.getTexeraGraph();
    graph.bundleActions(() => {
      const style = this.getStyle(id);
      const title = style.get("name");
      title.delete(0, title.length);
      title.insert(0, name.trim());
      const border = style.get("color");
      border.delete(0, border.length);
      border.insert(0, color);
    });
  }

  public resize(id: string, width: number, height: number): void {
    if (!this.actions.checkWorkflowModificationEnabled()) throw new Error("Workflow is read-only.");
    if (!Number.isFinite(width) || !Number.isFinite(height)) throw new Error("Section dimensions must be finite.");
    const graph = this.actions.getTexeraGraph();
    const style = this.getStyle(id);
    graph.bundleActions(() => {
      style.set("width", Math.max(MIN_OVERBOX_WIDTH, Math.round(width)));
      style.set("height", Math.max(MIN_OVERBOX_HEIGHT, Math.round(height)));
    });
  }
}

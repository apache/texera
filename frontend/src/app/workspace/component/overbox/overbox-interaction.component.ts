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

import { AfterViewInit, Component, Input, OnDestroy, inject } from "@angular/core";
import * as joint from "jointjs";
import { createOverboxResizeTool } from "../../service/overbox/overbox-resize-tool";
import { OverboxService } from "../../service/overbox/overbox.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";

/** Owns section-only pointer behavior so the generic workflow editor remains unaware of its geometry. */
@Component({ selector: "texera-overbox-interaction", template: "" })
export class OverboxInteractionComponent implements AfterViewInit, OnDestroy {
  @Input({ required: true }) paper!: joint.dia.Paper;
  @Input() disabled = false;
  private readonly actions = inject(WorkflowActionService);
  private readonly overboxes = inject(OverboxService);

  ngAfterViewInit(): void {
    this.paper.on("element:pointerdown", this.elementPointerDown);
    this.paper.on("blank:pointerdown", this.blankPointerDown);
  }

  ngOnDestroy(): void {
    this.paper.off("element:pointerdown", this.elementPointerDown);
    this.paper.off("blank:pointerdown", this.blankPointerDown);
  }

  private elementPointerDown = (view: joint.dia.ElementView): void => {
    const id = view.model.id.toString();
    if (!this.disabled && this.isOverbox(id)) this.showTools(view, id);
  };

  private blankPointerDown = (): void => this.removeTools();

  private showTools(view: joint.dia.ElementView, id: string): void {
    this.removeTools();
    view.addTools(
      new joint.dia.ToolsView({
        name: "overbox-tools",
        tools: [
          createOverboxResizeTool((width, height) => {
            if (this.canModify()) this.overboxes.resize(id, width, height);
          }),
        ],
      })
    );
  }

  private isOverbox(id: string): boolean {
    const graph = this.actions.getTexeraGraph();
    return graph.hasCommentBox(id) && graph.getCommentBox(id).overbox !== undefined;
  }

  private canModify(): boolean {
    return !this.disabled && this.actions.checkWorkflowModificationEnabled();
  }

  private removeTools(): void {
    const graph = this.actions.getTexeraGraph();
    graph
      .getAllCommentBoxes()
      .filter(box => box.overbox)
      .forEach(box => this.paper.findViewByModel(box.commentBoxID)?.removeTools());
  }
}

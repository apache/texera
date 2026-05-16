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

import { Component, Input } from "@angular/core";
import { CdkDrag, CdkDragPreview, CdkDropList } from "@angular/cdk/drag-drop";
import { CommonModule } from "@angular/common";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { SnippetCanvasService } from "../../../service/snippet/snippet-canvas.service";
import { WorkflowSnippet } from "../../../../dashboard/type/workflow-snippet.interface";

@UntilDestroy()
@Component({
  selector: "texera-snippet-label",
  templateUrl: "./snippet-label.component.html",
  styleUrls: ["./snippet-label.component.scss"],
  imports: [CommonModule, CdkDropList, CdkDrag, CdkDragPreview],
})
export class SnippetLabelComponent {
  @Input() snippet!: WorkflowSnippet;

  public draggable = true;

  constructor(
    private workflowActionService: WorkflowActionService,
    private snippetCanvasService: SnippetCanvasService
  ) {
    this.workflowActionService
      .getWorkflowModificationEnabledStream()
      .pipe(untilDestroyed(this))
      .subscribe(canModify => (this.draggable = canModify));
  }

  private lastPlacementToken: string | null = null;

  public onDragStarted(): void {
    // eslint-disable-next-line no-console
    console.debug("[snippet] drag started", this.snippet.name);
  }

  public onDropped(dropPoint: { x: number; y: number }): void {
    // eslint-disable-next-line no-console
    console.debug("[snippet] cdkDragDropped fired", { snippet: this.snippet.name, dropPoint });
    this.place(dropPoint, "dropped");
  }

  public onDragEnded(dropPoint: { x: number; y: number }): void {
    // eslint-disable-next-line no-console
    console.debug("[snippet] cdkDragEnded fired", { snippet: this.snippet.name, dropPoint });
    this.place(dropPoint, "ended");
  }

  // cdkDragDropped and cdkDragEnded can both fire for the same drag; the token
  // dedupes so we only place the snippet once per drag gesture.
  private place(dropPoint: { x: number; y: number } | undefined, source: string): void {
    if (!this.draggable || !dropPoint) return;
    const token = `${dropPoint.x},${dropPoint.y}`;
    if (this.lastPlacementToken === token) {
      // eslint-disable-next-line no-console
      console.debug("[snippet] skipping duplicate placement from", source);
      return;
    }
    this.lastPlacementToken = token;
    setTimeout(() => (this.lastPlacementToken = null), 250);
    const result = this.snippetCanvasService.placeSnippet(this.snippet, dropPoint);
    // eslint-disable-next-line no-console
    console.debug("[snippet] placeSnippet returned", { source, result });
  }

  public get tooltip(): string {
    const opCount = this.snippet.operators.length;
    const linkCount = this.snippet.links.length;
    const desc = this.snippet.description ? `${this.snippet.description}\n` : "";
    return `${desc}${opCount} operator(s), ${linkCount} link(s)`;
  }
}

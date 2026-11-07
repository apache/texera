/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import * as joint from "jointjs";
import { of } from "rxjs";
import { JointUIService as JointUI } from "../../app/workspace/service/joint-ui/joint-ui";
import type { OperatorMetadata } from "../../app/workspace/types/operator-schema.interface";
import type { OperatorLink } from "../../app/workspace/types/workflow-common.interface";
import { WorkflowSession } from "./workflow-session";

/** Synchronizes the original Texera JointJS cells with the original workflow graph. */
export class CanvasModel {
  readonly graph = new joint.dia.Graph();
  private readonly ui: JointUI;
  private rendering = false;
  private disposed = false;
  private readonly unsubscribe: () => void;

  constructor(
    readonly session: WorkflowSession,
    metadata: OperatorMetadata,
    private readonly onError: (error: Error) => void
  ) {
    this.ui = new JointUI({ getOperatorMetadata: () => of(metadata) });
    this.render();
    this.unsubscribe = session.subscribe(() => this.render());
    this.graph.on("change:position", this.onPosition);
    this.graph.on("add change:source change:target", this.onLink);
    this.graph.on("remove", this.onRemove);
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.unsubscribe();
    this.graph.off("change:position", this.onPosition);
    this.graph.off("add change:source change:target", this.onLink);
    this.graph.off("remove", this.onRemove);
  }

  private readonly onPosition = (cell: joint.dia.Element) => {
    if (this.rendering || this.disposed || !this.session.graph.hasOperator(String(cell.id))) return;
    this.perform(() => this.session.moveOperator(String(cell.id), cell.position()));
  };
  private readonly onLink = (cell: joint.dia.Cell) => {
    if (this.rendering || this.disposed || !cell.isLink()) return;
    const link = cell as joint.dia.Link;
    const source = link.source();
    const target = link.target();
    if (!source.id || !source.port || !target.id || !target.port) {
      if (this.session.graph.hasLinkWithID(String(link.id)))
        this.perform(() => this.session.deleteLink(String(link.id)));
      return;
    }
    const logical: OperatorLink = {
      linkID: String(link.id),
      source: { operatorID: String(source.id), portID: source.port },
      target: { operatorID: String(target.id), portID: target.port },
    };
    if (
      this.session.graph.hasLinkWithID(logical.linkID) &&
      JSON.stringify(this.session.graph.getLinkWithID(logical.linkID)) === JSON.stringify(logical)
    )
      return;
    this.perform(() => {
      this.session.graph.assertLinkIsValid(logical);
      if (this.session.graph.hasLinkWithID(logical.linkID)) this.session.deleteLink(logical.linkID);
      this.session.addLink(logical);
    });
  };
  private readonly onRemove = (cell: joint.dia.Cell) => {
    if (this.rendering || this.disposed) return;
    const id = String(cell.id);
    if (cell.isLink() && this.session.graph.hasLinkWithID(id)) this.perform(() => this.session.deleteLink(id));
    if (cell.isElement() && this.session.graph.hasOperator(id)) this.perform(() => this.session.deleteOperators([id]));
  };

  private perform(action: () => void): void {
    try {
      action();
    } catch (error) {
      this.onError(error instanceof Error ? error : new Error("The graph edit was rejected."));
      this.render();
    }
  }
  private render(): void {
    if (this.disposed) return;
    this.rendering = true;
    try {
      const { content } = this.session.getSnapshot().workflow;
      const ids = new Set([
        ...content.operators.map(operator => operator.operatorID),
        ...content.links.map(link => link.linkID),
        ...content.commentBoxes.map(box => box.commentBoxID),
      ]);
      for (const cell of this.graph.getCells()) if (!ids.has(String(cell.id))) cell.remove();
      for (const operator of content.operators) {
        const position = content.operatorPositions[operator.operatorID];
        let cell = this.graph.getCell(operator.operatorID) as joint.dia.Element | undefined;
        if (!cell) {
          cell = this.ui.getJointOperatorElement(operator, position);
          this.graph.addCell(cell);
        } else {
          if (JSON.stringify(cell.position()) !== JSON.stringify(position)) cell.position(position.x, position.y);
          const rendered = this.ui.getJointOperatorElement(operator, position);
          cell.attr(rendered.attr());
          if (JSON.stringify(cell.get("ports")) !== JSON.stringify(rendered.get("ports")))
            cell.set("ports", rendered.get("ports"));
        }
      }
      for (const box of content.commentBoxes)
        if (!this.graph.getCell(box.commentBoxID)) this.graph.addCell(this.ui.getCommentElement(box));
      for (const link of content.links) {
        const current = this.graph.getCell(link.linkID) as joint.dia.Link | undefined;
        if (!current) this.graph.addCell(JointUI.getJointLinkCell(link));
        else {
          current.source({ id: link.source.operatorID, port: link.source.portID });
          current.target({ id: link.target.operatorID, port: link.target.portID });
        }
      }
    } finally {
      this.rendering = false;
    }
  }
}

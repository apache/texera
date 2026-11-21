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
import { Component, Input, OnChanges } from "@angular/core";
import { NgFor, NgIf } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { RouterLink } from "@angular/router";
import { macroCanvasUrl } from "../../../../app-routing.constant";
import { NzSelectModule } from "ng-zorro-antd/select";
import { NzButtonModule } from "ng-zorro-antd/button";
import { MacroService } from "../../../service/macro/macro.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { MacroDefinition, macroRevisionLabel, macroType } from "../../../../common/util/workflow-macro";
import type { WorkflowMetadata } from "../../../../dashboard/type/workflow-metadata.interface";
import { NotificationService } from "../../../../common/service/notification/notification.service";

@Component({
  selector: "texera-macro-version",
  imports: [NgIf, NgFor, FormsModule, NzSelectModule, NzButtonModule, RouterLink],
  template: ` <section
    *ngIf="definition as d"
    aria-label="Macro revision"
    class="macro-version">
    <label>Revision</label>
    <nz-select
      *ngIf="versions.length; else captured"
      aria-label="Macro revision to use"
      [ngModel]="d.version"
      (ngModelChange)="use($event)"
      [nzDisabled]="disabled || busy"
      [nzLoading]="busy">
      <nz-option
        *ngFor="let version of versions"
        [nzValue]="version.id"
        [nzLabel]="version.label"></nz-option>
    </nz-select>
    <ng-template #captured
      ><span>{{ label(d) }}</span></ng-template
    >
    <a
      *ngIf="source as s"
      [routerLink]="sourceURL(s.wid!)"
      >Definition: {{ s.name }}</a
    >
    <p *ngIf="unavailable">Source unavailable. The saved snapshot still runs and exports with this workflow.</p>
    <p *ngIf="latest && latest !== d.version">A newer revision is available.</p>
    <div>
      <button
        nz-button
        nzSize="small"
        (click)="macros.inspect$.next(operatorID!)"
        [disabled]="busy">
        Open macro
      </button>
      <button
        *ngIf="latest && latest !== d.version"
        nz-button
        nzSize="small"
        (click)="use()"
        [disabled]="disabled || busy">
        Use latest
      </button>
    </div>
    <button
      *ngIf="unavailableValues.length"
      nz-button
      nzSize="small"
      (click)="removeUnavailableValues()"
      [disabled]="disabled || busy">
      Remove unavailable values
    </button>
  </section>`,
  styles: [
    `
      .macro-version {
        display: grid;
        gap: 8px;
        margin-bottom: 16px;
        padding-bottom: 12px;
        border-bottom: 1px solid #f0f0f0;
      }
      label {
        font-weight: 500;
      }
      p {
        color: #595959;
        margin: 0;
        font-size: 12px;
      }
      button + button {
        margin-left: 8px;
      }
    `,
  ],
})
export class MacroVersionComponent implements OnChanges {
  @Input() operatorID?: string;
  @Input() disabled = false;
  public versions: { id: number; label: string }[] = [];
  public latest?: number;
  public busy = false;
  public unavailable = false;
  public label = macroRevisionLabel;
  public sourceURL = macroCanvasUrl;
  public source?: WorkflowMetadata;
  private request = 0;
  public get definition(): MacroDefinition | undefined {
    const graph = this.actions.getTexeraGraph();
    const op = this.operatorID && graph.hasOperator(this.operatorID) ? graph.getOperator(this.operatorID) : undefined;
    return this.actions.getMacroDependencies().find(d => macroType(d) === op?.operatorType);
  }

  public get unavailableValues(): string[] {
    const definition = this.definition;
    if (!definition || !this.operatorID) return [];
    return Object.keys(this.actions.getTexeraGraph().getOperator(this.operatorID).operatorProperties).filter(
      key => !definition.content.formBinding?.fields.some(field => field.id === key)
    );
  }

  public removeUnavailableValues(): void {
    if (this.disabled || this.busy || !this.operatorID) return;
    const unavailable = new Set(this.unavailableValues);
    if (!unavailable.size) return;
    const properties = this.actions.getTexeraGraph().getOperator(this.operatorID).operatorProperties;
    this.actions.setOperatorProperty(
      this.operatorID,
      Object.fromEntries(Object.entries(properties).filter(([key]) => !unavailable.has(key)))
    );
  }
  constructor(
    public macros: MacroService,
    private actions: WorkflowActionService,
    private notification: NotificationService
  ) {}

  public async ngOnChanges(): Promise<void> {
    const request = ++this.request;
    const d = this.definition;
    this.versions = [];
    this.latest = undefined;
    this.source = undefined;
    this.unavailable = false;
    this.busy = !!d;
    if (!d) return;
    try {
      const { metadata, revisions } = await this.macros.sourceFor(d);
      if (request !== this.request) return;
      this.source = metadata;
      this.versions = revisions.map((id, i) => ({ id, label: `Revision ${i + 1}` }));
      this.latest = this.versions.at(-1)?.id;
    } catch {
      if (request === this.request) this.unavailable = true;
    } finally {
      if (request === this.request) this.busy = false;
    }
  }

  public async use(version?: number): Promise<void> {
    if (
      this.disabled ||
      this.busy ||
      !this.source ||
      !this.operatorID ||
      (version !== undefined && !this.versions.some(v => v.id === version))
    )
      return;
    this.busy = true;
    const before = JSON.stringify(this.actions.getWorkflow());
    const operatorID = this.operatorID;
    try {
      const next = await this.macros.pin(this.definition, version);
      if (this.operatorID !== operatorID || JSON.stringify(this.actions.getWorkflow()) !== before)
        throw new Error("The workflow changed. Select the revision again.");
      await this.macros.adoptVersion(operatorID, next);
      await this.ngOnChanges();
    } catch (error) {
      this.notification.error(error instanceof Error ? error.message : String(error));
    } finally {
      this.busy = false;
    }
  }
}

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
import { Component } from "@angular/core";
import { NgFor, NgIf } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { NzModalModule, NzModalService } from "ng-zorro-antd/modal";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { v4 as uuid } from "uuid";
import { MacroService } from "../../../service/macro/macro.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { OperatorLabelComponent } from "./operator-label/operator-label.component";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { isMacro, MacroPort, macroType } from "../../../../common/util/workflow-macro";
import { MacroInspectorComponent } from "./macro-inspector.component";

@UntilDestroy()
@Component({
  selector: "texera-macro-panel",
  imports: [NgIf, NgFor, FormsModule, NzModalModule, OperatorLabelComponent],
  template: ` <details (toggle)="opened($event)">
      <summary>Macros</summary>
      <button
        type="button"
        (click)="refresh()"
        [disabled]="busy">
        Refresh library
      </button>
      <div *ngFor="let definition of macros.library$.value">
        <texera-operator-label [operator]="metadata.getOperatorSchema(type(definition))"></texera-operator-label>
      </div>
      <input
        aria-label="Macro name"
        placeholder="Macro name"
        [(ngModel)]="name"
        [disabled]="!canModify" />
      <button
        type="button"
        data-action="create"
        (click)="create()"
        [disabled]="!canModify || busy || !name.trim() || !selected.length">
        Create from selection
      </button>
      <ng-container *ngIf="selectedMacro">
        <button
          type="button"
          (click)="inspect(selected[0])">
          Inspect pinned version
        </button>
        <button
          type="button"
          (click)="update()"
          [disabled]="!canModify || busy">
          Adopt latest compatible version
        </button>
      </ng-container>
    </details>
    <section *ngIf="actions.getMacroInterface() as boundary">
      <strong>Macro interface</strong>
      <p>Add this definition to a workflow to run it.</p>
      <div *ngFor="let direction of directions">
        <strong>{{ direction === "inputs" ? "Inputs" : "Outputs" }}</strong>
        <div *ngFor="let port of boundary[direction]">
          <input
            [attr.aria-label]="'Port name ' + port.portID"
            [ngModel]="port.displayName || port.portID"
            (ngModelChange)="rename(direction, port, $event)"
            [disabled]="!canModify" />
          <span *ngFor="let target of port.targets">{{ targetLabel(direction, target) }}</span>
          <button
            type="button"
            (click)="remove(direction, port)"
            [disabled]="!canModify">
            Remove
          </button>
          <button
            *ngIf="direction === 'inputs'"
            type="button"
            (click)="addTarget(port)"
            [disabled]="!canModify || !inputTarget">
            Add input target
          </button>
        </div>
      </div>
      <select
        aria-label="Input target"
        [(ngModel)]="inputTarget"
        [disabled]="!canModify">
        <option value="">Choose an input</option>
        <option
          *ngFor="let p of targets('inputs')"
          [value]="p.value">
          {{ p.label }}
        </option>
      </select>
      <button
        type="button"
        data-action="add-input"
        (click)="add('inputs', inputTarget)"
        [disabled]="!canModify || !inputTarget">
        Add input
      </button>
      <select
        aria-label="Output target"
        [(ngModel)]="outputTarget"
        [disabled]="!canModify">
        <option value="">Choose an output</option>
        <option
          *ngFor="let p of targets('outputs')"
          [value]="p.value">
          {{ p.label }}
        </option>
      </select>
      <button
        type="button"
        (click)="add('outputs', outputTarget)"
        [disabled]="!canModify || !outputTarget">
        Add output
      </button>
      <strong class="properties-heading">Properties</strong>
      <div *ngFor="let field of actions.getFormBinding().fields">
        <input
          [attr.aria-label]="'Property name ' + field.id"
          [ngModel]="field.displayName"
          (ngModelChange)="renameProperty(field.id, $event)"
          [disabled]="!canModify" />
        <button
          type="button"
          (click)="removeProperty(field.id)"
          [disabled]="!canModify">
          Remove property
        </button>
      </div>
      <select
        aria-label="Property target"
        [(ngModel)]="propertyTarget"
        [disabled]="!canModify">
        <option value="">Choose a property</option>
        <option
          *ngFor="let p of properties()"
          [value]="p.value">
          {{ p.label }}
        </option>
      </select>
      <button
        type="button"
        data-action="add-property"
        (click)="exposeProperty()"
        [disabled]="!canModify || !propertyTarget">
        Expose property
      </button>
    </section>`,
  styles: [
    `
      :host {
        display: block;
        padding: 10px;
      }
      summary {
        cursor: pointer;
        font-weight: 600;
      }
      input,
      select {
        width: 100%;
        margin: 4px 0;
        padding: 4px 7px;
      }
      input,
      select,
      button {
        max-width: 100%;
        font: inherit;
        border: 1px solid #d9d9d9;
        border-radius: 3px;
        background: white;
      }
      button {
        margin: 4px 4px 4px 0;
        padding: 3px 7px;
        cursor: pointer;
        color: #1677ff;
        white-space: normal;
      }
      button:disabled,
      input:disabled,
      select:disabled {
        color: #999;
        background: #fafafa;
        cursor: default;
      }
      section {
        margin-top: 12px;
      }
      .properties-heading {
        display: block;
        margin-top: 12px;
      }
      section span {
        display: block;
        font-size: 12px;
        color: #666;
        overflow-wrap: anywhere;
      }
    `,
  ],
})
export class MacroPanelComponent {
  public name = "";
  public busy = false;
  public inputTarget = "";
  public outputTarget = "";
  public propertyTarget = "";
  public readonly directions = ["inputs", "outputs"] as const;
  public readonly type = macroType;
  public get canModify(): boolean {
    return this.actions.checkWorkflowModificationEnabled();
  }
  public get selected(): readonly string[] {
    return this.actions.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
  }
  public get selectedMacro(): boolean {
    const op = this.selected.length === 1 ? this.actions.getTexeraGraph().getOperator(this.selected[0]) : undefined;
    return !!op && isMacro(op);
  }
  constructor(
    public macros: MacroService,
    public actions: WorkflowActionService,
    public metadata: OperatorMetadataService,
    private notification: NotificationService,
    private modal: NzModalService
  ) {
    this.macros.inspect$.pipe(untilDestroyed(this)).subscribe(id => {
      if (id) this.inspect(id);
    });
  }
  public opened(event: Event): void {
    if ((event.target as HTMLDetailsElement).open) void this.refresh();
  }
  public async refresh(): Promise<void> {
    await this.perform(() => this.macros.refresh());
  }
  public async create(): Promise<void> {
    await this.perform(() => this.macros.createFromSelection(this.name));
  }
  public async update(): Promise<void> {
    await this.perform(() => this.macros.adoptLatest(this.selected[0]));
  }
  private async perform(action: () => Promise<unknown>): Promise<void> {
    if (this.busy) return;
    this.busy = true;
    try {
      await action();
    } catch (e) {
      this.notification.error(e instanceof Error ? e.message : String(e));
    } finally {
      this.busy = false;
    }
  }
  public async inspect(id: string): Promise<void> {
    const content = this.actions.getWorkflowContent();
    const op = content.operators.find(o => o.operatorID === id);
    const definition = content.macroDependencies?.find(d => macroType(d) === op?.operatorType);
    if (!definition) return;
    try {
      await this.macros.refresh();
    } catch (e) {
      this.notification.error(e instanceof Error ? e.message : String(e));
    }
    this.modal.create({
      nzTitle: "Macro definition",
      nzContent: MacroInspectorComponent,
      nzWidth: 900,
      nzFooter: null,
      nzData: { definition, dependencies: content.macroDependencies, library: this.macros.library$.value },
    });
    this.macros.inspect$.next(undefined);
  }
  public targets(direction: "inputs" | "outputs"): { value: string; label: string }[] {
    return this.actions
      .getTexeraGraph()
      .getAllOperators()
      .flatMap(op =>
        (direction === "inputs" ? op.inputPorts : op.outputPorts).map(p => ({
          value: JSON.stringify({ operatorID: op.operatorID, portID: p.portID }),
          label: `${op.customDisplayName ?? this.metadata.getOperatorSchema(op.operatorType).additionalMetadata?.userFriendlyName ?? op.operatorType} / ${p.displayName ?? p.portID}`,
        }))
      );
  }
  public targetLabel(direction: "inputs" | "outputs", target: { operatorID: string; portID: string }): string {
    return this.targets(direction).find(p => p.value === JSON.stringify(target))?.label ?? "Missing target";
  }
  public properties(): { value: string; label: string; title: string }[] {
    return this.actions
      .getTexeraGraph()
      .getAllOperators()
      .flatMap(op =>
        Object.entries(this.metadata.getOperatorSchema(op.operatorType).jsonSchema.properties ?? {}).flatMap(
          ([key, schema]) =>
            typeof schema === "boolean"
              ? []
              : [
                  {
                    value: JSON.stringify({ operatorID: op.operatorID, propertyKey: key }),
                    label: `${op.customDisplayName ?? op.operatorType} / ${schema.title ?? key}`,
                    title: schema.title ?? key,
                  },
                ]
        )
      );
  }
  public exposeProperty(): void {
    const selected = this.properties().find(p => p.value === this.propertyTarget);
    if (!this.canModify || !selected) return;
    const target = JSON.parse(selected.value);
    const binding = this.actions.getFormBinding();
    if (binding.fields.some(f => f.operatorID === target.operatorID && f.propertyKey === target.propertyKey)) return;
    this.actions.setFormBinding({
      ...binding,
      fields: [...binding.fields, { id: uuid(), ...target, displayName: selected.title }],
    });
  }
  public renameProperty(id: string, displayName: string): void {
    if (!this.canModify) return;
    const binding = this.actions.getFormBinding();
    this.actions.setFormBinding({
      ...binding,
      fields: binding.fields.map(f => (f.id === id ? { ...f, displayName } : f)),
    });
  }
  public removeProperty(id: string): void {
    if (!this.canModify) return;
    const binding = this.actions.getFormBinding();
    this.actions.setFormBinding({ ...binding, fields: binding.fields.filter(f => f.id !== id) });
  }
  public add(direction: "inputs" | "outputs", target: string): void {
    if (!this.canModify || !this.targets(direction).some(p => p.value === target)) return;
    const boundary = this.actions.getMacroInterface()!;
    this.actions.setMacroInterface({
      ...boundary,
      [direction]: [
        ...boundary[direction],
        {
          portID: `${direction === "inputs" ? "input" : "output"}-${uuid()}`,
          displayName: direction === "inputs" ? "Input" : "Output",
          targets: [JSON.parse(target)],
        },
      ],
    });
  }
  public remove(direction: "inputs" | "outputs", port: MacroPort): void {
    if (!this.canModify) return;
    const boundary = this.actions.getMacroInterface()!;
    this.actions.setMacroInterface({
      ...boundary,
      [direction]: boundary[direction].filter(p => p.portID !== port.portID),
    });
  }
  public rename(direction: "inputs" | "outputs", port: MacroPort, displayName: string): void {
    if (!this.canModify) return;
    const boundary = this.actions.getMacroInterface()!;
    this.actions.setMacroInterface({
      ...boundary,
      [direction]: boundary[direction].map(p => (p.portID === port.portID ? { ...p, displayName } : p)),
    });
  }
  public addTarget(port: MacroPort): void {
    if (!this.canModify || !this.targets("inputs").some(p => p.value === this.inputTarget)) return;
    const target = JSON.parse(this.inputTarget);
    const boundary = this.actions.getMacroInterface()!;
    if (port.targets.some(p => p.operatorID === target.operatorID && p.portID === target.portID)) return;
    this.actions.setMacroInterface({
      ...boundary,
      inputs: boundary.inputs.map(p => (p.portID === port.portID ? { ...p, targets: [...p.targets, target] } : p)),
    });
  }
}

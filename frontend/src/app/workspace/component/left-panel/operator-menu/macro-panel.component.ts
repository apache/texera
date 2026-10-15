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
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzInputModule } from "ng-zorro-antd/input";
import { v4 as uuid } from "uuid";
import { MacroService } from "../../../service/macro/macro.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { OperatorLabelComponent } from "./operator-label/operator-label.component";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { isMacro, MacroDefinition, MacroPort, macroType } from "../../../../common/util/workflow-macro";
import { FormFieldBinding } from "../../../../common/type/workflow";
import { LogicalPort, OperatorPredicate } from "../../../types/workflow-common.interface";
import { FormBindingService } from "../../../service/form-binding/form-binding.service";

@Component({
  selector: "texera-macro-panel",
  imports: [NgIf, NgFor, FormsModule, NzButtonModule, NzInputModule, OperatorLabelComponent],
  templateUrl: "./macro-panel.component.html",
  styleUrls: ["./macro.component.scss"],
})
export class MacroPanelComponent {
  public name = "";
  public busy = false;
  public portTargets = { inputs: "", outputs: "" };
  public propertyTarget = "";
  public readonly directions = ["inputs", "outputs"] as const;
  public readonly type = macroType;
  public readonly trackDefinition = (_: number, d: MacroDefinition) => macroType(d);
  public readonly trackPort = (_: number, p: MacroPort) => p.portID;
  public readonly trackField = (_: number, f: FormFieldBinding) => f.id;
  public readonly trackOption = (_: number, p: { value: string }) => p.value;
  public readonly trackTarget = (_: number, p: LogicalPort) => JSON.stringify([p.operatorID, p.portID]);
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
  public get selectionHint(): string {
    if (!this.canModify) return "Read-only workflow";
    const count = this.selected.length;
    return count ? `${count} operator${count === 1 ? "" : "s"} selected` : "Select operators on the canvas";
  }
  constructor(
    public macros: MacroService,
    public actions: WorkflowActionService,
    public metadata: OperatorMetadataService,
    public forms: FormBindingService,
    private notification: NotificationService
  ) {}
  public opened(event: Event): void {
    if ((event.target as HTMLDetailsElement).open) void this.refresh();
  }
  public async refresh(): Promise<void> {
    await this.perform(() => this.macros.refresh());
  }
  public async create(): Promise<void> {
    await this.perform(async () => {
      await this.macros.createFromSelection(this.name);
      this.name = "";
      this.notification.success("Macro created");
    });
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
  public targets(direction: "inputs" | "outputs"): { value: string; label: string }[] {
    return this.options("portID", op =>
      (direction === "inputs" ? op.inputPorts : op.outputPorts).map(p => [p.portID, p.displayName ?? p.portID] as const)
    );
  }
  public targetLabel(direction: "inputs" | "outputs", target: LogicalPort): string {
    return (
      this.targets(direction).find(
        p => p.value === JSON.stringify({ operatorID: target.operatorID, portID: target.portID })
      )?.label ?? "Missing target"
    );
  }
  public properties(): { value: string; label: string }[] {
    return this.options("propertyKey", op =>
      Object.entries(this.metadata.getOperatorSchema(op.operatorType).jsonSchema.properties ?? {}).flatMap(
        ([key, schema]) => (typeof schema === "boolean" ? [] : [[key, schema.title ?? key] as const])
      )
    );
  }
  private options(kind: "portID" | "propertyKey", entries: (op: OperatorPredicate) => (readonly [string, string])[]) {
    return this.actions
      .getTexeraGraph()
      .getAllOperators()
      .flatMap(op =>
        entries(op).map(([key, label]) => ({
          value: JSON.stringify({ operatorID: op.operatorID, [kind]: key }),
          label: `${this.forms.operatorLabel(op)} / ${label}`,
        }))
      );
  }
  public exposeProperty(): void {
    const selected = this.properties().find(p => p.value === this.propertyTarget);
    if (!this.canModify || !selected) return;
    const target = JSON.parse(selected.value);
    this.forms.addBinding(target.operatorID, target.propertyKey);
  }
  public add(direction: "inputs" | "outputs", target: string): void {
    if (!this.canModify || !this.targets(direction).some(p => p.value === target)) return;
    this.updatePorts(direction, ports => [
      ...ports,
      {
        portID: `${direction === "inputs" ? "input" : "output"}-${uuid()}`,
        displayName: direction === "inputs" ? "Input" : "Output",
        targets: [JSON.parse(target)],
      },
    ]);
  }
  public remove(direction: "inputs" | "outputs", port: MacroPort): void {
    this.updatePorts(direction, ports => ports.filter(p => p.portID !== port.portID));
  }
  public rename(direction: "inputs" | "outputs", port: MacroPort, displayName: string): void {
    this.updatePorts(direction, ports => ports.map(p => (p.portID === port.portID ? { ...p, displayName } : p)));
  }
  public addTarget(port: MacroPort): void {
    if (!this.canModify || !this.targets("inputs").some(p => p.value === this.portTargets.inputs)) return;
    const target = JSON.parse(this.portTargets.inputs);
    if (port.targets.some(p => p.operatorID === target.operatorID && p.portID === target.portID)) return;
    this.updatePorts("inputs", ports =>
      ports.map(p => (p.portID === port.portID ? { ...p, targets: [...p.targets, target] } : p))
    );
  }
  private updatePorts(direction: "inputs" | "outputs", update: (ports: MacroPort[]) => MacroPort[]): void {
    if (!this.canModify) return;
    const boundary = this.actions.getMacroInterface();
    if (boundary) this.actions.setMacroInterface({ ...boundary, [direction]: update(boundary[direction]) });
  }
}

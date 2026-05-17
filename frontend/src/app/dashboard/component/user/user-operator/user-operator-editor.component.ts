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

import { AfterViewInit, Component, ElementRef, NgZone, OnDestroy, OnInit, ViewChild } from "@angular/core";
import { CommonModule } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { ActivatedRoute, Router } from "@angular/router";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzCardModule } from "ng-zorro-antd/card";
import { NzCheckboxModule } from "ng-zorro-antd/checkbox";
import { NzDividerModule } from "ng-zorro-antd/divider";
import { NzFormModule } from "ng-zorro-antd/form";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzInputModule } from "ng-zorro-antd/input";
import { NzInputNumberModule } from "ng-zorro-antd/input-number";
import { NzMessageService } from "ng-zorro-antd/message";
import { NzModalService } from "ng-zorro-antd/modal";
import { NzPopconfirmModule } from "ng-zorro-antd/popconfirm";
import { NzSelectModule } from "ng-zorro-antd/select";
import { NzTagModule } from "ng-zorro-antd/tag";
import { NzTooltipModule } from "ng-zorro-antd/tooltip";
import * as monaco from "monaco-editor";
import "@codingame/monaco-vscode-python-default-extension";
import {
  CUSTOM_OPERATOR_PROPERTY_TYPE_OPTIONS,
  CustomOperator,
  CustomOperatorProperty,
  CustomOperatorPropertyType,
  DEFAULT_CUSTOM_OPERATOR_CATEGORY,
} from "../../../type/custom-operator.interface";
import { CustomOperatorService } from "../../../service/user/custom-operator/custom-operator.service";
import { UserService } from "../../../../common/service/user/user.service";
import { DASHBOARD_USER_OPERATOR } from "../../../../app-routing.constant";

const EMOJI_CHOICES = ["🧩", "🛠️", "✨", "🧪", "📊", "🔢", "🧮", "🔍", "📈", "📉", "🪄", "🤖"];

@UntilDestroy()
@Component({
  selector: "texera-user-operator-editor",
  templateUrl: "./user-operator-editor.component.html",
  styleUrls: ["./user-operator-editor.component.scss"],
  imports: [
    CommonModule,
    FormsModule,
    NzButtonModule,
    NzCardModule,
    NzCheckboxModule,
    NzDividerModule,
    NzFormModule,
    NzIconModule,
    NzInputModule,
    NzInputNumberModule,
    NzPopconfirmModule,
    NzSelectModule,
    NzTagModule,
    NzTooltipModule,
  ],
})
export class UserOperatorEditorComponent implements OnInit, AfterViewInit, OnDestroy {
  @ViewChild("editor", { static: false }) editorElement?: ElementRef<HTMLDivElement>;

  public draft!: Omit<CustomOperator, "id" | "createdAt" | "updatedAt"> & Partial<Pick<CustomOperator, "id">>;
  public editing = false;
  public testOutput: string | null = null;
  public testStatus: "idle" | "ok" | "error" = "idle";
  public readonly emojiChoices = EMOJI_CHOICES;
  public readonly propertyTypeOptions = CUSTOM_OPERATOR_PROPERTY_TYPE_OPTIONS;
  public readonly defaultCategory = DEFAULT_CUSTOM_OPERATOR_CATEGORY;

  public knownCategories: string[] = [DEFAULT_CUSTOM_OPERATOR_CATEGORY];

  private monacoEditor?: monaco.editor.IStandaloneCodeEditor;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private customOperatorService: CustomOperatorService,
    private userService: UserService,
    private message: NzMessageService,
    private modal: NzModalService,
    private ngZone: NgZone
  ) {}

  ngOnInit(): void {
    const idParam = this.route.snapshot.paramMap.get("id");
    if (idParam) {
      const existing = this.customOperatorService.get(idParam);
      if (existing) {
        this.editing = true;
        const { id, createdAt, updatedAt, ...editable } = existing;
        this.draft = { ...editable, id };
      }
    }
    if (!this.draft) {
      this.editing = false;
      this.draft = this.customOperatorService.emptyDraft(this.currentUserName());
    }

    const others = new Set(this.customOperatorService.list().map(op => op.category));
    others.add(DEFAULT_CUSTOM_OPERATOR_CATEGORY);
    this.knownCategories = Array.from(others);
  }

  ngAfterViewInit(): void {
    this.ngZone.runOutsideAngular(() => {
      if (!this.editorElement) return;
      this.monacoEditor = monaco.editor.create(this.editorElement.nativeElement, {
        value: this.draft.code,
        language: "python",
        theme: "vs",
        automaticLayout: true,
        minimap: { enabled: false },
        scrollBeyondLastLine: false,
        fontSize: 13,
        tabSize: 4,
      });
      this.monacoEditor.onDidChangeModelContent(() => {
        const value = this.monacoEditor?.getValue() ?? "";
        this.ngZone.run(() => {
          this.draft.code = value;
          if (this.testStatus !== "idle") {
            this.testStatus = "idle";
            this.testOutput = null;
          }
        });
      });
    });
  }

  ngOnDestroy(): void {
    this.monacoEditor?.dispose();
  }

  public addInputPort(): void {
    this.draft.inputPorts = [...this.draft.inputPorts, { name: `input-${this.draft.inputPorts.length}`, type: "any" }];
  }

  public removeInputPort(idx: number): void {
    this.draft.inputPorts = this.draft.inputPorts.filter((_, i) => i !== idx);
  }

  public addOutputPort(): void {
    this.draft.outputPorts = [
      ...this.draft.outputPorts,
      { name: `output-${this.draft.outputPorts.length}`, type: "any" },
    ];
  }

  public removeOutputPort(idx: number): void {
    this.draft.outputPorts = this.draft.outputPorts.filter((_, i) => i !== idx);
  }

  public addProperty(): void {
    this.draft.properties = [
      ...this.draft.properties,
      { name: `param_${this.draft.properties.length + 1}`, type: "string", default: "", description: "" },
    ];
  }

  public removeProperty(idx: number): void {
    this.draft.properties = this.draft.properties.filter((_, i) => i !== idx);
  }

  public onPropertyTypeChange(prop: CustomOperatorProperty, newType: CustomOperatorPropertyType): void {
    prop.type = newType;
    switch (newType) {
      case "number":
        prop.default = typeof prop.default === "number" ? prop.default : 0;
        break;
      case "boolean":
        prop.default = typeof prop.default === "boolean" ? prop.default : false;
        break;
      case "select":
        prop.options = prop.options && prop.options.length > 0 ? prop.options : ["option_a", "option_b"];
        prop.default = prop.options[0];
        break;
      default:
        prop.default = typeof prop.default === "string" ? prop.default : "";
    }
  }

  public propertyOptionsAsString(prop: CustomOperatorProperty): string {
    return (prop.options ?? []).join(", ");
  }

  public setPropertyOptions(prop: CustomOperatorProperty, value: string): void {
    const next = value
      .split(",")
      .map(s => s.trim())
      .filter(s => s.length > 0);
    prop.options = next;
    if (!next.includes(String(prop.default))) {
      prop.default = next[0] ?? "";
    }
  }

  public testRun(): void {
    const code = this.draft.code ?? "";
    const errors: string[] = [];

    if (!code.trim()) {
      errors.push("Code is empty.");
    }

    const openParens = (code.match(/\(/g) ?? []).length;
    const closeParens = (code.match(/\)/g) ?? []).length;
    if (openParens !== closeParens) {
      errors.push(`Unbalanced parentheses: ${openParens} '(' vs ${closeParens} ')'`);
    }
    const openBrackets = (code.match(/\[/g) ?? []).length;
    const closeBrackets = (code.match(/\]/g) ?? []).length;
    if (openBrackets !== closeBrackets) {
      errors.push(`Unbalanced brackets: ${openBrackets} '[' vs ${closeBrackets} ']'`);
    }

    const hasYield = /\byield\b/.test(code);
    const hasReturn = /\breturn\b/.test(code);
    if (!hasYield && !hasReturn) {
      errors.push("Body has no `yield` or `return` — UDF will produce no output.");
    }

    const hasUDFClass = /class\s+\w+\s*\(\s*UDF\w+Operator\s*\)/.test(code);
    if (!hasUDFClass) {
      errors.push("Could not find a class that extends a UDF*Operator base class.");
    }

    if (errors.length === 0) {
      this.testStatus = "ok";
      this.testOutput = "Looks good. Code parses and follows the UDF operator shape.";
    } else {
      this.testStatus = "error";
      this.testOutput = errors.map(e => "• " + e).join("\n");
    }
  }

  public save(): void {
    const name = this.draft.name.trim();
    if (!name) {
      this.message.warning("Please give the operator a name.");
      return;
    }
    if (this.draft.inputPorts.length === 0 && this.draft.outputPorts.length === 0) {
      this.message.warning("An operator needs at least one input or output port.");
      return;
    }

    const category = this.draft.category?.trim() || DEFAULT_CUSTOM_OPERATOR_CATEGORY;
    const payload = { ...this.draft, name, category };

    if (this.editing && this.draft.id) {
      this.customOperatorService.update(this.draft.id, payload);
      this.message.success(`Updated "${name}"`);
    } else {
      this.customOperatorService.create(payload);
      this.message.success(`Created "${name}"`);
    }
    this.router.navigateByUrl(DASHBOARD_USER_OPERATOR);
  }

  public cancel(): void {
    this.router.navigateByUrl(DASHBOARD_USER_OPERATOR);
  }

  public confirmDelete(): void {
    if (!this.draft.id) return;
    this.customOperatorService.delete(this.draft.id);
    this.message.success("Operator deleted");
    this.router.navigateByUrl(DASHBOARD_USER_OPERATOR);
  }

  private currentUserName(): string {
    const user = this.userService.getCurrentUser();
    return user?.name ?? "anonymous";
  }
}

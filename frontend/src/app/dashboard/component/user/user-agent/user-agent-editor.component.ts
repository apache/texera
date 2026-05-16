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

import { Component, inject, OnInit } from "@angular/core";
import { FormsModule } from "@angular/forms";
import { CommonModule } from "@angular/common";
import { NzModalRef, NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { NzInputModule } from "ng-zorro-antd/input";
import { NzSelectModule } from "ng-zorro-antd/select";
import { NzCheckboxModule } from "ng-zorro-antd/checkbox";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzFormModule } from "ng-zorro-antd/form";
import { NzDividerModule } from "ng-zorro-antd/divider";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzMessageService } from "ng-zorro-antd/message";
import {
  CustomAgent,
  AGENT_DOMAIN_OPTIONS,
  AGENT_METHODOLOGY_OPTIONS,
  AGENT_MODEL_OPTIONS,
  AGENT_TASK_TYPE_OPTIONS,
  AGENT_OUTPUT_FORMAT_OPTIONS,
  KnowledgeFile,
  KNOWLEDGE_FILE_MAX_BYTES,
  KNOWLEDGE_FILE_ACCEPT,
} from "../../../type/custom-agent.interface";
import { OperatorMetadataService } from "../../../../workspace/service/operator-metadata/operator-metadata.service";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";

interface OperatorOption {
  value: string;
  label: string;
}

interface WorkflowOption {
  value: number;
  label: string;
}

@Component({
  selector: "texera-user-agent-editor",
  templateUrl: "./user-agent-editor.component.html",
  styleUrls: ["./user-agent-editor.component.scss"],
  imports: [
    CommonModule,
    FormsModule,
    NzInputModule,
    NzSelectModule,
    NzCheckboxModule,
    NzButtonModule,
    NzFormModule,
    NzDividerModule,
    NzIconModule,
  ],
})
export class UserAgentEditorComponent implements OnInit {
  protected readonly DOMAINS = AGENT_DOMAIN_OPTIONS;
  protected readonly METHODOLOGIES = AGENT_METHODOLOGY_OPTIONS;
  protected readonly MODELS = AGENT_MODEL_OPTIONS;
  protected readonly TASK_TYPES = AGENT_TASK_TYPE_OPTIONS;
  protected readonly OUTPUT_FORMATS = AGENT_OUTPUT_FORMAT_OPTIONS;
  protected readonly KNOWLEDGE_ACCEPT = KNOWLEDGE_FILE_ACCEPT;

  public draft!: Omit<CustomAgent, "id" | "createdAt" | "updatedAt"> & Partial<Pick<CustomAgent, "id">>;
  public operatorOptions: OperatorOption[] = [];
  public workflowOptions: WorkflowOption[] = [];
  public dragging = false;

  private readonly modalRef = inject(NzModalRef);
  private readonly modalData = inject<{
    initial: Omit<CustomAgent, "id" | "createdAt" | "updatedAt"> & Partial<Pick<CustomAgent, "id">>;
    title: string;
  }>(NZ_MODAL_DATA);

  constructor(
    private operatorMetadataService: OperatorMetadataService,
    private workflowPersistService: WorkflowPersistService,
    private message: NzMessageService
  ) {}

  ngOnInit(): void {
    this.draft = { ...this.modalData.initial };

    this.operatorMetadataService.getOperatorMetadata().subscribe(meta => {
      this.operatorOptions = (meta?.operators ?? [])
        .map(op => ({
          value: op.operatorType,
          label: op.additionalMetadata?.userFriendlyName
            ? `${op.additionalMetadata.userFriendlyName} (${op.operatorType})`
            : op.operatorType,
        }))
        .sort((a, b) => a.label.localeCompare(b.label));
    });

    this.workflowPersistService.retrieveWorkflowsBySessionUser().subscribe({
      next: workflows => {
        this.workflowOptions = workflows
          .filter(w => w.workflow.wid !== undefined)
          .map(w => ({ value: w.workflow.wid as number, label: w.workflow.name }))
          .sort((a, b) => a.label.localeCompare(b.label));
      },
      error: () => {
        // Not logged in or backend unavailable — leave list empty.
      },
    });
  }

  public save(): void {
    if (!this.draft.name.trim()) return;
    this.modalRef.close(this.draft);
  }

  public cancel(): void {
    this.modalRef.close(null);
  }

  public onFileInput(event: Event): void {
    const input = event.target as HTMLInputElement;
    if (!input.files) return;
    this.ingestFiles(Array.from(input.files));
    input.value = "";
  }

  public onDrop(event: DragEvent): void {
    event.preventDefault();
    this.dragging = false;
    const files = event.dataTransfer?.files;
    if (files) this.ingestFiles(Array.from(files));
  }

  public onDragOver(event: DragEvent): void {
    event.preventDefault();
    this.dragging = true;
  }

  public onDragLeave(event: DragEvent): void {
    event.preventDefault();
    this.dragging = false;
  }

  public removeKnowledgeFile(id: string): void {
    this.draft.knowledgeFiles = this.draft.knowledgeFiles.filter(f => f.id !== id);
  }

  private ingestFiles(files: File[]): void {
    files.forEach(file => {
      if (file.size > KNOWLEDGE_FILE_MAX_BYTES) {
        this.message.warning(`"${file.name}" is too large (max 1 MB).`);
        return;
      }
      const reader = new FileReader();
      reader.onload = () => {
        const result = reader.result;
        if (typeof result !== "string") return;
        const commaIdx = result.indexOf(",");
        const base64 = commaIdx >= 0 ? result.slice(commaIdx + 1) : result;
        const entry: KnowledgeFile = {
          id: `kf-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`,
          name: file.name,
          mimeType: file.type || "application/octet-stream",
          size: file.size,
          contentBase64: base64,
        };
        this.draft.knowledgeFiles = [...this.draft.knowledgeFiles, entry];
      };
      reader.readAsDataURL(file);
    });
  }

  public formatSize(bytes: number): string {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
  }
}

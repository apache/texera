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

import { Component, OnInit } from "@angular/core";
import { CommonModule } from "@angular/common";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzModalService } from "ng-zorro-antd/modal";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzCardModule } from "ng-zorro-antd/card";
import { NzTagModule } from "ng-zorro-antd/tag";
import { NzEmptyModule } from "ng-zorro-antd/empty";
import { NzPopconfirmModule } from "ng-zorro-antd/popconfirm";
import { NzTooltipModule } from "ng-zorro-antd/tooltip";
import { CustomAgentService } from "../../../service/user/custom-agent/custom-agent.service";
import { UserService } from "../../../../common/service/user/user.service";
import { UserAgentEditorComponent } from "./user-agent-editor.component";
import {
  AGENT_DOMAIN_OPTIONS,
  AGENT_METHODOLOGY_OPTIONS,
  CustomAgent,
} from "../../../type/custom-agent.interface";

@UntilDestroy()
@Component({
  selector: "texera-user-agent",
  templateUrl: "./user-agent.component.html",
  styleUrls: ["./user-agent.component.scss"],
  imports: [
    CommonModule,
    NzButtonModule,
    NzIconModule,
    NzCardModule,
    NzTagModule,
    NzEmptyModule,
    NzPopconfirmModule,
    NzTooltipModule,
  ],
})
export class UserAgentComponent implements OnInit {
  public agents: CustomAgent[] = [];

  private readonly domainLabels = new Map(AGENT_DOMAIN_OPTIONS.map(o => [o.value, o.label]));
  private readonly methodologyLabels = new Map(AGENT_METHODOLOGY_OPTIONS.map(o => [o.value, o.label]));

  constructor(
    private customAgentService: CustomAgentService,
    private modalService: NzModalService,
    private userService: UserService
  ) {}

  ngOnInit(): void {
    this.customAgentService
      .list$()
      .pipe(untilDestroyed(this))
      .subscribe(agents => (this.agents = agents));
  }

  public domainLabel(value: string): string {
    return this.domainLabels.get(value as any) ?? value;
  }

  public methodologyLabel(value: string): string {
    return this.methodologyLabels.get(value as any) ?? value;
  }

  public openCreate(): void {
    const creator = this.currentUserName();
    const draft = this.customAgentService.emptyDraft(creator);
    this.openEditor("Create agent", draft, saved => {
      this.customAgentService.create(saved);
    });
  }

  public openEdit(agent: CustomAgent): void {
    const { id, createdAt, updatedAt, ...editable } = agent;
    this.openEditor("Edit agent", { ...editable, id }, saved => {
      const { id: savedId, ...patch } = saved as CustomAgent;
      this.customAgentService.update(agent.id, patch);
    });
  }

  public duplicate(agent: CustomAgent): void {
    this.customAgentService.duplicate(agent.id);
  }

  public delete(agent: CustomAgent): void {
    this.customAgentService.delete(agent.id);
  }

  private currentUserName(): string {
    const user = this.userService.getCurrentUser();
    return user?.name ?? "anonymous";
  }

  private openEditor(
    title: string,
    initial: Omit<CustomAgent, "id" | "createdAt" | "updatedAt"> & Partial<Pick<CustomAgent, "id">>,
    onSave: (saved: any) => void
  ): void {
    const ref = this.modalService.create({
      nzTitle: title,
      nzContent: UserAgentEditorComponent,
      nzData: { initial, title },
      nzFooter: null,
      nzWidth: 720,
    });
    ref.afterClose.subscribe(result => {
      if (result) onSave(result);
    });
  }
}

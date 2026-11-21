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
import { NzBreadCrumbModule } from "ng-zorro-antd/breadcrumb";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzButtonModule } from "ng-zorro-antd/button";
import { MacroNavigationService } from "../../../service/macro/macro-navigation.service";
import { macroRevisionLabel } from "../../../../common/util/workflow-macro";
import { NzPopoverModule } from "ng-zorro-antd/popover";
import { Router, RouterLink } from "@angular/router";
import { macroCanvasUrl, workspaceCanvasUrl } from "../../../../app-routing.constant";
import { MacroService } from "../../../service/macro/macro.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";

@Component({
  selector: "texera-macro-breadcrumbs",
  imports: [NgFor, NgIf, NzBreadCrumbModule, NzButtonModule, NzIconModule, NzPopoverModule, RouterLink],
  template: ` <nav
    aria-label="Workflow hierarchy"
    [title]="navigation.hint">
    <button
      *ngIf="navigation.parents.length"
      nz-button
      nzType="text"
      aria-label="Return to parent"
      [disabled]="navigation.busy"
      (click)="back(navigation.parents.length - 1)">
      <span
        nz-icon
        nzType="arrow-left"></span>
    </button>
    <nz-breadcrumb *ngIf="navigation.parents.length">
      <nz-breadcrumb-item *ngFor="let parent of navigation.parents; let i = index">
        <button
          nz-button
          nzType="link"
          [title]="navigation.breadcrumb(i)"
          [disabled]="navigation.busy"
          (click)="back(i)">
          {{ navigation.breadcrumb(i) }}
        </button>
      </nz-breadcrumb-item>
    </nz-breadcrumb>
    <span
      *ngIf="navigation.parents.length"
      aria-hidden="true"
      class="separator"
      >/</span
    >
    <span aria-current="page">
      <ng-container *ngIf="navigation.current as current">
        <span
          *ngIf="current.instanceName !== current.definition.name"
          class="instance-name"
          [title]="current.instanceName + ' (instance name)'"
          >{{ current.instanceName }} <small>uses</small></span
        >
      </ng-container>
      <ng-content></ng-content>
    </span>
    <span
      *ngIf="navigation.parents.length"
      class="revision"
      >{{ label(navigation.parents[navigation.parents.length - 1].definition) }}</span
    >
    <button
      *ngIf="navigation.readOnlyReason"
      nz-button
      nzType="text"
      nzSize="small"
      nz-popover
      nzPopoverTitle="Read-only snapshot"
      nzPopoverTrigger="click"
      [nzPopoverContent]="readOnlyHelp"
      [title]="navigation.readOnlyReason"
      aria-label="Why this macro is read-only">
      <span
        nz-icon
        nzType="lock"></span>
      Read-only
    </button>
    <ng-template #readOnlyHelp>
      <div class="read-only-help">
        <p>{{ navigation.readOnlyReason }}</p>
        <p *ngIf="navigation.current as current">
          Existing usages keep their saved revisions. Saving an editable copy creates a separate macro.
        </p>
        <a
          *ngIf="navigation.current?.source && !navigation.current!.source!.readonly"
          [routerLink]="sourceURL(navigation.current!.source!.wid!)"
          >Edit latest definition</a
        >
      </div>
    </ng-template>
    <button
      *ngIf="navigation.dirty"
      nz-button
      nzType="link"
      nzSize="small"
      [disabled]="navigation.busy"
      (click)="back(navigation.parents.length - 1, true)">
      Discard edits
    </button>
    <button
      *ngIf="
        navigation.actions.getMacroInterface() &&
        (navigation.dirty || !navigation.actions.checkWorkflowModificationEnabled())
      "
      nz-button
      nzSize="small"
      [disabled]="navigation.busy || copying"
      (click)="copy()">
      Save editable copy
    </button>
    <button
      *ngIf="navigation.actions.getMacroInterface()"
      nz-button
      nzType="text"
      nz-popover
      nzPopoverTitle="Macro details"
      nzPopoverTrigger="click"
      [nzPopoverContent]="details"
      (nzPopoverVisibleChange)="$event && refreshUsages()"
      aria-label="Macro details">
      <span
        nz-icon
        nzType="info-circle"></span>
    </button>
    <ng-template #details>
      <strong>Used by</strong>
      <p>Accessible usages, including nested macros. Each keeps its saved revision.</p>
      <p *ngIf="!usages.length">No accessible usages loaded.</p>
      <div *ngFor="let usage of usages">
        <a
          [routerLink]="canvasURL(usage.wid)"
          [queryParams]="{ macro: usage.path }"
          >{{ usage.label }}</a
        >
      </div>
      <button
        nz-button
        nzSize="small"
        [nzLoading]="refreshing"
        (click)="refreshUsages()">
        Refresh
      </button>
    </ng-template>
  </nav>`,
  styles: [
    `
      :host {
        display: inline-block;
        vertical-align: middle;
        max-width: 100%;
      }
      nav {
        display: flex;
        align-items: center;
        gap: 4px;
        white-space: nowrap;
      }
      nz-breadcrumb {
        max-width: 38vw;
        overflow-x: auto;
        scrollbar-width: thin;
      }
      nz-breadcrumb button {
        padding: 0 4px;
        max-width: 190px;
        overflow: hidden;
        text-overflow: ellipsis;
      }
      .separator,
      small {
        color: #8c8c8c;
      }
      .instance-name {
        margin-right: 4px;
      }
      .read-only-help {
        max-width: 360px;
        white-space: normal;
      }
      .revision {
        color: #595959;
        font-size: 12px;
        margin-right: 8px;
      }
    `,
  ],
})
export class MacroBreadcrumbsComponent {
  public label = macroRevisionLabel;
  public copying = false;
  public refreshing = false;
  public canvasURL = workspaceCanvasUrl;
  public sourceURL = macroCanvasUrl;
  public get usages() {
    return this.macros.usages$.value.filter(u => u.id === this.navigation.actions.getMacroInterface()?.id);
  }
  public async refreshUsages(): Promise<void> {
    if (this.refreshing) return;
    this.refreshing = true;
    try {
      await this.macros.refresh();
    } catch (error) {
      this.notification.error(error instanceof Error ? error.message : String(error));
    } finally {
      this.refreshing = false;
    }
  }
  constructor(
    public navigation: MacroNavigationService,
    private notification: NotificationService,
    private macros: MacroService,
    private router: Router
  ) {}
  public async copy(): Promise<void> {
    if (this.copying) return;
    this.copying = true;
    try {
      const copy = await this.macros.copyDefinition(
        this.navigation.actions.getWorkflow(),
        this.navigation.actions.getMacroDependencies()
      );
      if (this.navigation.parents.length) await this.navigation.back(0, true);
      await this.router.navigateByUrl(macroCanvasUrl(copy.wid!));
    } catch (error) {
      this.notification.error(error instanceof Error ? error.message : String(error));
    } finally {
      this.copying = false;
    }
  }
  public async back(index: number, discard = false): Promise<void> {
    try {
      await this.navigation.back(index, discard);
    } catch (error) {
      this.notification.error(error instanceof Error ? error.message : String(error));
    }
  }
}

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

import { NgIf, TitleCasePipe } from "@angular/common";
import { Component, EventEmitter, Input, Output } from "@angular/core";
import { FormsModule } from "@angular/forms";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzDividerComponent } from "ng-zorro-antd/divider";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzSwitchComponent } from "ng-zorro-antd/switch";
import { MarkdownDescriptionComponent } from "./markdown-description/markdown-description.component";

/**
 * Shared Settings-tab cards (General and Access & visibility) for dataset and model detail pages,
 * with a projection slot for resource-specific middle cards and trailing sections.
 */
@Component({
  // eslint-disable-next-line @angular-eslint/component-selector -- Keep the existing settings scroll container as the host.
  selector: "div[texeraResourceDetailSettings]",
  imports: [
    FormsModule,
    MarkdownDescriptionComponent,
    NgIf,
    NzButtonComponent,
    NzCardComponent,
    NzDividerComponent,
    NzInputDirective,
    NzSwitchComponent,
    TitleCasePipe,
  ],
  template: `
    <nz-card class="settings-general-card">
      <h3 class="settings-card-title">General</h3>

      <div class="settings-name-row">
        <div class="settings-name-label">
          <label>{{ resourceType | titlecase }} name</label>
          <p class="settings-hint">{{ nameHints[resourceType] }}</p>
          <p
            *ngIf="uploadsInFlight"
            class="settings-hint">
            An upload is in progress — finish or cancel it in Versions &amp; Files first, or it will be left incomplete.
          </p>
        </div>
        <div class="settings-name-controls">
          <input
            nz-input
            [disabled]="uploadsInFlight"
            [ngModel]="editedName"
            (ngModelChange)="editedNameChange.emit($event)" />
          <button
            nz-button
            nzType="primary"
            [disabled]="uploadsInFlight"
            (click)="saveName.emit()">
            Save
          </button>
        </div>
      </div>

      <nz-divider></nz-divider>

      <div class="settings-field">
        <label class="settings-field-label">Description</label>
        <div class="settings-field-hint">Shown on cards.</div>
        <texera-markdown-description
          [description]="description"
          [editable]="true"
          [enableViewMore]="true"
          (descriptionChange)="descriptionChange.emit($event)">
        </texera-markdown-description>
      </div>
    </nz-card>

    <ng-content select="[resourceSettingsBetweenCards]"></ng-content>

    <nz-card class="settings-general-card">
      <h3 class="settings-card-title">Access &amp; visibility</h3>

      <div class="settings-name-row">
        <div class="settings-name-label">
          <label>Visibility</label>
          <p class="settings-hint">
            {{
              isPublic
                ? "Public — anyone can view this " + resourceType + "."
                : "Private — only you and invited collaborators can see this " + resourceType + "."
            }}
          </p>
        </div>
        <div class="settings-name-controls">
          <nz-switch
            [ngModel]="isPublic"
            (ngModelChange)="publicChange.emit($event)"
            nzCheckedChildren="public"
            nzUnCheckedChildren="private"></nz-switch>
        </div>
      </div>

      <nz-divider></nz-divider>

      <div class="settings-name-row">
        <div class="settings-name-label">
          <label>Downloadable</label>
          <p class="settings-hint">
            {{
              isDownloadable
                ? "Viewers can download this " + resourceType + "."
                : "Viewers can browse files but cannot download them."
            }}
          </p>
        </div>
        <div class="settings-name-controls">
          <nz-switch
            [ngModel]="isDownloadable"
            (ngModelChange)="downloadableChange.emit($event)"
            [nzDisabled]="!isOwner"
            nzCheckedChildren="allowed"
            nzUnCheckedChildren="blocked"></nz-switch>
        </div>
      </div>
    </nz-card>

    <ng-content></ng-content>
  `,
  styles: [
    `
      :host ::ng-deep .settings-general-card {
        max-width: 700px;
        margin: 24px auto 0;
        border-radius: 16px;
        border: 1px solid #d9d9d9;
      }
      :host ::ng-deep .settings-general-card .settings-card-title {
        font-weight: 600;
        margin-bottom: 20px;
      }
      :host ::ng-deep .settings-general-card .settings-field-label {
        display: block;
        font-weight: 600;
      }
      :host ::ng-deep .settings-general-card .settings-field-hint {
        color: rgba(0, 0, 0, 0.45);
        font-size: 12px;
        margin-bottom: 8px;
      }
      :host ::ng-deep .settings-general-card .settings-name-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 16px;
      }
      :host ::ng-deep .settings-general-card .settings-name-row .settings-name-label label {
        font-weight: 600;
      }
      :host ::ng-deep .settings-general-card .settings-name-row .settings-hint {
        color: rgba(0, 0, 0, 0.45);
        font-size: 12px;
        margin: 4px 0 0;
      }
      :host ::ng-deep .settings-general-card .settings-name-row .settings-name-controls {
        display: flex;
        gap: 8px;
        flex-shrink: 0;
      }
      :host ::ng-deep .settings-general-card .settings-name-row .settings-name-controls > input,
      :host ::ng-deep .settings-general-card .settings-name-row .settings-name-controls > button {
        border-radius: 8px;
      }
    `,
  ],
})
export class ResourceDetailSettingsComponent {
  protected readonly nameHints = {
    dataset: "Lowercase letters, numbers and hyphens.",
    model: "Letters, numbers, underscores and hyphens.",
  };

  @Input({ required: true }) resourceType!: "dataset" | "model";
  @Input() editedName = "";
  @Input() uploadsInFlight = false;
  @Input() description = "";
  @Input() isPublic = false;
  @Input() isDownloadable = true;
  @Input() isOwner = false;

  @Output() editedNameChange = new EventEmitter<string>();
  @Output() saveName = new EventEmitter<void>();
  @Output() descriptionChange = new EventEmitter<string>();
  @Output() publicChange = new EventEmitter<boolean>();
  @Output() downloadableChange = new EventEmitter<boolean>();
}

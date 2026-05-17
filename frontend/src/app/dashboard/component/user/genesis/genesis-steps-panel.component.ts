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

import { CommonModule } from "@angular/common";
import { Component, Input } from "@angular/core";

export type GenesisStepState = "pending" | "current" | "completed";

export interface GenesisStepItem {
  label: string;
  state: GenesisStepState;
}

@Component({
  selector: "texera-genesis-steps-panel",
  standalone: true,
  imports: [CommonModule],
  template: `
    <div
      class="genesis-steps-panel"
      role="status"
      aria-live="polite">
      <div class="genesis-steps-panel__list">
        <div
          *ngFor="let s of steps"
          class="genesis-steps-panel__row"
          [class.genesis-steps-panel__row--pending]="s.state === 'pending'"
          [class.genesis-steps-panel__row--current]="s.state === 'current'"
          [class.genesis-steps-panel__row--completed]="s.state === 'completed'">
          <span
            class="genesis-steps-panel__icon"
            aria-hidden="true">
            <ng-container [ngSwitch]="s.state">
              <svg
                *ngSwitchCase="'completed'"
                class="genesis-steps-panel__check"
                viewBox="0 0 20 20"
                width="18"
                height="18"
                fill="none"
                stroke="currentColor"
                stroke-width="2.2"
                stroke-linecap="round"
                stroke-linejoin="round">
                <path d="M4.5 10.5 8.5 14.5 15.5 5.5" />
              </svg>
              <span
                *ngSwitchCase="'current'"
                class="genesis-steps-panel__spinner"></span>
              <span
                *ngSwitchDefault
                class="genesis-steps-panel__circle"></span>
            </ng-container>
          </span>
          <span class="genesis-steps-panel__label">{{ s.label }}</span>
        </div>
      </div>
    </div>
  `,
  styles: [
    `
      .genesis-steps-panel {
        width: 100%;
        max-width: 400px;
        margin: 0 auto;
        padding: 16px 20px;
        border-radius: 8px;
        box-sizing: border-box;
        background: rgba(255, 255, 255, 0.6);
        border: 1px solid #e5e7eb;
        font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        font-size: 14px;
        line-height: 1.35;
      }
      .genesis-steps-panel__list {
        display: flex;
        flex-direction: column;
        gap: 8px;
      }
      .genesis-steps-panel__row {
        display: flex;
        align-items: flex-start;
        gap: 10px;
      }
      .genesis-steps-panel__icon {
        flex-shrink: 0;
        width: 18px;
        height: 18px;
        margin-top: 1px;
        display: flex;
        align-items: center;
        justify-content: center;
      }
      .genesis-steps-panel__check {
        color: #16a34a;
      }
      .genesis-steps-panel__spinner {
        display: block;
        width: 16px;
        height: 16px;
        border: 2px solid #dbeafe;
        border-top-color: #2563eb;
        border-radius: 50%;
        animation: genesis-steps-spin 0.7s linear infinite;
      }
      .genesis-steps-panel__circle {
        display: block;
        width: 14px;
        height: 14px;
        border: 1.5px solid #9ca3af;
        border-radius: 50%;
        opacity: 0.35;
      }
      .genesis-steps-panel__row--pending .genesis-steps-panel__label {
        color: rgba(17, 24, 39, 0.3);
      }
      .genesis-steps-panel__row--current .genesis-steps-panel__label {
        color: #111827;
        font-weight: 500;
      }
      .genesis-steps-panel__row--completed .genesis-steps-panel__label {
        color: #9ca3af;
        font-weight: 400;
      }
      @keyframes genesis-steps-spin {
        to {
          transform: rotate(360deg);
        }
      }
    `,
  ],
})
export class GenesisStepsPanelComponent {
  @Input() steps: GenesisStepItem[] = [];
}

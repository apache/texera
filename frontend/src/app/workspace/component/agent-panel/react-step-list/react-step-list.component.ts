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

import { Component, Input, Output, EventEmitter } from "@angular/core";
import { ReActStep } from "../../../service/copilot/copilot-types";

/**
 * Component for displaying a list of ReActSteps.
 * Shows agent ID, message ID, and step number for each step.
 * Emits an event when a step is clicked.
 */
@Component({
  selector: "texera-react-step-list",
  templateUrl: "./react-step-list.component.html",
  styleUrls: ["./react-step-list.component.scss"],
})
export class ReActStepListComponent {
  @Input() steps: ReActStep[] = [];
  @Input() agentId: string | null = null;
  @Input() title: string = "ReActSteps";
  @Input() emptyMessage: string = "No steps available";
  @Output() stepClicked = new EventEmitter<ReActStep>();

  public selectedStep: ReActStep | null = null;
  public isModalVisible: boolean = false;

  public onStepClick(step: ReActStep): void {
    this.selectedStep = step;
    this.isModalVisible = true;
    this.stepClicked.emit(step);
  }

  public onModalClose(): void {
    this.isModalVisible = false;
  }
}

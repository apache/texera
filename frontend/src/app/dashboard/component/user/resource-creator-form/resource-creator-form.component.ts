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

import { Component, EventEmitter, Input, Output } from "@angular/core";
import { FormGroup, FormsModule } from "@angular/forms";
import { FormlyFieldConfig, FormlyModule } from "@ngx-formly/core";
import { NgIf } from "@angular/common";
import { NzSpinComponent } from "ng-zorro-antd/spin";
import { NzSwitchComponent } from "ng-zorro-antd/switch";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";

/**
 * The creation form shared by the dataset and model creators: the Formly fields, the visibility
 * and downloadability switches, and the Create and Cancel buttons. The parent keeps the state and
 * handles every event.
 */
@Component({
  selector: "texera-resource-creator-form",
  templateUrl: "./resource-creator-form.component.html",
  styleUrls: ["./resource-creator-form.component.scss"],
  imports: [
    NzSpinComponent,
    FormlyModule,
    NgIf,
    NzSwitchComponent,
    FormsModule,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
  ],
})
export class ResourceCreatorFormComponent {
  @Input({ required: true }) form!: FormGroup;
  @Input({ required: true }) fields!: FormlyFieldConfig[];
  @Input({ required: true }) model!: Record<string, unknown>;
  @Input() isCreating = false;
  @Input() showToggles = true;
  @Input() isPublic = false;
  @Input() isDownloadable = false;
  @Output() isPublicChange = new EventEmitter<boolean>();
  @Output() isDownloadableChange = new EventEmitter<boolean>();
  @Output() createClick = new EventEmitter<void>();
  @Output() cancelClick = new EventEmitter<void>();
}

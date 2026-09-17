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

import { Component, OnInit, Inject } from "@angular/core";
import { FormGroup, ReactiveFormsModule } from "@angular/forms";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { FormlyFieldConfig, FormlyModule } from "@ngx-formly/core";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { Contributor } from "../../../../../../common/type/dataset";
import { contributorFieldGroup } from "./contributor-form-fields";

@Component({
  selector: "texera-user-dataset-contributor-editor",
  templateUrl: "./user-dataset-contributor-editor.component.html",
  styleUrls: ["./user-dataset-contributor-editor.component.scss"],
  imports: [ReactiveFormsModule, FormlyModule, NzButtonComponent, NzWaveDirective, ɵNzTransitionPatchDirective],
})
export class UserDatasetContributorEditorComponent implements OnInit {
  contributorForm: FormGroup = new FormGroup({});
  model: Partial<Contributor> = {};

  fields: FormlyFieldConfig[] = contributorFieldGroup();

  constructor(
    @Inject(NZ_MODAL_DATA) private contributorData: Contributor | null,
    private modalRef: NzModalRef
  ) {}

  ngOnInit() {
    if (this.contributorData) {
      this.model = { ...this.contributorData };
    }
  }

  submit() {
    if (this.contributorForm.invalid) {
      return;
    }
    this.modalRef.close({ ...this.model });
  }

  cancel() {
    this.modalRef.close();
  }
}

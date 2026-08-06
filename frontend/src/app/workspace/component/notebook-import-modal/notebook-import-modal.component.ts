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

import { Component, inject } from "@angular/core";
import { FormBuilder, FormGroup, Validators, ReactiveFormsModule } from "@angular/forms";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { NzUploadComponent, NzUploadFile } from "ng-zorro-antd/upload";
import { Observable } from "rxjs";
import { AsyncPipe, NgIf, NgFor, NgOptimizedImage } from "@angular/common";
import { NzFormModule } from "ng-zorro-antd/form";
import { NzSelectModule } from "ng-zorro-antd/select";
import { NzAlertModule } from "ng-zorro-antd/alert";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NotebookMigrationService } from "../../service/notebook-migration/notebook-migration.service";

// Passed in via nzData. The modal delegates "may I proceed?" to the opener so the
// opener can keep the overwrite-confirm and generation logic (and the workflow state it
// needs) without the modal knowing about them. Resolve true to close the modal (the
// import has started), false to keep it open with the user's selection intact.
export interface NotebookImportModalData {
  requestImport: (file: NzUploadFile, model: string) => Promise<boolean>;
}

/**
 * The "AI Generate Workflow from Python Notebook" modal body. It owns the upload form and
 * the three model-dropdown states (loading / has models / none). On Submit it delegates the
 * decision to proceed to its opener via the requestImport callback (passed in through
 * nzData); the opener runs the overwrite-confirm and generation pipeline and the modal
 * closes itself only when that resolves true. Mirrors the component-as-nzContent pattern
 * used by the other modals opened from the menu (ResultExportationComponent, ...).
 */
@Component({
  selector: "texera-notebook-import-modal",
  templateUrl: "./notebook-import-modal.component.html",
  styleUrls: ["./notebook-import-modal.component.scss"],
  imports: [
    NgIf,
    NgFor,
    AsyncPipe,
    NgOptimizedImage,
    ReactiveFormsModule,
    NzFormModule,
    NzSelectModule,
    NzAlertModule,
    NzUploadComponent,
    NzButtonComponent,
    NzIconDirective,
  ],
})
export class NotebookImportModalComponent {
  private readonly fb = inject(FormBuilder);
  private readonly modalRef = inject(NzModalRef);
  private readonly notebookMigrationService = inject(NotebookMigrationService);
  private readonly data: NotebookImportModalData = inject(NZ_MODAL_DATA);

  public readonly importForm: FormGroup = this.fb.group({
    file: [null, Validators.required],
    model: ["", Validators.required],
  });

  // Drives the three model-dropdown states: pending (loading), a non-empty list (selectable),
  // and an empty list (no models available, e.g. the fetch failed or the feature is off).
  public readonly models$: Observable<{ name: string }[]> = this.notebookMigrationService.getAvailableModels();

  public beforeUpload = (file: NzUploadFile) => {
    this.importForm.patchValue({ file });
    this.importForm.get("file")?.markAsDirty();
    this.importForm.get("file")?.updateValueAndValidity();
    return false; // prevent auto upload
  };

  public onCancel(): void {
    this.modalRef.close();
  }

  // Guards against a second submit while the opener callback (which may show an
  // overwrite confirmation) is still pending, so a double-click cannot start two imports.
  public isSubmitting = false;

  public async onSubmit(): Promise<void> {
    if (this.isSubmitting || !this.importForm.valid) return;
    const file: NzUploadFile = this.importForm.get("file")?.value;
    const model: string = this.importForm.get("model")?.value;
    this.isSubmitting = true;
    try {
      // Ask the opener whether to proceed; close only if it does, so cancelling the
      // overwrite-confirm leaves this modal open with the selection preserved.
      if (await this.data.requestImport(file, model)) {
        this.modalRef.close();
      }
    } finally {
      // Re-enable submit if the modal is still open (import declined or it threw).
      this.isSubmitting = false;
    }
  }
}

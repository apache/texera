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

import { Component, HostListener, inject, OnDestroy } from "@angular/core";
import { FormBuilder, FormControl, FormGroup, Validators, ReactiveFormsModule } from "@angular/forms";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { NzUploadComponent, NzUploadFile } from "ng-zorro-antd/upload";
import { Observable } from "rxjs";
import { AsyncPipe, NgIf, NgFor, NgOptimizedImage, NgTemplateOutlet } from "@angular/common";
import { NzFormModule } from "ng-zorro-antd/form";
import { NzSelectModule } from "ng-zorro-antd/select";
import { NzSpinComponent } from "ng-zorro-antd/spin";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzTabsComponent, NzTabComponent, NzTabDirective } from "ng-zorro-antd/tabs";
import { NzSegmentedComponent } from "ng-zorro-antd/segmented";
import { NotebookMigrationService } from "../../service/notebook-migration/notebook-migration.service";
import { folderRootName, pickedFilePath } from "../../service/notebook-migration/folder-assembly";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { MigrationLanguage } from "../../service/notebook-migration/migration-llm";

// Passed in via nzData. requestImport resolves true to close the modal, false to keep it open
// with the user's selection intact (bad file or a retryable failure). A folder is handed over as
// the whole picked list, which is also how the opener tells a folder from a single file.
export interface NotebookImportModalData {
  requestImport: (
    selection: NzUploadFile | NzUploadFile[],
    model: string,
    language: MigrationLanguage
  ) => Promise<boolean>;
}

// How long after the last rejected file a drop is treated as over: long enough that a slow
// directory walk stays one burst, short enough that a second drop is reported again.
export const DROP_REJECTION_LATCH_MS = 2000;

// Falls back to a count when the picker reported no path, so the row always says something
// was picked.
function describeFolderSelection(files: readonly NzUploadFile[]): string {
  const count = `${files.length} file${files.length === 1 ? "" : "s"}`;
  return folderRootName(pickedFilePath(files[0])) ?? count;
}

/**
 * The "AI Generate Workflow from Source Code" modal body: a tab per accepted input kind
 * (Jupyter notebook, Python script, Python folder, and R when enabled), each with its own diagram, description and
 * upload control, over a shared model dropdown and footer.
 *
 * On Submit it hands the selection and model to requestImport and shows a loading state until
 * it resolves; the opener dispatches on a folder's list shape, or on a single file's extension.
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
    NgTemplateOutlet,
    ReactiveFormsModule,
    NzFormModule,
    NzSelectModule,
    NzSpinComponent,
    NzUploadComponent,
    NzButtonComponent,
    NzIconDirective,
    NzTabsComponent,
    NzTabComponent,
    NzTabDirective,
    NzSegmentedComponent,
  ],
})
export class NotebookImportModalComponent implements OnDestroy {
  private readonly fb = inject(FormBuilder);
  private readonly modalRef = inject(NzModalRef);
  private readonly notebookMigrationService = inject(NotebookMigrationService);
  private readonly notificationService = inject(NotificationService);
  private readonly data: NotebookImportModalData = inject(NZ_MODAL_DATA);
  private readonly config = inject(GuiConfigService);

  public readonly importForm: FormGroup = this.fb.group({
    file: [null, Validators.required],
    model: ["", Validators.required],
  });

  // Drives the three model-dropdown states: pending (loading), a non-empty list (selectable),
  // and an empty list (no models available, e.g. the fetch failed or the feature is off).
  public readonly models$: Observable<{ name: string }[]> = this.notebookMigrationService.getAvailableModels();

  // The pane cross-fade reads as a flicker on a dense form, so only the ink bar animates.
  // Held as a field rather than an inline literal so the binding keeps a stable reference.
  public readonly tabAnimation = { inkBar: true, tabPane: false };

  // Tab order: 0 = Jupyter notebook, 1 = Python file, 2 = Python folder, 3 = R.
  public selectedTabIndex = 0;

  public readonly rInputKindOptions = [
    { label: "Notebook", value: "notebook" },
    { label: "File", value: "file" },
  ];
  // A control rather than a field so the switch shows the right option when the R tab re-renders.
  public readonly rInputKind = new FormControl<"notebook" | "file">("notebook", { nonNullable: true });

  public get rMigrationEnabled(): boolean {
    return this.config.env.rNotebookMigrationEnabled;
  }

  public get isFolderTab(): boolean {
    return this.selectedTabIndex === 2;
  }

  public get language(): MigrationLanguage {
    return this.selectedTabIndex === 3 ? "r" : "python";
  }

  /** What the upload row shows once something is picked, or null while nothing is. */
  public get selectionSummary(): string | null {
    const value = this.importForm.get("file")?.value;
    if (Array.isArray(value)) {
      return value.length === 0 ? null : `Selected folder: ${describeFolderSelection(value)}`;
    }
    return value?.name ? `Selected file: ${value.name}` : null;
  }

  /**
   * Switching tabs drops the selected file: the tabs accept different inputs, so carrying a
   * selection across would leave, say, an .ipynb staged under the Python folder tab.
   * The model stays selected because it applies to any of them.
   */
  public onTabChange(index: number): void {
    this.selectedTabIndex = index;
    this.clearSelection();
  }

  public clearSelection(): void {
    const fileControl = this.importForm.get("file");
    fileControl?.reset(null);
    fileControl?.updateValueAndValidity();
  }

  // ng-zorro delivers a dropped directory one pathless file at a time, so accepting one would
  // convert a single arbitrary file as the whole project. Latched for the burst, not rate-limited,
  // because walking a large tree outlasts any rate worth picking.
  private dropRejectionTimer: ReturnType<typeof setTimeout> | null = null;

  private rejectFolderDrop(): void {
    if (this.dropRejectionTimer === null) {
      this.notificationService.error("Drop is not supported for folders. Use the button to pick a folder.");
    } else {
      clearTimeout(this.dropRejectionTimer);
    }
    this.dropRejectionTimer = setTimeout(() => (this.dropRejectionTimer = null), DROP_REJECTION_LATCH_MS);
  }

  public beforeUpload = (file: NzUploadFile, fileList: NzUploadFile[]) => {
    // Only the directory picker reports a path. No path on the folder tab means this came from a
    // drop, which cannot be assembled into a folder, so nothing is staged.
    if (this.isFolderTab && pickedFilePath(file) === "") {
      this.rejectFolderDrop();
      return false;
    }

    // A directory pick calls this once per file with the same list, so the folder tab takes the
    // list and the guard below keeps a large folder from re-validating thousands of times.
    const selection = this.isFolderTab ? fileList : file;
    const control = this.importForm.get("file");
    if (control?.value !== selection) {
      this.importForm.patchValue({ file: selection });
      control?.markAsDirty();
      control?.updateValueAndValidity();
    }
    return false; // prevent auto upload
  };

  public onCancel(): void {
    this.modalRef.close();
  }

  // True while generation runs: guards against a second submit and drives the loading overlay.
  public isSubmitting = false;
  private startTime: number | null = null;
  private timerHandle: ReturnType<typeof setInterval> | null = null;

  public ngOnDestroy(): void {
    this.stopTimer();
    if (this.dropRejectionTimer !== null) {
      clearTimeout(this.dropRejectionTimer);
    }
  }

  public get formattedElapsedTime(): string {
    const diffMs = this.startTime === null ? 0 : Date.now() - this.startTime;
    const totalSeconds = Math.floor(diffMs / 1000);
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    return `${minutes}:${seconds.toString().padStart(2, "0")}`;
  }

  // Empty body on purpose: the zone-patched event firing is itself what repaints the stopwatch,
  // so it catches up when the user returns to a backgrounded tab. Same reason as the timer below.
  @HostListener("document:visibilitychange")
  public onVisibilityChange(): void {}

  private startTimer(): void {
    this.stopTimer();
    this.startTime = Date.now();
    // Empty body: elapsed is computed from startTime; the zone-patched tick just triggers a repaint.
    this.timerHandle = setInterval(() => {}, 1000);
  }

  private stopTimer(): void {
    if (this.timerHandle !== null) {
      clearInterval(this.timerHandle);
      this.timerHandle = null;
    }
  }

  public async onSubmit(): Promise<void> {
    if (this.isSubmitting || !this.importForm.valid) return;
    const selection: NzUploadFile | NzUploadFile[] = this.importForm.get("file")?.value;
    const model: string = this.importForm.get("model")?.value;
    this.isSubmitting = true;
    this.startTimer();
    this.modalRef.updateConfig({ nzClosable: false, nzMaskClosable: false, nzKeyboard: false });
    try {
      // Close only on success, so a failure leaves the modal open with the selection preserved.
      if (await this.data.requestImport(selection, model, this.language)) {
        this.modalRef.close();
        return;
      }
      this.modalRef.updateConfig({ nzClosable: true, nzMaskClosable: true, nzKeyboard: true });
    } finally {
      this.isSubmitting = false;
      this.stopTimer();
    }
  }
}

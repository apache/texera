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

import {
  Component,
  ElementRef,
  EventEmitter,
  Input,
  OnChanges,
  OnInit,
  Output,
  SimpleChanges,
  ViewChild,
} from "@angular/core";
import { DatasetService } from "../../../../../service/user/dataset/dataset.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NotificationService } from "../../../../../../common/service/notification/notification.service";
import { switchMap } from "rxjs/operators";
import { of } from "rxjs";

@UntilDestroy()
@Component({
  selector: "texera-user-dataset-readme",
  templateUrl: "./user-dataset-readme.component.html",
  styleUrls: ["./user-dataset-readme.component.scss"],
})
export class UserDatasetReadmeComponent implements OnInit, OnChanges {
  @Input() did: number | undefined;
  @Input() dvid: number | undefined;
  @Input() selectedVersion: any | undefined;
  @Input() datasetName: string = "";
  @Input() isMaximized: boolean = false;
  @Input() userHasWriteAccess: boolean = false;
  @Input() isLogin: boolean = true;
  @Output() userMakeChanges = new EventEmitter<void>();

  @ViewChild("markdownTextarea") markdownTextarea!: ElementRef<HTMLTextAreaElement>;

  public readmeContent: string = "";
  public isEditing: boolean = false;
  public readmeExists: boolean = false;
  public isLoading: boolean = false;
  public editingContent: string = "";

  private readonly README_FILE_PATH = "README.md";

  constructor(
    private datasetService: DatasetService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    if (this.dvid && this.datasetName && this.selectedVersion) {
      this.loadReadme();
    }
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (
      (changes["dvid"] || changes["datasetName"] || changes["selectedVersion"]) &&
      this.dvid &&
      this.datasetName &&
      this.selectedVersion
    ) {
      this.loadReadme();
    }
  }

  private loadReadme(): void {
    if (!this.did || !this.dvid || !this.datasetName || !this.selectedVersion) return;

    this.isLoading = true;

    const fullPath = `/texera/${this.datasetName}/${this.selectedVersion.name}/${this.README_FILE_PATH}`;

    this.datasetService
      .retrieveDatasetVersionSingleFile(fullPath, this.isLogin)
      .pipe(
        switchMap(blob => {
          return new Promise<string>((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = () => resolve(reader.result as string);
            reader.onerror = () => reject(reader.error);
            reader.readAsText(blob);
          });
        }),
        untilDestroyed(this)
      )
      .subscribe({
        next: content => {
          this.isLoading = false;
          this.readmeExists = true;
          this.readmeContent = content;
          this.editingContent = content;
        },
        error: () => {
          this.isLoading = false;
          this.readmeExists = false;
          this.readmeContent = "";
          this.editingContent = "";
          console.log("README not found or error loading");
        },
      });
  }

  public createReadme(): void {
    if (!this.did || !this.userHasWriteAccess) return;

    const initialContent = "# Dataset README\n\nDescribe your dataset here...";
    this.uploadReadmeContent(initialContent, "README created successfully");
  }

  public startEditing(): void {
    if (!this.userHasWriteAccess) return;
    this.editingContent = this.readmeContent;
    this.isEditing = true;
  }

  public cancelEditing(): void {
    this.editingContent = this.readmeContent;
    this.isEditing = false;
  }

  public onEditorKeydown(event: KeyboardEvent): void {
    if ((event.ctrlKey || event.metaKey) && event.key === "s") {
      event.preventDefault();
      this.saveReadme();
    }

    if (event.key === "Tab") {
      event.preventDefault();
      const textarea = event.target as HTMLTextAreaElement;
      const start = textarea.selectionStart;
      const end = textarea.selectionEnd;

      const value = textarea.value;
      textarea.value = value.substring(0, start) + "  " + value.substring(end);

      textarea.selectionStart = textarea.selectionEnd = start + 2;

      this.editingContent = textarea.value;
    }
  }

  public insertMarkdown(before: string, after: string = "", placeholder: string = ""): void {
    if (!this.markdownTextarea) return;

    const textarea = this.markdownTextarea.nativeElement;
    const start = textarea.selectionStart;
    const end = textarea.selectionEnd;
    const selectedText = textarea.value.substring(start, end);

    let insertText: string;
    if (selectedText) {
      insertText = before + selectedText + after;
    } else {
      insertText = before + placeholder + after;
    }

    // Trigger input event to preserve undo
    textarea.focus();
    document.execCommand("insertText", false, insertText);

    this.editingContent = textarea.value;

    // Update cursor position
    setTimeout(() => {
      if (selectedText) {
        textarea.selectionStart = start + before.length;
        textarea.selectionEnd = start + before.length + selectedText.length;
      } else {
        textarea.selectionStart = textarea.selectionEnd = start + before.length;
      }
      textarea.focus();
    });
  }

  public saveReadme(): void {
    if (!this.did || !this.userHasWriteAccess) return;

    if (this.editingContent === this.readmeContent) {
      this.notificationService.warning("No changes detected in README content");
      return;
    }

    this.uploadReadmeContent(this.editingContent, "README updated successfully");
  }

  public deleteReadme(): void {
    if (!this.did || !this.userHasWriteAccess) return;

    this.datasetService
      .deleteDatasetFile(this.did, this.README_FILE_PATH)
      .pipe(
        // After deleting, create a new version to save changes.
        switchMap(() => this.datasetService.createDatasetVersion(this.did!, "Deleted README.md")),
        untilDestroyed(this)
      )
      .subscribe({
        next: () => {
          this.readmeExists = false;
          this.readmeContent = "";
          this.editingContent = "";
          this.isEditing = false;
          this.notificationService.success("README deleted successfully");

          // Emit the change to refresh file version screen
          this.userMakeChanges.emit();
        },
        error: (error: unknown) => {
          console.error("Error deleting README:", error);
          this.notificationService.error("Failed to delete README");
        },
      });
  }

  private uploadReadmeContent(content: string, successMessage: string): void {
    if (!this.did) return;

    this.datasetService
      .getDataset(this.did, this.isLogin)
      .pipe(
        switchMap(dashboardDataset => {
          const datasetName = dashboardDataset.dataset.name;

          const readmeBlob = new Blob([content], { type: "text/markdown" });
          const readmeFile = new File([readmeBlob], this.README_FILE_PATH, { type: "text/markdown" });

          return this.datasetService.multipartUpload(datasetName, this.README_FILE_PATH, readmeFile);
        }),
        // After upload completes, automatically create a new version
        switchMap(progress => {
          if (progress.status === "finished") {
            const versionMessage = successMessage.includes("created") ? "Created README.md" : "Updated README.md";
            return this.datasetService.createDatasetVersion(this.did!, versionMessage);
          }
          return of(progress);
        }),
        untilDestroyed(this)
      )
      .subscribe({
        next: result => {
          if (result && typeof result === "object" && "dvid" in result) {
            this.readmeExists = true;
            this.readmeContent = content;
            this.isEditing = false;
            this.notificationService.success(successMessage);

            // Emit the change to refresh file version screen
            this.userMakeChanges.emit();
          }
        },
        error: (error: unknown) => {
          console.error("Error uploading README:", error);
          this.notificationService.error("Failed to save README");
        },
      });
  }
}

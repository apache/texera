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

import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { AfterViewInit, Component, ViewChild } from "@angular/core";
import { UserService } from "../../../../common/service/user/user.service";
import { Router } from "@angular/router";
import { SearchService } from "../../../service/user/search.service";
import { DatasetService } from "../../../service/user/dataset/dataset.service";
import { SortMethod } from "../../../type/sort-method";
import { DashboardEntry } from "../../../type/dashboard-entry";
import { SearchResultsComponent } from "../search-results/search-results.component";
import { FiltersComponent } from "../filters/filters.component";
import { firstValueFrom, lastValueFrom } from "rxjs";
import { DASHBOARD_USER_DATASET } from "../../../../app-routing.constant";
import { NzModalService } from "ng-zorro-antd/modal";
import { UserDatasetVersionCreatorComponent } from "./user-dataset-explorer/user-dataset-version-creator/user-dataset-version-creator.component";
import { DashboardDataset } from "../../../type/dashboard-dataset.interface";
import { NzMessageService } from "ng-zorro-antd/message";
import { map, tap } from "rxjs/operators";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { FiltersInstructionsComponent } from "../filters-instructions/filters-instructions.component";
import { NzSelectComponent } from "ng-zorro-antd/select";
import { FormsModule } from "@angular/forms";
import { CommonModule } from "@angular/common";
import { NotificationService } from "../../../../common/service/notification/notification.service";

@UntilDestroy()
@Component({
  selector: "texera-dataset-section",
  templateUrl: "user-dataset.component.html",
  styleUrls: ["user-dataset.component.scss"],
  imports: [
    CommonModule,
    NzCardComponent,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    FiltersComponent,
    FiltersInstructionsComponent,
    NzSelectComponent,
    FormsModule,
    SearchResultsComponent,
  ],
})
export class UserDatasetComponent implements AfterViewInit {
  public sortMethod = SortMethod.EditTimeDesc;
  lastSortMethod: SortMethod | null = null;
  public isLogin = this.userService.isLogin();
  public currentUid = this.userService.getCurrentUser()?.uid;
  public hasMismatch = false; // Display warning when there are mismatched datasets

  // URL import state
  public urlImportInput = "";
  public isImporting = false;
  public isDragging = false;

  private _searchResultsComponent?: SearchResultsComponent;
  @ViewChild(SearchResultsComponent) get searchResultsComponent(): SearchResultsComponent {
    if (this._searchResultsComponent) {
      return this._searchResultsComponent;
    }
    throw new Error("Property cannot be accessed before it is initialized.");
  }

  set searchResultsComponent(value: SearchResultsComponent) {
    this._searchResultsComponent = value;
  }

  private _filters?: FiltersComponent;
  @ViewChild(FiltersComponent) get filters(): FiltersComponent {
    if (this._filters) {
      return this._filters;
    }
    throw new Error("Property cannot be accessed before it is initialized.");
  }

  set filters(value: FiltersComponent) {
    value.masterFilterListChange.pipe(untilDestroyed(this)).subscribe({ next: () => this.search() });
    this._filters = value;
  }

  private masterFilterList: ReadonlyArray<string> | null = null;
  constructor(
    private modalService: NzModalService,
    private userService: UserService,
    private router: Router,
    private searchService: SearchService,
    private datasetService: DatasetService,
    private message: NzMessageService,
    private notificationService: NotificationService
  ) {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.isLogin = this.userService.isLogin();
        this.currentUid = this.userService.getCurrentUser()?.uid;
      });
  }

  ngAfterViewInit() {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => this.search());
  }

  /*
   * Executes a dataset search with filtering, sorting.
   *
   * Parameters:
   * - filterScope = "all" | "public" | "private" - Determines visibility scope for search:
   *  - "all": includes all datasets, public and private
   *  - "public": limits the search to public datasets
   *  - "private": limits the search to dataset where the user has direct access rights.
   */
  async search(forced: Boolean = false, filterScope: "all" | "public" | "private" = "private"): Promise<void> {
    const sameList =
      this.masterFilterList !== null &&
      this.filters.masterFilterList.length === this.masterFilterList.length &&
      this.filters.masterFilterList.every((v, i) => v === this.masterFilterList![i]);
    if (!forced && sameList && this.sortMethod === this.lastSortMethod) {
      // If the filter lists are the same, do no make the same request again.
      return;
    }
    this.lastSortMethod = this.sortMethod;
    this.masterFilterList = this.filters.masterFilterList;
    if (!this.searchResultsComponent) {
      throw new Error("searchResultsComponent is undefined.");
    }
    let filterParams = this.filters.getSearchFilterParameters();

    // if the filter requires only public datasets, the public search should be invoked, and the search method should
    // set the isLogin parameter to false in this case
    const isLogin = filterScope === "public" ? false : this.isLogin;
    const includePublic = filterScope === "all" || filterScope === "public";

    this.searchResultsComponent.reset((start, count) => {
      return firstValueFrom(
        this.searchService
          .executeSearch(
            this.filters.getSearchKeywords(),
            filterParams,
            start,
            count,
            "dataset",
            this.sortMethod,
            isLogin,
            includePublic
          )
          .pipe(
            tap(({ hasMismatch }) => {
              this.hasMismatch = hasMismatch ?? false;
              if (this.hasMismatch) {
                this.message.warning(
                  "There is a mismatch between some datasets in the database and LakeFS. Only matched datasets are displayed.",
                  { nzDuration: 4000 }
                );
              }
            }),
            map(({ entries, more }) => ({ entries, more }))
          )
      );
    });
    await this.searchResultsComponent.loadMore();
  }

  public onClickOpenDatasetAddComponent(): void {
    const modal = this.modalService.create({
      nzTitle: "Create New Dataset",
      nzContent: UserDatasetVersionCreatorComponent,
      nzFooter: null,
      nzData: {
        isCreatingVersion: false,
      },
      nzBodyStyle: {
        resize: "both",
        overflow: "auto",
        minHeight: "200px",
        minWidth: "550px",
        maxWidth: "90vw",
        maxHeight: "80vh",
      },
      nzWidth: "fit-content",
    });
    // Handle the selection from the modal
    modal.afterClose.pipe(untilDestroyed(this)).subscribe(result => {
      if (result != null) {
        const dashboardDataset: DashboardDataset = result as DashboardDataset;
        this.router.navigate([`${DASHBOARD_USER_DATASET}/${dashboardDataset.dataset.did}`]);
      }
    });
  }

  public deleteDataset(entry: DashboardEntry): void {
    if (entry.dataset.dataset.did == undefined) {
      return;
    }
    this.datasetService
      .deleteDatasets(entry.dataset.dataset.did)
      .pipe(untilDestroyed(this))
      .subscribe(_ => {
        this.searchResultsComponent.entries = this.searchResultsComponent.entries.filter(
          datasetEntry => datasetEntry.dataset.dataset.did !== entry.dataset.dataset.did
        );
      });
  }

  // ============================================================
  // Import-from-URL flow
  // ============================================================

  public async onClickImportFromUrl(): Promise<void> {
    const rawUrl = (this.urlImportInput || "").trim();
    if (!rawUrl || this.isImporting) return;

    let parsed: URL;
    try {
      parsed = new URL(rawUrl);
    } catch {
      this.notificationService.error("Please enter a valid URL.");
      return;
    }
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      this.notificationService.error("Only http:// and https:// URLs are supported.");
      return;
    }

    this.isImporting = true;
    const messageId = this.message.loading("Importing from URL…", { nzDuration: 0 }).messageId;

    try {
      const fetched = await firstValueFrom(this.datasetService.fetchUrlAsCsv(rawUrl));
      const blob = new Blob([fetched.csv], { type: "text/csv" });
      const file = new File([blob], fetched.filename, { type: "text/csv" });

      const datasetName = this.deriveDatasetName(fetched.filename);
      await this.createDatasetFromFile(datasetName, "Imported from " + rawUrl, file);

      this.message.remove(messageId);
      this.notificationService.success(
        `Imported "${fetched.filename}" (${fetched.rows} rows, ${fetched.columns.length} columns).`
      );
      this.urlImportInput = "";
      this.refreshAfterImport();
    } catch (err: any) {
      this.message.remove(messageId);
      const errMsg = err?.error?.error || err?.error?.message || err?.message || "Unknown error";
      this.notificationService.error(`Import failed: ${errMsg}`);
    } finally {
      this.isImporting = false;
    }
  }

  // ============================================================
  // Local file drop flow (CSV / JSON / XLSX direct; SQLite stub)
  // ============================================================

  public onDragEnter(event: DragEvent): void {
    event.preventDefault();
    event.stopPropagation();
    this.isDragging = true;
  }

  public onDragOver(event: DragEvent): void {
    event.preventDefault();
    event.stopPropagation();
    this.isDragging = true;
  }

  public onDragLeave(event: DragEvent): void {
    event.preventDefault();
    event.stopPropagation();
    this.isDragging = false;
  }

  public onDrop(event: DragEvent): void {
    event.preventDefault();
    event.stopPropagation();
    this.isDragging = false;
    const files = event.dataTransfer?.files;
    if (files && files.length > 0) {
      this.handleLocalFile(files[0]);
    }
  }

  public onFileSelected(event: Event): void {
    const input = event.target as HTMLInputElement;
    if (input.files && input.files.length > 0) {
      this.handleLocalFile(input.files[0]);
    }
    // reset so re-selecting the same file fires change again
    input.value = "";
  }

  private async handleLocalFile(file: File): Promise<void> {
    const lower = file.name.toLowerCase();
    if (lower.endsWith(".sqlite") || lower.endsWith(".db")) {
      // SQLite path requires the user to pick a table; route to backend importer.
      await this.importSqliteFile(file);
      return;
    }
    if (
      lower.endsWith(".csv") ||
      lower.endsWith(".json") ||
      lower.endsWith(".xlsx") ||
      lower.endsWith(".tsv") ||
      lower.endsWith(".txt")
    ) {
      await this.importTabularFile(file);
      return;
    }
    this.notificationService.warning(
      "Unsupported file type. Use .sqlite, .db, .csv, .json, .xlsx, .tsv, or .txt."
    );
  }

  private async importTabularFile(file: File): Promise<void> {
    if (this.isImporting) return;
    this.isImporting = true;
    const messageId = this.message.loading(`Uploading ${file.name}…`, { nzDuration: 0 }).messageId;
    try {
      const datasetName = this.deriveDatasetName(file.name);
      await this.createDatasetFromFile(datasetName, `Imported from local file ${file.name}`, file);
      this.message.remove(messageId);
      this.notificationService.success(`Imported "${file.name}".`);
      this.refreshAfterImport();
    } catch (err: any) {
      this.message.remove(messageId);
      const errMsg = err?.error?.error || err?.error?.message || err?.message || "Unknown error";
      this.notificationService.error(`Upload failed: ${errMsg}`);
    } finally {
      this.isImporting = false;
    }
  }

  private async importSqliteFile(file: File): Promise<void> {
    if (this.isImporting) return;
    this.isImporting = true;
    const messageId = this.message.loading(`Reading ${file.name}…`, { nzDuration: 0 }).messageId;
    try {
      const tables = await firstValueFrom(this.datasetService.listSqliteTables(file));
      this.message.remove(messageId);
      if (!tables.tables || tables.tables.length === 0) {
        this.notificationService.warning(`${file.name} contains no readable tables.`);
        return;
      }

      // Minimal UX: prompt for which table(s) to import. Single-table files import directly.
      let chosenTables: string[];
      if (tables.tables.length === 1) {
        chosenTables = [tables.tables[0].name];
      } else {
        const summary = tables.tables
          .map((t, i) => `${i + 1}. ${t.name} (${t.rowCount} rows, ${t.columns.length} cols)`)
          .join("\n");
        const answer = window.prompt(
          `Found ${tables.tables.length} tables in ${file.name}:\n\n${summary}\n\nEnter table names to import (comma-separated), or leave empty for all:`,
          ""
        );
        if (answer === null) {
          this.notificationService.info("Import cancelled.");
          return;
        }
        const trimmed = answer.trim();
        if (trimmed.length === 0) {
          chosenTables = tables.tables.map(t => t.name);
        } else {
          const wanted = new Set(trimmed.split(",").map(s => s.trim()).filter(Boolean));
          chosenTables = tables.tables.filter(t => wanted.has(t.name)).map(t => t.name);
          if (chosenTables.length === 0) {
            this.notificationService.warning("None of those table names were found.");
            return;
          }
        }
      }

      const baseDatasetName = this.deriveDatasetName(file.name);
      for (const tableName of chosenTables) {
        const exportId = this.message.loading(`Exporting "${tableName}"…`, { nzDuration: 0 }).messageId;
        try {
          const exported = await firstValueFrom(
            this.datasetService.exportSqliteTable(tables.fileHandle, tableName)
          );
          const blob = new Blob([exported.csv], { type: "text/csv" });
          const csvFile = new File([blob], `${tableName}.csv`, { type: "text/csv" });
          const dsName =
            chosenTables.length === 1 ? baseDatasetName : `${baseDatasetName}-${this.slug(tableName)}`;
          await this.createDatasetFromFile(
            dsName,
            `Imported from SQLite ${file.name} (table ${tableName})`,
            csvFile
          );
          this.message.remove(exportId);
          this.notificationService.success(
            `Imported "${tableName}" (${exported.rows} rows, ${exported.columns.length} columns).`
          );
        } catch (e: any) {
          this.message.remove(exportId);
          const errMsg = e?.error?.error || e?.error?.message || e?.message || "Unknown error";
          this.notificationService.error(`Failed to import table "${tableName}": ${errMsg}`);
        }
      }
      this.refreshAfterImport();
    } catch (err: any) {
      this.message.remove(messageId);
      const errMsg = err?.error?.error || err?.error?.message || err?.message || "Unknown error";
      this.notificationService.error(`Failed to read SQLite: ${errMsg}`);
    } finally {
      this.isImporting = false;
    }
  }

  // ============================================================
  // Helpers
  // ============================================================

  /**
   * Reload the page after a successful import. The brief delay lets the success
   * toast finish rendering before the page swap. Paired with the
   * DatasetSearchQueryBuilder.scala fix that no longer drops entries on LakeFS
   * ApiException, the new dataset will appear in the list after the reload.
   */
  private refreshAfterImport(): void {
    this.lastCreatedDid = null;
    setTimeout(() => window.location.reload(), 800);
  }

  /**
   * Derive a dataset name from the source filename: drop the extension, slug it,
   * and append a short hh:mm:ss-style suffix to avoid the "name already exists"
   * error if the same file is imported twice in a session.
   */
  private deriveDatasetName(filename: string): string {
    const base = filename.replace(/\.[^.]+$/, "");
    const baseSlug = this.slug(base) || "imported";
    const d = new Date();
    const hhmmss =
      String(d.getHours()).padStart(2, "0") +
      String(d.getMinutes()).padStart(2, "0") +
      String(d.getSeconds()).padStart(2, "0");
    return `${baseSlug}-${hhmmss}`;
  }

  private slug(value: string): string {
    return value
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "");
  }

  /** Tracks the most recently created dataset so we can route to its detail page after import. */
  private lastCreatedDid: number | null = null;

  /**
   * Create a new dataset, push the file via multipart upload, and commit an initial version.
   * Shared by URL, SQLite, and direct-file import paths.
   *
   * Three sequential backend calls, each logged so a Network-tab/console trace
   * pinpoints which one failed if the dataset doesn't show up.
   */
  private async createDatasetFromFile(datasetName: string, description: string, file: File): Promise<void> {
    console.log("[dataset-import] step 1/3 createDataset", { datasetName, fileSize: file.size });
    let dashboardDataset: DashboardDataset;
    try {
      dashboardDataset = await firstValueFrom(
        this.datasetService.createDataset({
          name: datasetName,
          description,
          isPublic: false,
          isDownloadable: false,
          did: undefined,
          ownerUid: undefined,
          storagePath: undefined,
          creationTime: undefined,
          coverImage: undefined,
        })
      );
    } catch (e) {
      console.error("[dataset-import] createDataset FAILED", e);
      throw e;
    }

    const ownerEmail = dashboardDataset.ownerEmail;
    const did = dashboardDataset.dataset.did;
    console.log("[dataset-import] step 1/3 OK", { did, ownerEmail });
    if (!did) {
      throw new Error("Dataset created but missing did.");
    }
    this.lastCreatedDid = did;

    console.log("[dataset-import] step 2/3 multipartUpload", { filePath: file.name });
    const partSize = 8 * 1024 * 1024;
    const concurrency = 4;
    try {
      await lastValueFrom(
        this.datasetService.multipartUpload(
          ownerEmail,
          datasetName,
          file.name,
          file,
          partSize,
          concurrency,
          false
        )
      );
    } catch (e) {
      console.error("[dataset-import] multipartUpload FAILED", e);
      throw e;
    }
    console.log("[dataset-import] step 2/3 OK");

    console.log("[dataset-import] step 3/3 createDatasetVersion");
    try {
      await firstValueFrom(this.datasetService.createDatasetVersion(did, "Initial import"));
    } catch (e) {
      console.error("[dataset-import] createDatasetVersion FAILED", e);
      throw e;
    }
    console.log("[dataset-import] step 3/3 OK — dataset is ready", { datasetName, did });

    // Verify the dataset is actually queryable. /api/dataset/list reads from DB
    // (no LakeFS check); the search listing in the UI uses /api/dashboard/search
    // which calls LakeFSStorageClient.retrieveRepositorySize and silently drops
    // any dataset whose LakeFS repo throws — that's the most common reason a
    // just-created dataset is missing from the list.
    try {
      const accessible = await firstValueFrom(this.datasetService.retrieveAccessibleDatasets());
      const found = accessible.find(d => d.dataset.did === did);
      console.log("[dataset-import] verify via /api/dataset/list:", found ? "FOUND" : "NOT FOUND in DB", {
        did,
        datasetName,
        totalAccessible: accessible.length,
        match: found,
      });
    } catch (e) {
      console.warn("[dataset-import] verification list call failed (non-fatal)", e);
    }
  }
}

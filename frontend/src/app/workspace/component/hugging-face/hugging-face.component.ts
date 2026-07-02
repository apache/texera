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

import { Component, OnInit, OnDestroy, ChangeDetectorRef } from "@angular/core";
import { CommonModule } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { FieldType, FieldTypeConfig, FormlyModule } from "@ngx-formly/core";
import { HttpClient } from "@angular/common/http";
import { NzSelectModule } from "ng-zorro-antd/select";
import { NzInputModule } from "ng-zorro-antd/input";
import { NzSpinModule } from "ng-zorro-antd/spin";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzIconModule } from "ng-zorro-antd/icon";
import { AppSettings } from "../../../common/app-setting";
import { of, Subject, Subscription } from "rxjs";
import { catchError, debounceTime, finalize, switchMap, takeUntil } from "rxjs/operators";

export interface HuggingFaceModelOption {
  id: string;
  label: string;
  pipeline_tag?: string;
  downloads?: number;
  likes?: number;
}

export interface HuggingFaceTaskOption {
  tag: string;
  label: string;
}

// ── Static fallback task list (used when the dynamic fetch fails) ──
export const STATIC_TASK_OPTIONS: HuggingFaceTaskOption[] = [
  { tag: "text-generation", label: "Text Generation" },
  { tag: "automatic-speech-recognition", label: "Automatic Speech Recognition" },
  { tag: "audio-classification", label: "Audio Classification" },
  { tag: "text-classification", label: "Text Classification" },
  { tag: "text-to-speech", label: "Text to Speech" },
  { tag: "token-classification", label: "Token Classification" },
  { tag: "question-answering", label: "Question Answering" },
  { tag: "table-question-answering", label: "Table Question Answering" },
  { tag: "zero-shot-classification", label: "Zero-Shot Classification" },
  { tag: "translation", label: "Translation" },
  { tag: "summarization", label: "Summarization" },
  { tag: "feature-extraction", label: "Feature Extraction" },
  { tag: "fill-mask", label: "Fill-Mask" },
  { tag: "sentence-similarity", label: "Sentence Similarity" },
  { tag: "text-ranking", label: "Text Ranking" },
  { tag: "image-classification", label: "Image Classification" },
  { tag: "object-detection", label: "Object Detection" },
  { tag: "image-segmentation", label: "Image Segmentation" },
  { tag: "image-to-text", label: "Image to Text" },
  { tag: "visual-question-answering", label: "Visual Question Answering" },
  { tag: "document-question-answering", label: "Document Question Answering" },
  { tag: "zero-shot-image-classification", label: "Zero-Shot Image Classification" },
];

const PAGE_SIZE = 50;

const TRUNCATED_HEADER = "X-Texera-Truncated";

// ── Module-level caches (reused across component instances) ──
const allModelsByTag: Map<string, HuggingFaceModelOption[]> = new Map();
const truncatedByTag: Set<string> = new Set();
const inFlightByTag: Map<string, Subscription> = new Map();
const errorByTag: Map<string, string> = new Map();

let cachedTaskOptions: HuggingFaceTaskOption[] | null = null;
let tasksFetchSubscription: Subscription | null = null;
let tasksFetchError: string | null = null;

/** Clear all cached data (useful for tests or manual invalidation). */
export function invalidateHuggingFaceModelCache(): void {
  allModelsByTag.clear();
  truncatedByTag.clear();
  errorByTag.clear();
  inFlightByTag.forEach(sub => sub.unsubscribe());
  inFlightByTag.clear();
  cachedTaskOptions = null;
  tasksFetchError = null;
  tasksFetchSubscription?.unsubscribe();
  tasksFetchSubscription = null;
}

@Component({
  selector: "texera-hugging-face-model-select",
  templateUrl: "./hugging-face.component.html",
  styleUrls: ["hugging-face.component.scss"],
  imports: [
    CommonModule,
    FormsModule,
    NzSelectModule,
    NzInputModule,
    NzSpinModule,
    NzButtonModule,
    NzIconModule,
    FormlyModule,
  ],
})
export class HuggingFaceComponent extends FieldType<FieldTypeConfig> implements OnInit, OnDestroy {
  private readonly taskScopedKeys = [
    "modelId",
    "promptColumn",
    "imageInput",
    "audioInput",
    "inputImageColumn",
    "inputAudioColumn",
    "candidateLabels",
    "sentencesColumn",
    "contextColumn",
    "systemPrompt",
    "maxNewTokens",
    "temperature",
  ] as const;
  private readonly taskStateByTag = new Map<string, Partial<Record<(typeof this.taskScopedKeys)[number], unknown>>>();
  // ── Task state ──
  taskOptions: HuggingFaceTaskOption[] = cachedTaskOptions ?? STATIC_TASK_OPTIONS;
  selectedTaskTag = "text-generation";
  tasksLoading = false;
  tasksError: string | null = null;

  // ── All models for the current task (fetched once from backend, cached) ──
  private allModels: HuggingFaceModelOption[] = [];

  // ── Displayed state ──
  pagedModels: HuggingFaceModelOption[] = [];
  currentPage = 0;
  totalPages = 0;

  loading = false;
  errorMessage: string | null = null;

  // ── Truncation notice ──
  truncated = false;

  // ── Search state ──
  searchText = "";
  searchLoading = false;
  private filteredModels: HuggingFaceModelOption[] | null = null;
  private readonly searchSubject$ = new Subject<string>();
  private searchSubscription: Subscription | null = null;

  private readonly destroy$ = new Subject<void>();
  private subscription: Subscription | null = null;
  private taskPollInterval: ReturnType<typeof setInterval> | null = null;
  private modelPollInterval: ReturnType<typeof setInterval> | null = null;
  private initTimeout: ReturnType<typeof setTimeout> | null = null;

  constructor(
    private http: HttpClient,
    private cdr: ChangeDetectorRef
  ) {
    super();
  }

  ngOnInit(): void {
    const savedTag = this.getCurrentTaskTag();
    this.selectedTaskTag = savedTag ?? this.selectedTaskTag;
    this.syncTaskSelection(this.selectedTaskTag, false);
    this.loadTasks();
    this.loadAllModels();
    this.setupServerSearch();
    // Formly can attach sibling controls after this field initializes.
    // Re-sync once the control tree settles so a fresh operator starts in a valid task state.
    this.initTimeout = setTimeout(
      () => this.syncTaskSelection(this.getCurrentTaskTag() ?? this.selectedTaskTag, false),
      0
    );
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    this.subscription?.unsubscribe();
    this.searchSubscription?.unsubscribe();
    this.searchSubject$.complete();
    if (this.taskPollInterval !== null) {
      clearInterval(this.taskPollInterval);
    }
    if (this.modelPollInterval !== null) {
      clearInterval(this.modelPollInterval);
    }
    if (this.initTimeout !== null) {
      clearTimeout(this.initTimeout);
    }
  }

  // ── Task loading ──

  /**
   * Fetch available pipeline tags from the backend, which proxies HuggingFace's /api/tasks.
   * Falls back to STATIC_TASK_OPTIONS if the fetch fails.
   */
  private loadTasks(): void {
    // Already fetched and cached
    if (cachedTaskOptions !== null) {
      this.taskOptions = cachedTaskOptions;
      return;
    }

    // Previous fetch errored — show static list, don't retry automatically
    if (tasksFetchError !== null) {
      this.tasksError = tasksFetchError;
      this.taskOptions = STATIC_TASK_OPTIONS;
      return;
    }

    // Another component instance already has a fetch in flight — wait for it
    if (tasksFetchSubscription !== null) {
      this.tasksLoading = true;
      if (this.taskPollInterval !== null) clearInterval(this.taskPollInterval);
      const poll = setInterval(() => {
        if (cachedTaskOptions !== null || tasksFetchError !== null) {
          clearInterval(poll);
          this.taskPollInterval = null;
          this.tasksLoading = false;
          this.taskOptions = cachedTaskOptions ?? STATIC_TASK_OPTIONS;
          if (tasksFetchError) this.tasksError = tasksFetchError;
          this.cdr.detectChanges();
        } else if (tasksFetchSubscription === null) {
          // Fetch was canceled before populating caches; stop polling and fall back.
          clearInterval(poll);
          this.taskPollInterval = null;
          this.tasksLoading = false;
          this.taskOptions = STATIC_TASK_OPTIONS;
          this.cdr.detectChanges();
        }
      }, 200);
      this.taskPollInterval = poll;
      return;
    }

    this.tasksLoading = true;
    this.tasksError = null;
    this.cdr.detectChanges();

    tasksFetchSubscription = this.http
      .get<HuggingFaceTaskOption[]>(`${AppSettings.getApiEndpoint()}/huggingface/tasks`)
      .pipe(
        takeUntil(this.destroy$),
        finalize(() => {
          // If takeUntil fires before next/error, reset the module-level guard
          // so the next component instance can start a fresh fetch.
          if (cachedTaskOptions === null && tasksFetchError === null) {
            tasksFetchSubscription = null;
          }
        })
      )
      .subscribe({
        next: tasks => {
          tasksFetchSubscription = null;
          cachedTaskOptions = tasks.length > 0 ? tasks : STATIC_TASK_OPTIONS;
          this.taskOptions = cachedTaskOptions;
          this.tasksLoading = false;
          this.cdr.detectChanges();
        },
        error: (err: unknown) => {
          console.error("Failed to load HuggingFace tasks:", err);
          tasksFetchSubscription = null;
          tasksFetchError = "Could not load tasks from Hugging Face. Using default list.";
          this.tasksError = tasksFetchError;
          this.taskOptions = STATIC_TASK_OPTIONS;
          this.tasksLoading = false;
          this.cdr.detectChanges();
        },
      });
  }

  retryTasksLoad(): void {
    tasksFetchError = null;
    this.tasksError = null;
    this.loadTasks();
  }

  // ── Task selection ──

  onTaskSelected(tag: string): void {
    const previousTask = this.getCurrentTaskTag() ?? this.selectedTaskTag;
    this.snapshotTaskState(previousTask);
    this.syncTaskSelection(tag, true);
    this.restoreTaskState(tag);
    this.searchText = "";
    this.filteredModels = null;
    // Cancel any in-flight server search for the previous task
    this.searchSubject$.next("");
    this.loadAllModels();
  }

  // ── Data loading ──

  /**
   * Fetch ALL models for the selected task.
   * The backend paginates through HF Hub internally and caches the result.
   * The first request per task may be slow; subsequent requests are instant.
   */
  private loadAllModels(): void {
    const tag = this.selectedTaskTag || "text-generation";

    this.loading = false;
    this.errorMessage = null;

    // Fast path: cached on the frontend
    if (allModelsByTag.has(tag)) {
      this.allModels = allModelsByTag.get(tag)!;
      this.truncated = truncatedByTag.has(tag);
      this.goToPage(0);
      return;
    }

    // Previous error
    if (errorByTag.has(tag)) {
      this.errorMessage = errorByTag.get(tag)!;
      this.allModels = [];
      this.pagedModels = [];
      this.totalPages = 0;
      return;
    }

    // Another instance is already fetching this task — wait for it
    if (inFlightByTag.has(tag)) {
      this.loading = true;
      if (this.modelPollInterval !== null) clearInterval(this.modelPollInterval);
      const poll = setInterval(() => {
        if (allModelsByTag.has(tag) || errorByTag.has(tag)) {
          clearInterval(poll);
          this.modelPollInterval = null;
          this.loading = false;
          if (allModelsByTag.has(tag)) {
            this.allModels = allModelsByTag.get(tag)!;
            this.truncated = truncatedByTag.has(tag);
            this.goToPage(0);
          } else {
            this.errorMessage = errorByTag.get(tag)!;
            this.cdr.detectChanges();
          }
        } else if (!inFlightByTag.has(tag)) {
          // Fetch was canceled before populating caches; stop polling and fall back.
          clearInterval(poll);
          this.modelPollInterval = null;
          this.loading = false;
          this.cdr.detectChanges();
        }
      }, 200);
      this.modelPollInterval = poll;
      return;
    }

    // Cancel previous
    this.subscription?.unsubscribe();
    this.subscription = null;

    this.allModels = [];
    this.pagedModels = [];
    this.totalPages = 0;

    // Show spinner immediately for the initial fetch — it can take a while
    // as the backend pages through HF Hub for the first time.
    this.loading = true;
    this.cdr.detectChanges();

    this.subscription = this.http
      .get<HuggingFaceModelOption[]>(
        `${AppSettings.getApiEndpoint()}/huggingface/models?task=${encodeURIComponent(tag)}`,
        { observe: "response" }
      )
      .pipe(
        takeUntil(this.destroy$),
        finalize(() => {
          // If takeUntil cancels before next/error fires, clear the in-flight
          // guard so a later instance re-fetches instead of polling forever.
          if (!allModelsByTag.has(tag) && !errorByTag.has(tag)) {
            inFlightByTag.delete(tag);
          }
        })
      )
      .subscribe({
        next: resp => {
          const models = resp.body ?? [];
          if (resp.headers.get(TRUNCATED_HEADER) === "true") {
            truncatedByTag.add(tag);
          }
          allModelsByTag.set(tag, models);
          inFlightByTag.delete(tag);
          this.loading = false;
          this.truncated = truncatedByTag.has(tag);
          this.allModels = models;
          this.goToPage(0);
        },
        error: (err: unknown) => {
          console.error(`Failed to load HuggingFace models for task '${tag}':`, err);
          const msg = "Failed to load models. Click retry to try again.";
          errorByTag.set(tag, msg);
          inFlightByTag.delete(tag);
          this.loading = false;
          this.errorMessage = msg;
          this.cdr.detectChanges();
        },
      });

    inFlightByTag.set(tag, this.subscription);
  }

  // ── Pagination (client-side over the active list) ──

  private get activeList(): HuggingFaceModelOption[] {
    return this.filteredModels !== null ? this.filteredModels : this.allModels;
  }

  goToPage(page: number): void {
    const list = this.activeList;
    this.totalPages = Math.max(1, Math.ceil(list.length / PAGE_SIZE));
    this.currentPage = Math.min(page, this.totalPages - 1);
    const start = this.currentPage * PAGE_SIZE;
    this.pagedModels = list.slice(start, start + PAGE_SIZE);
    this.cdr.detectChanges();
  }

  prevPage(): void {
    if (this.currentPage > 0) {
      this.goToPage(this.currentPage - 1);
    }
  }

  nextPage(): void {
    if (this.currentPage < this.totalPages - 1) {
      this.goToPage(this.currentPage + 1);
    }
  }

  get hasNextPage(): boolean {
    return this.currentPage < this.totalPages - 1;
  }

  retryLoad(): void {
    const tag = this.selectedTaskTag || "text-generation";
    errorByTag.delete(tag);
    this.loadAllModels();
  }

  // ── Search ──

  private setupServerSearch(): void {
    this.searchSubscription = this.searchSubject$
      .pipe(
        debounceTime(300),
        switchMap(query => {
          if (!query.trim()) {
            this.searchLoading = false;
            this.cdr.detectChanges();
            return of(null);
          }
          const tag = this.selectedTaskTag || "text-generation";
          this.searchLoading = true;
          this.cdr.detectChanges();
          return this.http
            .get<
              HuggingFaceModelOption[]
            >(`${AppSettings.getApiEndpoint()}/huggingface/models?task=${encodeURIComponent(tag)}&search=${encodeURIComponent(query)}`)
            .pipe(
              catchError((err: unknown) => {
                console.error("Server-side search failed:", err);
                this.searchLoading = false;
                this.cdr.detectChanges();
                return of(null);
              })
            );
        }),
        takeUntil(this.destroy$)
      )
      .subscribe({
        next: models => {
          if (models === null) return;
          this.searchLoading = false;
          this.filteredModels = models;
          this.goToPage(0);
        },
      });
  }

  onSearchInput(query: string): void {
    this.searchText = query;
    if (!query.trim()) {
      this.filteredModels = null;
      this.searchLoading = false;
      // Cancel any in-flight server search via switchMap
      this.searchSubject$.next("");
      this.goToPage(0);
      return;
    }
    if (this.truncated) {
      // Server-side search — needed because local list is incomplete
      this.searchSubject$.next(query);
    } else {
      // Local filter — full list is available
      const lower = query.toLowerCase();
      this.filteredModels = this.allModels.filter(m => m.id.toLowerCase().includes(lower));
      this.goToPage(0);
    }
  }

  clearSearch(): void {
    this.searchText = "";
    this.filteredModels = null;
    this.searchLoading = false;
    // Cancel any in-flight server search via switchMap
    this.searchSubject$.next("");
    this.goToPage(0);
  }

  get isSearching(): boolean {
    return this.filteredModels !== null || this.searchLoading;
  }

  // ── Model selection ──

  onModelSelected(modelId: string): void {
    this.formControl.setValue(modelId);
  }

  // ── Private helpers ──

  private getCurrentTaskTag(): string | undefined {
    const fromModel = this.model?.task;
    if (typeof fromModel === "string" && fromModel.trim().length > 0) {
      return fromModel;
    }
    const fromParentControl = this.formControl?.parent?.get("task")?.value;
    if (typeof fromParentControl === "string" && fromParentControl.trim().length > 0) {
      return fromParentControl;
    }
    const fromFieldForm = this.field.form?.get("task")?.value;
    if (typeof fromFieldForm === "string" && fromFieldForm.trim().length > 0) {
      return fromFieldForm;
    }
    return undefined;
  }

  private persistTaskSelection(tag: string): void {
    // 1. Update the backing model FIRST so expression functions read the new value.
    if (this.model) {
      this.model.task = tag;
    }

    // 2. Update the hidden task form control. Using emitEvent: true (default)
    //    ensures formly picks up the change and re-evaluates all sibling expressions.
    const taskControlFromField = this.field.form?.get("task");
    if (taskControlFromField) {
      taskControlFromField.setValue(tag);
    }

    const taskControlFromParent = this.formControl?.parent?.get("task");
    if (taskControlFromParent && taskControlFromParent !== taskControlFromField) {
      taskControlFromParent.setValue(tag);
    }

    // 3. Force formly to re-evaluate ALL field expressions (not just this field's subtree).
    //    this.field is the modelId field; its parent covers all sibling fields.
    const rootField = this.field.parent ?? this.field;
    this.field.options?.detectChanges?.(rootField);
  }

  private syncTaskSelection(tag: string, resetTaskSpecificFields: boolean): void {
    this.selectedTaskTag = tag;
    if (resetTaskSpecificFields) {
      this.resetTaskStateForFirstVisit(tag);
    }
    this.persistTaskSelection(tag);
    this.refreshTaskScopedValidity();
  }

  private refreshTaskScopedValidity(): void {
    const keys = [
      "task",
      "modelId",
      "promptColumn",
      "imageInput",
      "audioInput",
      "inputImageColumn",
      "inputAudioColumn",
      "candidateLabels",
      "sentencesColumn",
      "contextColumn",
      "systemPrompt",
      "maxNewTokens",
      "temperature",
    ];
    for (const key of keys) {
      const control = this.field.form?.get(key) ?? this.formControl?.parent?.get(key);
      control?.updateValueAndValidity({ emitEvent: false });
    }
    this.field.form?.updateValueAndValidity({ emitEvent: false });
    this.formControl?.parent?.updateValueAndValidity({ emitEvent: false });

    // Emit a single value change after all fields are settled so the
    // workflow action service picks up the new operator properties.
    this.formControl?.parent?.updateValueAndValidity({ emitEvent: true });
  }

  private snapshotTaskState(tag: string): void {
    if (!tag) {
      return;
    }
    const snapshot: Partial<Record<(typeof this.taskScopedKeys)[number], unknown>> = {};
    for (const key of this.taskScopedKeys) {
      snapshot[key] = this.readFieldValue(key);
    }
    this.taskStateByTag.set(tag, snapshot);
  }

  private restoreTaskState(tag: string): void {
    const snapshot = this.taskStateByTag.get(tag);
    if (!snapshot) {
      return;
    }
    for (const key of this.taskScopedKeys) {
      if (Object.prototype.hasOwnProperty.call(snapshot, key)) {
        this.writeFieldValue(key, snapshot[key]);
      }
    }
    this.refreshTaskScopedValidity();
  }

  private resetTaskStateForFirstVisit(tag: string): void {
    if (this.taskStateByTag.has(tag)) {
      return;
    }
    const defaults: Partial<Record<(typeof this.taskScopedKeys)[number], unknown>> = {
      modelId: "",
      promptColumn: "",
      imageInput: "",
      audioInput: "",
      inputImageColumn: "",
      inputAudioColumn: "",
      candidateLabels: "",
      sentencesColumn: "",
      contextColumn: "",
      systemPrompt: "You are a helpful assistant.",
      maxNewTokens: 256,
      temperature: 0.7,
    };
    for (const key of this.taskScopedKeys) {
      this.writeFieldValue(key, defaults[key] ?? "");
    }
  }

  private readFieldValue(key: (typeof this.taskScopedKeys)[number]): unknown {
    const control = this.field.form?.get(key) ?? this.formControl?.parent?.get(key);
    if (control) {
      return control.value;
    }
    return this.model?.[key];
  }

  private writeFieldValue(key: (typeof this.taskScopedKeys)[number], value: unknown): void {
    const control = this.field.form?.get(key) ?? this.formControl?.parent?.get(key);
    if (control) {
      control.setValue(value, { emitEvent: false });
      control.markAsDirty();
      control.updateValueAndValidity({ emitEvent: false });
    }
    if (this.model) {
      (this.model as Record<string, unknown>)[key] = value;
    }
  }
}

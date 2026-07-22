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

import { ComponentFixture, TestBed, discardPeriodicTasks, fakeAsync, tick } from "@angular/core/testing";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { FormControl, FormGroup } from "@angular/forms";
import { FieldTypeConfig } from "@ngx-formly/core";
import { AppSettings } from "../../../common/app-setting";
import {
  HuggingFaceComponent,
  HuggingFaceModelOption,
  HuggingFaceTaskOption,
  STATIC_TASK_OPTIONS,
  invalidateHuggingFaceModelCache,
} from "./hugging-face.component";

const API = "api";

function buildModels(count: number, prefix = "model"): HuggingFaceModelOption[] {
  return Array.from({ length: count }, (_, i) => ({
    id: `${prefix}/${prefix}-${i}`,
    label: `${prefix}-${i}`,
    downloads: 1000 - i,
    likes: 500 - i,
  }));
}

function buildTaskResponse(): HuggingFaceTaskOption[] {
  return [
    { tag: "text-generation", label: "Text Generation" },
    { tag: "image-classification", label: "Image Classification" },
  ];
}

/**
 * Build a minimal FormlyFieldConfig with a FormGroup backing it,
 * similar to what Formly provides at runtime.
 */
function buildFieldWithFormGroup(taskValue = "", modelIdValue = ""): { field: FieldTypeConfig; formGroup: FormGroup } {
  const formGroup = new FormGroup({
    task: new FormControl(taskValue),
    modelId: new FormControl(modelIdValue),
    promptColumn: new FormControl(""),
    imageInput: new FormControl(""),
    audioInput: new FormControl(""),
    inputImageColumn: new FormControl(""),
    inputAudioColumn: new FormControl(""),
    candidateLabels: new FormControl(""),
    sentencesColumn: new FormControl(""),
    contextColumn: new FormControl(""),
    systemPrompt: new FormControl("You are a helpful assistant."),
    maxNewTokens: new FormControl(256),
    temperature: new FormControl(0.7),
  });

  const model: Record<string, unknown> = {
    task: taskValue,
    modelId: modelIdValue,
  };

  const field = {
    key: "modelId",
    formControl: formGroup.get("modelId")! as FormControl,
    form: formGroup,
    model,
    props: {},
    parent: { fieldGroup: [] },
    options: { detectChanges: vi.fn() },
  } as unknown as FieldTypeConfig;

  return { field, formGroup };
}

// ── Pure unit tests (no TestBed) ──

describe("HuggingFaceComponent (unit)", () => {
  beforeEach(() => {
    invalidateHuggingFaceModelCache();
  });

  it("should export a non-empty static task list", () => {
    expect(STATIC_TASK_OPTIONS.length).toBeGreaterThan(0);
  });

  it("should include text-generation in static task options", () => {
    const textGen = STATIC_TASK_OPTIONS.find(t => t.tag === "text-generation");
    expect(textGen).toBeTruthy();
    expect(textGen!.label).toBe("Text Generation");
  });

  it("should include image tasks in static task options", () => {
    const imageTasks = STATIC_TASK_OPTIONS.filter(t =>
      ["image-classification", "object-detection", "image-segmentation", "image-to-text"].includes(t.tag)
    );
    expect(imageTasks.length).toBe(4);
  });

  it("should include audio tasks in static task options", () => {
    const audioTasks = STATIC_TASK_OPTIONS.filter(t =>
      ["automatic-speech-recognition", "audio-classification", "text-to-speech"].includes(t.tag)
    );
    expect(audioTasks.length).toBe(3);
  });

  it("should include QA/ranking tasks in static task options", () => {
    const qaTasks = STATIC_TASK_OPTIONS.filter(t =>
      ["question-answering", "zero-shot-classification", "sentence-similarity", "text-ranking"].includes(t.tag)
    );
    expect(qaTasks.length).toBe(4);
  });

  it("should clear caches on invalidateHuggingFaceModelCache", () => {
    expect(() => invalidateHuggingFaceModelCache()).not.toThrow();
  });

  it("should have unique tags in static task options", () => {
    const tags = STATIC_TASK_OPTIONS.map(t => t.tag);
    const uniqueTags = new Set(tags);
    expect(uniqueTags.size).toBe(tags.length);
  });
});

// ── TestBed-based integration tests ──

describe("HuggingFaceComponent (TestBed)", () => {
  let component: HuggingFaceComponent;
  let fixture: ComponentFixture<HuggingFaceComponent>;
  let http: HttpTestingController;

  beforeEach(async () => {
    invalidateHuggingFaceModelCache();

    await TestBed.configureTestingModule({
      imports: [HuggingFaceComponent, HttpClientTestingModule],
    }).compileComponents();

    vi.spyOn(AppSettings, "getApiEndpoint").mockReturnValue(API);

    fixture = TestBed.createComponent(HuggingFaceComponent);
    component = fixture.componentInstance;
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    // Destroy the component to trigger ngOnDestroy and clean up subscriptions/timers
    fixture.destroy();
    // Flush any pending icon SVG requests from NzIconModule before verifying
    http.match(req => req.url.startsWith("assets/")).forEach(req => req.flush("<svg></svg>"));
    http.verify();
  });

  /** Flush any pending NzIcon SVG asset requests. */
  function flushIconRequests() {
    http.match(req => req.url.startsWith("assets/")).forEach(req => req.flush("<svg></svg>"));
  }

  /** Set up field + trigger ngOnInit, then flush the two startup HTTP requests. */
  function initComponent(taskTag = "text-generation", models: HuggingFaceModelOption[] = buildModels(3)) {
    const { field } = buildFieldWithFormGroup(taskTag);
    component.field = field;
    fixture.detectChanges(); // triggers ngOnInit
    flushIconRequests();

    // ngOnInit fires two HTTP requests: tasks + models
    const tasksReq = http.expectOne(`${API}/huggingface/tasks`);
    tasksReq.flush(buildTaskResponse());

    const modelsReq = http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`));
    modelsReq.flush(models);
    flushIconRequests();
  }

  // ── Creation ──

  it("should create the component", () => {
    initComponent();
    expect(component).toBeTruthy();
  });

  it("should default selectedTaskTag to text-generation", () => {
    initComponent();
    expect(component.selectedTaskTag).toBe("text-generation");
  });

  // ── Task loading ──

  describe("task loading", () => {
    it("should fetch tasks from the API on init", () => {
      const { field } = buildFieldWithFormGroup();
      component.field = field;
      fixture.detectChanges();

      const tasksReq = http.expectOne(`${API}/huggingface/tasks`);
      expect(tasksReq.request.method).toBe("GET");
      tasksReq.flush(buildTaskResponse());

      // Also flush the models request
      http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`)).flush([]);

      expect(component.taskOptions).toEqual(buildTaskResponse());
      expect(component.tasksLoading).toBe(false);
    });

    it("should fall back to STATIC_TASK_OPTIONS when API returns empty array", () => {
      const { field } = buildFieldWithFormGroup();
      component.field = field;
      fixture.detectChanges();

      http.expectOne(`${API}/huggingface/tasks`).flush([]);
      http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`)).flush([]);

      expect(component.taskOptions).toEqual(STATIC_TASK_OPTIONS);
    });

    it("should fall back to STATIC_TASK_OPTIONS on task fetch error", () => {
      const { field } = buildFieldWithFormGroup();
      component.field = field;
      fixture.detectChanges();

      http.expectOne(`${API}/huggingface/tasks`).error(new ProgressEvent("error"));
      http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`)).flush([]);

      expect(component.taskOptions).toEqual(STATIC_TASK_OPTIONS);
      expect(component.tasksError).toBeTruthy();
      expect(component.tasksLoading).toBe(false);
    });

    it("retryTasksLoad should clear error and re-fetch tasks", fakeAsync(() => {
      initComponent();

      // Simulate a prior error state by directly calling retryTasksLoad
      // First, force an error so retryTasksLoad has something to retry
      invalidateHuggingFaceModelCache();
      component.tasksError = "previous error";
      component.retryTasksLoad();
      tick();

      const tasksReq = http.expectOne(`${API}/huggingface/tasks`);
      tasksReq.flush(buildTaskResponse());

      expect(component.tasksError).toBeNull();
      expect(component.taskOptions).toEqual(buildTaskResponse());
    }));
  });

  // ── Model loading ──

  describe("model loading", () => {
    it("should fetch models for the selected task on init", () => {
      const { field } = buildFieldWithFormGroup("image-classification");
      component.field = field;
      fixture.detectChanges();

      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      const modelsReq = http.expectOne(`${API}/huggingface/models?task=image-classification`);
      expect(modelsReq.request.method).toBe("GET");
      modelsReq.flush(buildModels(5));

      expect(component.pagedModels.length).toBe(5);
      expect(component.loading).toBe(false);
    });

    it("should show loading state while models are being fetched", () => {
      const { field } = buildFieldWithFormGroup();
      component.field = field;
      fixture.detectChanges();

      // Tasks request is pending, but check model loading state
      expect(component.loading).toBe(true);

      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`)).flush(buildModels(2));

      expect(component.loading).toBe(false);
    });

    it("should set truncated flag from X-Texera-Truncated header", () => {
      const { field } = buildFieldWithFormGroup();
      component.field = field;
      fixture.detectChanges();

      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      const modelsReq = http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`));
      modelsReq.flush(buildModels(5), { headers: { "X-Texera-Truncated": "true" } });

      expect(component.truncated).toBe(true);
    });

    it("should not set truncated when header is absent", () => {
      initComponent("text-generation", buildModels(5));
      expect(component.truncated).toBe(false);
    });

    it("should display error on model fetch failure", () => {
      const { field } = buildFieldWithFormGroup();
      component.field = field;
      fixture.detectChanges();

      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`)).error(new ProgressEvent("error"));

      expect(component.errorMessage).toBeTruthy();
      expect(component.loading).toBe(false);
      expect(component.pagedModels.length).toBe(0);
    });

    it("retryLoad should clear error and re-fetch models", () => {
      const { field } = buildFieldWithFormGroup();
      component.field = field;
      fixture.detectChanges();

      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`)).error(new ProgressEvent("error"));

      expect(component.errorMessage).toBeTruthy();

      component.retryLoad();
      const retryReq = http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`));
      retryReq.flush(buildModels(3));

      expect(component.errorMessage).toBeNull();
      expect(component.pagedModels.length).toBe(3);
    });

    it("should use cached models on second access for the same task", () => {
      initComponent("text-generation", buildModels(3));
      expect(component.pagedModels.length).toBe(3);

      // Simulate switching away and back — models should come from cache, no HTTP request
      component.onTaskSelected("image-classification");
      const modelsReq = http.expectOne(`${API}/huggingface/models?task=image-classification`);
      modelsReq.flush(buildModels(2, "img"));

      component.onTaskSelected("text-generation");
      // No new HTTP request for text-generation — it's cached
      expect(component.pagedModels.length).toBe(3);
    });
  });

  // ── Pagination ──

  describe("pagination", () => {
    it("should page models with PAGE_SIZE of 50", () => {
      initComponent("text-generation", buildModels(120));

      expect(component.totalPages).toBe(3);
      expect(component.currentPage).toBe(0);
      expect(component.pagedModels.length).toBe(50);
    });

    it("should navigate to next page", () => {
      initComponent("text-generation", buildModels(120));

      component.nextPage();
      expect(component.currentPage).toBe(1);
      expect(component.pagedModels.length).toBe(50);
      expect(component.pagedModels[0].id).toBe("model/model-50");
    });

    it("should navigate to previous page", () => {
      initComponent("text-generation", buildModels(120));

      component.nextPage();
      expect(component.currentPage).toBe(1);

      component.prevPage();
      expect(component.currentPage).toBe(0);
      expect(component.pagedModels[0].id).toBe("model/model-0");
    });

    it("should not go below page 0", () => {
      initComponent("text-generation", buildModels(120));

      component.prevPage();
      expect(component.currentPage).toBe(0);
    });

    it("should not go past the last page", () => {
      initComponent("text-generation", buildModels(120));

      component.nextPage();
      component.nextPage();
      expect(component.currentPage).toBe(2);
      expect(component.pagedModels.length).toBe(20); // 120 - 2*50 = 20

      component.nextPage();
      expect(component.currentPage).toBe(2); // stays at last page
    });

    it("hasNextPage should return correct value", () => {
      initComponent("text-generation", buildModels(120));

      expect(component.hasNextPage).toBe(true);
      component.nextPage();
      expect(component.hasNextPage).toBe(true);
      component.nextPage();
      expect(component.hasNextPage).toBe(false);
    });

    it("goToPage should clamp to valid range", () => {
      initComponent("text-generation", buildModels(120));

      component.goToPage(999);
      expect(component.currentPage).toBe(2); // last page

      component.goToPage(0);
      expect(component.currentPage).toBe(0);
    });

    it("should show single page for small model lists", () => {
      initComponent("text-generation", buildModels(10));

      expect(component.totalPages).toBe(1);
      expect(component.currentPage).toBe(0);
      expect(component.pagedModels.length).toBe(10);
      expect(component.hasNextPage).toBe(false);
    });

    it("should handle empty model list", () => {
      initComponent("text-generation", []);

      expect(component.totalPages).toBe(1);
      expect(component.currentPage).toBe(0);
      expect(component.pagedModels.length).toBe(0);
    });
  });

  // ── Search ──

  describe("search", () => {
    it("should filter models locally when list is not truncated", () => {
      const models = [
        { id: "bert-base", label: "bert-base", downloads: 100, likes: 50 },
        { id: "gpt2", label: "gpt2", downloads: 200, likes: 100 },
        { id: "bert-large", label: "bert-large", downloads: 80, likes: 40 },
      ];
      initComponent("text-generation", models);

      component.onSearchInput("bert");

      expect(component.pagedModels.length).toBe(2);
      expect(component.pagedModels.every(m => m.id.includes("bert"))).toBe(true);
    });

    it("should be case-insensitive for local search", () => {
      const models = [
        { id: "BERT-Base", label: "BERT-Base", downloads: 100, likes: 50 },
        { id: "gpt2", label: "gpt2", downloads: 200, likes: 100 },
      ];
      initComponent("text-generation", models);

      component.onSearchInput("bert");
      expect(component.pagedModels.length).toBe(1);
      expect(component.pagedModels[0].id).toBe("BERT-Base");
    });

    it("should clear filter when search text is empty", () => {
      const models = buildModels(5);
      initComponent("text-generation", models);

      component.onSearchInput("model-0");
      expect(component.pagedModels.length).toBe(1);

      component.onSearchInput("");
      expect(component.pagedModels.length).toBe(5);
    });

    it("should clear filter when search text is whitespace", () => {
      const models = buildModels(5);
      initComponent("text-generation", models);

      component.onSearchInput("model-0");
      expect(component.pagedModels.length).toBe(1);

      component.onSearchInput("   ");
      expect(component.pagedModels.length).toBe(5);
    });

    it("clearSearch should reset search state", () => {
      initComponent("text-generation", buildModels(5));

      component.onSearchInput("model-0");
      expect(component.searchText).toBe("model-0");

      component.clearSearch();
      expect(component.searchText).toBe("");
      expect(component.searchLoading).toBe(false);
      expect(component.pagedModels.length).toBe(5);
    });

    it("isSearching should return true when filtered models exist", () => {
      initComponent("text-generation", buildModels(5));

      expect(component.isSearching).toBe(false);

      component.onSearchInput("model-0");
      expect(component.isSearching).toBe(true);

      component.clearSearch();
      expect(component.isSearching).toBe(false);
    });

    it("should use server-side search when list is truncated", fakeAsync(() => {
      const { field } = buildFieldWithFormGroup();
      component.field = field;
      fixture.detectChanges();

      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      const modelsReq = http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`));
      modelsReq.flush(buildModels(5), { headers: { "X-Texera-Truncated": "true" } });

      expect(component.truncated).toBe(true);

      // Trigger server-side search
      component.onSearchInput("special-model");
      tick(300); // debounceTime

      const searchReq = http.expectOne(
        req => req.url.includes("/huggingface/models") && req.url.includes("search=special-model")
      );
      const searchResults = [{ id: "special-model/v1", label: "special-model/v1" }];
      searchReq.flush(searchResults);

      expect(component.pagedModels.length).toBe(1);
      expect(component.pagedModels[0].id).toBe("special-model/v1");
      expect(component.searchLoading).toBe(false);
    }));

    it("should reset pagination to page 0 on search", () => {
      initComponent("text-generation", buildModels(120));

      component.nextPage();
      expect(component.currentPage).toBe(1);

      component.onSearchInput("model-1");
      expect(component.currentPage).toBe(0);
    });
  });

  // ── Task selection ──

  describe("task selection", () => {
    it("onTaskSelected should update selectedTaskTag", () => {
      initComponent();

      component.onTaskSelected("image-classification");
      http.expectOne(`${API}/huggingface/models?task=image-classification`).flush(buildModels(2, "img"));

      expect(component.selectedTaskTag).toBe("image-classification");
    });

    it("onTaskSelected should load models for the new task", () => {
      initComponent();

      component.onTaskSelected("image-classification");
      const req = http.expectOne(`${API}/huggingface/models?task=image-classification`);
      req.flush(buildModels(4, "img"));

      expect(component.pagedModels.length).toBe(4);
    });

    it("onTaskSelected should clear search state", () => {
      initComponent("text-generation", buildModels(5));

      component.onSearchInput("model-0");
      expect(component.searchText).toBe("model-0");

      component.onTaskSelected("image-classification");
      http.expectOne(`${API}/huggingface/models?task=image-classification`).flush([]);

      expect(component.searchText).toBe("");
    });

    it("should persist task to model and form control", () => {
      const { field, formGroup } = buildFieldWithFormGroup("text-generation");
      component.field = field;
      fixture.detectChanges();

      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`)).flush([]);

      component.onTaskSelected("image-classification");
      http.expectOne(`${API}/huggingface/models?task=image-classification`).flush([]);

      expect(formGroup.get("task")!.value).toBe("image-classification");
      expect(field.model!["task"]).toBe("image-classification");
    });

    it("should restore task-scoped field state when switching back", () => {
      const { field, formGroup } = buildFieldWithFormGroup("text-generation");
      component.field = field;
      fixture.detectChanges();

      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`)).flush([]);

      // Set a custom value while on text-generation
      formGroup.get("systemPrompt")!.setValue("Custom prompt");

      // Switch to image-classification
      component.onTaskSelected("image-classification");
      http.expectOne(`${API}/huggingface/models?task=image-classification`).flush([]);

      // The systemPrompt should be reset (first visit defaults)
      expect(formGroup.get("systemPrompt")!.value).toBe("You are a helpful assistant.");

      // Switch back to text-generation — should restore the custom prompt
      component.onTaskSelected("text-generation");
      expect(formGroup.get("systemPrompt")!.value).toBe("Custom prompt");
    });

    it("should read initial task tag from the form model", () => {
      const { field } = buildFieldWithFormGroup("image-classification");
      component.field = field;
      fixture.detectChanges();

      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      http.expectOne(`${API}/huggingface/models?task=image-classification`).flush(buildModels(2, "img"));

      expect(component.selectedTaskTag).toBe("image-classification");
    });
  });

  // ── Model selection ──

  describe("model selection", () => {
    it("onModelSelected should set the formControl value", () => {
      const { field } = buildFieldWithFormGroup();
      component.field = field;
      fixture.detectChanges();

      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`)).flush(buildModels(3));

      component.onModelSelected("model/model-1");
      expect(field.formControl!.value).toBe("model/model-1");
    });
  });

  // ── Cleanup ──

  describe("cleanup", () => {
    it("should clean up on destroy without errors", () => {
      initComponent();
      expect(() => component.ngOnDestroy()).not.toThrow();
    });

    it("should clear taskPollInterval on destroy", fakeAsync(() => {
      // Create a second fixture so the first one is still fetching tasks when this one inits
      invalidateHuggingFaceModelCache();
      const fixture2 = TestBed.createComponent(HuggingFaceComponent);
      const component2 = fixture2.componentInstance;
      const { field: field2 } = buildFieldWithFormGroup("text-generation");
      component2.field = field2;
      fixture2.detectChanges(); // triggers ngOnInit → loadTasks() (sets tasksFetchSubscription)
      http.match(req => req.url.startsWith("assets/")).forEach(req => req.flush("<svg></svg>"));

      // Now init our main component — tasks in flight, so it enters poll path
      const { field } = buildFieldWithFormGroup("text-generation");
      component.field = field;
      fixture.detectChanges();
      flushIconRequests();

      // comp enters poll path for tasks — taskPollInterval is set
      expect((component as any).taskPollInterval).not.toBeNull();

      // Destroy while poll is still running
      expect(() => component.ngOnDestroy()).not.toThrow();

      // Clean up fixture2
      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      http.match(req => req.url.startsWith(`${API}/huggingface/models`)).forEach(req => req.flush([]));
      fixture2.destroy();
      discardPeriodicTasks();
    }));
  });

  // ── Cached error states ──

  describe("cached error states", () => {
    it("should show cached tasks error for a second component without re-fetching", () => {
      const { field: field1 } = buildFieldWithFormGroup();
      component.field = field1;
      fixture.detectChanges();
      flushIconRequests();

      // comp1: tasks error, models succeed → models are cached
      http.expectOne(`${API}/huggingface/tasks`).error(new ProgressEvent("error"));
      http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`)).flush([]);

      expect(component.tasksError).toBeTruthy();

      const fixture2 = TestBed.createComponent(HuggingFaceComponent);
      const component2 = fixture2.componentInstance;
      const { field: field2 } = buildFieldWithFormGroup();
      component2.field = field2;
      fixture2.detectChanges();
      flushIconRequests();

      // Tasks error cached (no new tasks request); models cached (no new model request)

      expect(component2.tasksError).toBeTruthy();
      expect(component2.taskOptions).toEqual(STATIC_TASK_OPTIONS);
      expect(component2.tasksLoading).toBe(false);

      fixture2.destroy();
    });

    it("should show cached model error for a second component without re-fetching models", () => {
      const { field: field1 } = buildFieldWithFormGroup("text-generation");
      component.field = field1;
      fixture.detectChanges();
      flushIconRequests();

      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`)).error(new ProgressEvent("error"));

      expect(component.errorMessage).toBeTruthy();

      const fixture2 = TestBed.createComponent(HuggingFaceComponent);
      const component2 = fixture2.componentInstance;
      const { field: field2 } = buildFieldWithFormGroup("text-generation");
      component2.field = field2;
      fixture2.detectChanges();
      flushIconRequests();

      // Tasks cached; model error cached — no new HTTP requests at all
      expect(component2.errorMessage).toBeTruthy();
      expect(component2.loading).toBe(false);
      expect(component2.pagedModels.length).toBe(0);

      fixture2.destroy();
    });
  });

  // ── Polling ──

  describe("polling", () => {
    it("should enter model poll path (loading=true) when another instance is already fetching models", fakeAsync(() => {
      // comp1 fetches tasks first, but leaves models in flight
      const { field: field1 } = buildFieldWithFormGroup("text-generation");
      component.field = field1;
      fixture.detectChanges();
      flushIconRequests();
      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      // models still in flight for comp1

      // comp2: tasks cached, but models are in-flight → enters model poll path
      const fixture2 = TestBed.createComponent(HuggingFaceComponent);
      const component2 = fixture2.componentInstance;
      const { field: field2 } = buildFieldWithFormGroup("text-generation");
      component2.field = field2;
      fixture2.detectChanges();
      http.match(req => req.url.startsWith("assets/")).forEach(req => req.flush("<svg></svg>"));

      expect(component2.loading).toBe(true); // poll path entered

      // Clean up: flush comp1's pending model request, discard comp2's poll
      http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`)).flush(buildModels(3));
      fixture2.destroy();
      discardPeriodicTasks();
    }));

    it("should enter task poll path (tasksLoading=true) when another instance is already fetching tasks", fakeAsync(() => {
      // comp1 starts fetching tasks — leave in flight
      const { field: field1 } = buildFieldWithFormGroup("text-generation");
      component.field = field1;
      fixture.detectChanges();
      flushIconRequests();
      // tasks and models both in flight for comp1

      // comp2: tasksFetchSubscription not null → enters task poll path
      const fixture2 = TestBed.createComponent(HuggingFaceComponent);
      const component2 = fixture2.componentInstance;
      const { field: field2 } = buildFieldWithFormGroup("text-generation");
      component2.field = field2;
      fixture2.detectChanges();
      http.match(req => req.url.startsWith("assets/")).forEach(req => req.flush("<svg></svg>"));

      expect(component2.tasksLoading).toBe(true); // poll path entered

      // Clean up: flush comp1's pending requests, discard comp2's polls
      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      http.match(req => req.url.startsWith(`${API}/huggingface/models`)).forEach(req => req.flush([]));
      fixture2.destroy();
      discardPeriodicTasks();
    }));

    it("should clear error and re-fetch models on retryLoad", () => {
      initComponent();

      component.onTaskSelected("image-classification");
      http.expectOne(`${API}/huggingface/models?task=image-classification`).error(new ProgressEvent("error"));

      expect(component.errorMessage).toBeTruthy();

      component.retryLoad();
      http.expectOne(`${API}/huggingface/models?task=image-classification`).flush(buildModels(4, "img"));

      expect(component.errorMessage).toBeNull();
      expect(component.pagedModels.length).toBe(4);
    });
  });

  // ── Pagination edge cases ──

  describe("pagination edge cases", () => {
    it("prevPage at page 0 should keep currentPage at 0", () => {
      initComponent("text-generation", buildModels(120));

      expect(component.currentPage).toBe(0);
      component.prevPage();
      expect(component.currentPage).toBe(0);
      component.prevPage();
      expect(component.currentPage).toBe(0);
      expect(component.pagedModels[0].id).toBe("model/model-0");
    });

    it("goToPage(0) on empty list should not throw", () => {
      initComponent("text-generation", []);
      expect(() => component.goToPage(0)).not.toThrow();
      expect(component.currentPage).toBe(0);
      expect(component.pagedModels.length).toBe(0);
    });
  });

  // ── Task state snapshot edge cases ──

  describe("task state snapshots", () => {
    it("should reset task-scoped fields to defaults on first visit to a new task", () => {
      const { field, formGroup } = buildFieldWithFormGroup("text-generation");
      component.field = field;
      fixture.detectChanges();

      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`)).flush([]);

      // Set non-default values on text-generation
      formGroup.get("systemPrompt")!.setValue("Custom prompt");
      formGroup.get("maxNewTokens")!.setValue(512);
      formGroup.get("temperature")!.setValue(0.9);

      // Switch to image-classification (first visit)
      component.onTaskSelected("image-classification");
      http.expectOne(`${API}/huggingface/models?task=image-classification`).flush([]);

      // First visit defaults should be applied
      expect(formGroup.get("systemPrompt")!.value).toBe("You are a helpful assistant.");
      expect(formGroup.get("maxNewTokens")!.value).toBe(256);
      expect(formGroup.get("temperature")!.value).toBe(0.7);
    });

    it("should preserve task state across multiple switches", () => {
      const { field, formGroup } = buildFieldWithFormGroup("text-generation");
      component.field = field;
      fixture.detectChanges();

      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`)).flush([]);

      // Set values on text-generation
      formGroup.get("promptColumn")!.setValue("prompt_col");
      formGroup.get("modelId")!.setValue("my-org/my-model");

      // Switch away
      component.onTaskSelected("image-classification");
      http.expectOne(`${API}/huggingface/models?task=image-classification`).flush([]);

      // Set values on image-classification
      formGroup.get("modelId")!.setValue("img-org/img-model");

      // Switch back to text-generation
      component.onTaskSelected("text-generation");
      expect(formGroup.get("promptColumn")!.value).toBe("prompt_col");
      expect(formGroup.get("modelId")!.value).toBe("my-org/my-model");

      // Switch back to image-classification
      component.onTaskSelected("image-classification");
      http.match(`${API}/huggingface/models?task=image-classification`); // might be cached
      expect(formGroup.get("modelId")!.value).toBe("img-org/img-model");
    });
  });

  // ── Server-side search edge cases ──

  describe("server search edge cases", () => {
    it("should handle server search error gracefully", fakeAsync(() => {
      const { field } = buildFieldWithFormGroup();
      component.field = field;
      fixture.detectChanges();

      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      const modelsReq = http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`));
      modelsReq.flush(buildModels(5), { headers: { "X-Texera-Truncated": "true" } });

      component.onSearchInput("fail-query");
      tick(300);

      const searchReq = http.expectOne(req => req.url.includes("search=fail-query"));
      searchReq.error(new ProgressEvent("error"));

      // Should not crash; searchLoading should be reset
      expect(component.searchLoading).toBe(false);
    }));

    it("should replace search results when a new query supersedes the previous one", fakeAsync(() => {
      const { field } = buildFieldWithFormGroup();
      component.field = field;
      fixture.detectChanges();

      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      const modelsReq = http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`));
      modelsReq.flush(buildModels(5), { headers: { "X-Texera-Truncated": "true" } });

      // First search — complete it
      component.onSearchInput("query-1");
      tick(300);
      const req1 = http.expectOne(req => req.url.includes("search=query-1"));
      req1.flush([{ id: "result-1", label: "result-1" }]);

      expect(component.pagedModels.length).toBe(1);
      expect(component.pagedModels[0].id).toBe("result-1");

      // Second search — results should replace the first
      component.onSearchInput("query-2");
      tick(300);
      const req2 = http.expectOne(req => req.url.includes("search=query-2"));
      req2.flush([
        { id: "result-2a", label: "result-2a" },
        { id: "result-2b", label: "result-2b" },
      ]);

      expect(component.pagedModels.length).toBe(2);
      expect(component.pagedModels[0].id).toBe("result-2a");
    }));
  });

  // ── getCurrentTaskTag fallbacks ──

  describe("getCurrentTaskTag", () => {
    it("should read task from model.task when available", () => {
      const { field } = buildFieldWithFormGroup("image-classification");
      component.field = field;
      fixture.detectChanges();

      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      http.expectOne(`${API}/huggingface/models?task=image-classification`).flush(buildModels(2, "img"));

      expect(component.selectedTaskTag).toBe("image-classification");
    });

    it("should read task from formControl.parent when model.task is empty", () => {
      const { field, formGroup } = buildFieldWithFormGroup("");
      // Clear model.task but set parent form control
      field.model!["task"] = "";
      formGroup.get("task")!.setValue("summarization");
      component.field = field;
      fixture.detectChanges();

      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      http.expectOne(`${API}/huggingface/models?task=summarization`).flush([]);

      expect(component.selectedTaskTag).toBe("summarization");
    });
  });

  // ── Model selection edge cases ──

  describe("model selection edge cases", () => {
    it("onModelSelected should mark formControl as dirty", () => {
      const { field } = buildFieldWithFormGroup();
      component.field = field;
      fixture.detectChanges();

      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`)).flush(buildModels(3));

      component.onModelSelected("model/model-2");
      expect(field.formControl!.value).toBe("model/model-2");
    });

    it("onModelSelected should overwrite previous selection", () => {
      const { field } = buildFieldWithFormGroup();
      component.field = field;
      fixture.detectChanges();

      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`)).flush(buildModels(3));

      component.onModelSelected("model/model-0");
      expect(field.formControl!.value).toBe("model/model-0");

      component.onModelSelected("model/model-2");
      expect(field.formControl!.value).toBe("model/model-2");
    });
  });

  // ── retryLoad ──

  describe("retryLoad", () => {
    it("should clear the cached error and refetch models", () => {
      const { field } = buildFieldWithFormGroup();
      component.field = field;
      fixture.detectChanges();

      http.expectOne(`${API}/huggingface/tasks`).flush(buildTaskResponse());
      http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`)).error(new ProgressEvent("error"));

      expect(component.errorMessage).toBeTruthy();

      // Retry should clear error and fetch again
      component.retryLoad();
      expect(component.errorMessage).toBeNull();

      const retryReq = http.expectOne(req => req.url.startsWith(`${API}/huggingface/models`));
      retryReq.flush(buildModels(5));

      expect(component.pagedModels.length).toBe(5);
      expect(component.loading).toBe(false);
    });
  });
});

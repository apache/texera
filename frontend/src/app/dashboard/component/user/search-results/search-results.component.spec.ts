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

import { Component, EventEmitter, Input, Output, TemplateRef, ViewChild } from "@angular/core";
import { NgTemplateOutlet } from "@angular/common";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { LoadMoreFunction, SearchResultsComponent } from "./search-results.component";
import { ListItemComponent } from "../list-item/list-item.component";
import { DashboardEntry } from "../../../type/dashboard-entry";
import { UserService } from "../../../../common/service/user/user.service";
import { StubUserService } from "../../../../common/service/user/stub-user.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";

/**
 * Builds a minimal DashboardEntry good enough for the pure selection/tracking
 * logic under test. The real constructor requires a fully-formed backend value,
 * which these tests do not exercise, so a plain cast object is used instead.
 */
function makeEntry(type: string, id: number, checked = false): DashboardEntry {
  return { type, id, checked } as unknown as DashboardEntry;
}

/**
 * Light stand-in for the heavy ListItemComponent. It declares the same selector
 * and the exact inputs/outputs the template binds, so rendering exercises the
 * *ngFor without pulling in ListItemComponent's service graph.
 */
@Component({
  standalone: true,
  selector: "texera-list-item",
  template: "<div class='stub-list-item'>{{ entry?.id }}</div>",
})
class StubListItemComponent {
  @Input() isPrivateSearch = false;
  @Input() editable = false;
  @Input() entry?: DashboardEntry;
  @Input() currentUid?: number;
  @Output() deleted = new EventEmitter<DashboardEntry>();
  @Output() duplicated = new EventEmitter<DashboardEntry>();
  @Output() refresh = new EventEmitter<void>();
  @Output() checkboxChanged = new EventEmitter<void>();
}

/** Host used only to obtain a real TemplateRef for the card-view render test. */
@Component({
  standalone: true,
  imports: [NgTemplateOutlet],
  template: `<ng-template
    #tpl
    let-entry
    ><span class="card-entry">{{ entry.id }}</span></ng-template
  >`,
})
class CardTemplateHostComponent {
  @ViewChild("tpl", { static: true }) tpl!: TemplateRef<{ $implicit: DashboardEntry }>;
}

describe("SearchResultsComponent", () => {
  let component: SearchResultsComponent;
  let fixture: ComponentFixture<SearchResultsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [SearchResultsComponent, NoopAnimationsModule],
      providers: [{ provide: UserService, useClass: StubUserService }, ...commonTestProviders],
    }).compileComponents();

    // ListItemComponent is heavy (needs a full service graph); swap it for a
    // light stub with the same selector so detectChanges can render the *ngFor.
    TestBed.overrideComponent(SearchResultsComponent, {
      remove: { imports: [ListItemComponent] },
      add: { imports: [StubListItemComponent] },
    });

    fixture = TestBed.createComponent(SearchResultsComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    fixture?.destroy();
    document.querySelectorAll(".cdk-overlay-container").forEach(c => (c.innerHTML = ""));
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("getUid", () => {
    it("returns the current user's uid when a user is logged in", () => {
      // StubUserService default user has uid = 1 (MOCK_USER_ID).
      expect(component.getUid()).toBe(1);
    });

    it("returns a spied uid", () => {
      const userService = TestBed.inject(UserService);
      vi.spyOn(userService, "getCurrentUser").mockReturnValue({ uid: 42 } as any);
      expect(component.getUid()).toBe(42);
    });

    it("returns undefined when no user is logged in", () => {
      const userService = TestBed.inject(UserService);
      vi.spyOn(userService, "getCurrentUser").mockReturnValue(undefined);
      expect(component.getUid()).toBeUndefined();
    });
  });

  describe("reset", () => {
    it("clears entries and stores the provided loadMoreFunction", async () => {
      component.entries = [makeEntry("workflow", 1), makeEntry("dataset", 2)];

      const loadMoreFunction: LoadMoreFunction = vi.fn(async () => ({ entries: [makeEntry("file", 3)], more: false }));
      component.reset(loadMoreFunction);

      expect(component.entries).toEqual([]);
      expect(component.loadMoreFunction).toBe(loadMoreFunction);

      // The stored function is the one loadMore invokes.
      await component.loadMore();
      expect(loadMoreFunction).toHaveBeenCalledTimes(1);
      expect(component.entries).toEqual([makeEntry("file", 3)]);
    });
  });

  describe("loadMore", () => {
    it("throws when there is no loadMoreFunction and leaves loading false", async () => {
      await expect(component.loadMore()).rejects.toThrow("This is an empty list and cannot load more entries.");
      expect(component.loading).toBe(false);
    });

    it("appends returned entries, sets more, and advances the start offset across calls", async () => {
      const first = vi.fn<LoadMoreFunction>(async () => ({ entries: [makeEntry("workflow", 1)], more: true }));
      component.reset(first);

      await component.loadMore();

      expect(first).toHaveBeenCalledWith(0, 20);
      expect(component.entries).toEqual([makeEntry("workflow", 1)]);
      expect(component.more).toBe(true);

      const second = vi.fn<LoadMoreFunction>(async () => ({ entries: [makeEntry("dataset", 2)], more: false }));
      component.loadMoreFunction = second;

      await component.loadMore();

      // start offset is the current entry count (1), not 0.
      expect(second).toHaveBeenCalledWith(1, 20);
      expect(component.entries).toEqual([makeEntry("workflow", 1), makeEntry("dataset", 2)]);
      expect(component.more).toBe(false);
    });

    it("sets loading true while in flight and false after completion", async () => {
      let resolveFn!: (value: { entries: DashboardEntry[]; more: boolean }) => void;
      const gate = new Promise<{ entries: DashboardEntry[]; more: boolean }>(resolve => (resolveFn = resolve));
      component.reset(() => gate);

      const inFlight = component.loadMore();
      expect(component.loading).toBe(true);

      resolveFn({ entries: [], more: false });
      await inFlight;
      expect(component.loading).toBe(false);
    });

    it("discards results when reset() is called mid-flight (reset-during-flight race)", async () => {
      let resolveFn!: (value: { entries: DashboardEntry[]; more: boolean }) => void;
      const gate = new Promise<{ entries: DashboardEntry[]; more: boolean }>(resolve => (resolveFn = resolve));
      component.reset(() => gate);

      const inFlight = component.loadMore();
      expect(component.loading).toBe(true);

      // A reset arrives before the in-flight load resolves: it bumps the internal
      // resetCounter, so the stale results must be thrown away.
      component.reset(vi.fn(async () => ({ entries: [], more: false })));

      resolveFn({ entries: [makeEntry("workflow", 99)], more: true });
      await inFlight;

      expect(component.entries).toEqual([]);
      expect(component.more).toBe(false);
      expect(component.loading).toBe(false);
    });
  });

  describe("onEntryCheckboxChange", () => {
    it("emits notifyWorkflow when every entry is checked", () => {
      component.entries = [makeEntry("workflow", 1, true), makeEntry("dataset", 2, true)];
      const emit = vi.fn();
      const sub = component.notifyWorkflow.subscribe(() => emit());

      component.onEntryCheckboxChange();

      expect(emit).toHaveBeenCalledTimes(1);
      sub.unsubscribe();
    });

    it("does not emit notifyWorkflow when at least one entry is unchecked", () => {
      component.entries = [makeEntry("workflow", 1, true), makeEntry("dataset", 2, false)];
      const emit = vi.fn();
      const sub = component.notifyWorkflow.subscribe(() => emit());

      component.onEntryCheckboxChange();

      expect(emit).not.toHaveBeenCalled();
      sub.unsubscribe();
    });
  });

  describe("selectAll / clearAllSelections", () => {
    it("selectAll sets checked=true on every entry", () => {
      component.entries = [makeEntry("workflow", 1, false), makeEntry("dataset", 2, false)];

      component.selectAll();

      expect(component.entries.every(entry => entry.checked)).toBe(true);
    });

    it("clearAllSelections sets checked=false on every entry", () => {
      component.entries = [makeEntry("workflow", 1, true), makeEntry("dataset", 2, true)];

      component.clearAllSelections();

      expect(component.entries.every(entry => !entry.checked)).toBe(true);
    });
  });

  describe("trackByEntryId", () => {
    it("returns `${type}-${id}`", () => {
      expect(component.trackByEntryId(0, makeEntry("workflow", 5))).toBe("workflow-5");
      expect(component.trackByEntryId(3, makeEntry("dataset", 12))).toBe("dataset-12");
    });
  });

  describe("template rendering", () => {
    it("renders the list view with a load-more button when more results exist", () => {
      component.viewMode = "list";
      component.entries = [makeEntry("workflow", 1), makeEntry("dataset", 2)];
      component.more = true;
      component.loading = false;

      fixture.detectChanges();

      const el = fixture.nativeElement as HTMLElement;
      expect(el.querySelector("cdk-virtual-scroll-viewport")).toBeTruthy();
      // one list item per entry (rendered via the stub)
      expect(el.querySelectorAll("texera-list-item").length).toBe(2);
      const button = el.querySelector("button");
      expect(button?.textContent).toContain("Load more");
    });

    it("hides the load-more button while loading", () => {
      component.viewMode = "list";
      component.entries = [makeEntry("workflow", 1)];
      component.more = true;
      component.loading = true;

      fixture.detectChanges();

      const el = fixture.nativeElement as HTMLElement;
      expect(el.querySelector("button")).toBeNull();
    });

    it("renders the card view via the provided cardTemplate", () => {
      const hostFixture = TestBed.createComponent(CardTemplateHostComponent);
      hostFixture.detectChanges();
      component.cardTemplate = hostFixture.componentInstance.tpl;
      component.viewMode = "card";
      component.entries = [makeEntry("workflow", 7), makeEntry("dataset", 8)];

      fixture.detectChanges();

      const el = fixture.nativeElement as HTMLElement;
      const cards = el.querySelectorAll(".card-entry");
      expect(cards.length).toBe(2);
      expect(Array.from(cards).map(c => c.textContent)).toEqual(["7", "8"]);

      hostFixture.destroy();
    });
  });
});

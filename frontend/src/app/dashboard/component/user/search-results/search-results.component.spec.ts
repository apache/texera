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
import { By } from "@angular/platform-browser";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { RouterTestingModule } from "@angular/router/testing";
import { NzModalService } from "ng-zorro-antd/modal";
import { LoadMoreFunction, SearchResultsComponent, SearchResultsViewMode } from "./search-results.component";
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

/**
 * Host that drives SearchResultsComponent purely through its template contract:
 * inputs go in through bindings and every output is recorded as it arrives.
 */
@Component({
  standalone: true,
  imports: [SearchResultsComponent],
  template: `
    <texera-search-results
      [viewMode]="viewMode"
      [isPrivateSearch]="isPrivateSearch"
      [editable]="true"
      [currentUid]="currentUid"
      [cardTemplate]="cardTemplateInput"
      (deleted)="deletedEntries.push($event)"
      (duplicated)="duplicatedEntries.push($event)"
      (refresh)="refreshCount = refreshCount + 1"
      (notifyWorkflow)="notifyCount = notifyCount + 1">
    </texera-search-results>
    <ng-template
      #card
      let-entry
      ><span class="card-entry">card:{{ entry.name }}</span></ng-template
    >
  `,
})
class SearchResultsTemplateHostComponent {
  @ViewChild("card", { static: true }) cardTemplate!: TemplateRef<{ $implicit: DashboardEntry }>;
  @ViewChild(SearchResultsComponent, { static: true }) results!: SearchResultsComponent;
  viewMode: SearchResultsViewMode = "list";
  isPrivateSearch = true;
  currentUid: number | undefined = 7;
  cardTemplateInput?: TemplateRef<{ $implicit: DashboardEntry }>;
  deletedEntries: DashboardEntry[] = [];
  duplicatedEntries: DashboardEntry[] = [];
  refreshCount = 0;
  notifyCount = 0;
}

/**
 * Deliberately a separate suite from the one above, with its own TestBed and no
 * TestBed.overrideComponent: an override makes Angular re-JIT SearchResultsComponent
 * from its decorator metadata, and the recompiled template loses the source map that
 * attributes executed bindings back to search-results.component.html (issue #7458).
 * Without the override the real ListItemComponent renders, so the child's outputs can
 * be fired from the rendered DOM and the template's own bindings are actually counted.
 */
describe("SearchResultsComponent rendered template", () => {
  let fixture: ComponentFixture<SearchResultsTemplateHostComponent>;
  let host: SearchResultsTemplateHostComponent;

  /** A workflow entry complete enough for the real ListItemComponent to render it. */
  const workflowEntry = (id: number, name: string, checked = false, ownerId?: number): DashboardEntry =>
    ({
      id,
      name,
      description: `description of ${name}`,
      type: "workflow",
      workflow: { isOwner: true },
      accessibleUserIds: [7],
      likeCount: 0,
      viewCount: 0,
      isLiked: false,
      size: 0,
      checked,
      ownerId,
    }) as unknown as DashboardEntry;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [SearchResultsTemplateHostComponent, NoopAnimationsModule, HttpClientTestingModule, RouterTestingModule],
      providers: [{ provide: UserService, useClass: StubUserService }, NzModalService, ...commonTestProviders],
    }).compileComponents();

    fixture = TestBed.createComponent(SearchResultsTemplateHostComponent);
    host = fixture.componentInstance;
  });

  afterEach(() => {
    fixture?.destroy();
    document.querySelectorAll(".cdk-overlay-container").forEach(c => (c.innerHTML = ""));
  });

  const el = (): HTMLElement => fixture.nativeElement as HTMLElement;

  /** The only load-more button in the template; the list items render buttons of their own. */
  const loadMoreButton = (): HTMLButtonElement | null => el().querySelector(".load-more button");

  const renderedNames = (): string[] =>
    Array.from(el().querySelectorAll(".resource-name")).map(node => (node.textContent ?? "").trim());

  const renderedCards = (): string[] =>
    Array.from(el().querySelectorAll(".card-entry")).map(node => (node.textContent ?? "").trim());

  /** Loads the first page through the component's real API and renders it. */
  const loadFirstPage = async (loadMoreFunction: LoadMoreFunction): Promise<void> => {
    host.results.reset(loadMoreFunction);
    await host.results.loadMore();
    fixture.detectChanges();
  };

  /**
   * Lets the click handler's promise chain settle. fixture.whenStable() cannot be used here:
   * the cdk-virtual-scroll-viewport in the list view keeps the zone permanently unstable.
   */
  const settle = (): Promise<void> => new Promise(resolve => setTimeout(resolve, 0));

  const clickLoadMore = async (): Promise<void> => {
    loadMoreButton()!.click();
    await settle();
    fixture.detectChanges();
  };

  describe("list view", () => {
    it("renders one list item per entry and appends the next page when Load more is clicked", async () => {
      const loadMoreFunction = vi
        .fn<LoadMoreFunction>()
        .mockResolvedValueOnce({ entries: [workflowEntry(1, "alpha"), workflowEntry(2, "beta")], more: true })
        .mockResolvedValueOnce({ entries: [workflowEntry(3, "gamma")], more: false });

      await loadFirstPage(loadMoreFunction);

      expect(el().querySelectorAll("texera-list-item").length).toBe(2);
      expect(renderedNames()).toEqual(["alpha", "beta"]);
      expect(loadMoreButton()?.textContent?.trim()).toBe("Load more");

      await clickLoadMore();

      expect(loadMoreFunction).toHaveBeenCalledTimes(2);
      // the second page starts where the first one ended
      expect(loadMoreFunction).toHaveBeenLastCalledWith(2, 20);
      expect(renderedNames()).toEqual(["alpha", "beta", "gamma"]);
      // the last page reported more: false, so the button is gone
      expect(loadMoreButton()).toBeNull();
    });

    it("hides Load more while a page is still in flight", async () => {
      let releasePage!: (page: { entries: DashboardEntry[]; more: boolean }) => void;
      const pending = new Promise<{ entries: DashboardEntry[]; more: boolean }>(resolve => (releasePage = resolve));

      await loadFirstPage(vi.fn<LoadMoreFunction>().mockResolvedValue({ entries: [], more: true }));
      expect(loadMoreButton()).not.toBeNull();

      host.results.loadMoreFunction = () => pending;
      const inFlight = host.results.loadMore();
      fixture.detectChanges();
      expect(loadMoreButton()).toBeNull();

      releasePage({ entries: [workflowEntry(1, "alpha")], more: true });
      await inFlight;
      fixture.detectChanges();
      expect(loadMoreButton()).not.toBeNull();
    });

    it("forwards each list item's deleted and duplicated outputs with that item's own entry", async () => {
      const alpha = workflowEntry(1, "alpha");
      const beta = workflowEntry(2, "beta");
      await loadFirstPage(vi.fn<LoadMoreFunction>().mockResolvedValue({ entries: [alpha, beta], more: false }));

      // the second item's Copy button, so an entry mix-up cannot pass unnoticed
      const copyButtons = el().querySelectorAll<HTMLButtonElement>('button[title="Copy"]');
      expect(copyButtons.length).toBe(2);
      copyButtons[1].click();
      fixture.detectChanges();

      expect(host.duplicatedEntries).toEqual([beta]);
      expect(host.duplicatedEntries[0]).toBe(beta);

      // delete is behind a popconfirm overlay, so fire it on the real child instead
      const listItems = fixture.debugElement.queryAll(By.directive(ListItemComponent));
      expect(listItems.length).toBe(2);
      (listItems[1].componentInstance as ListItemComponent).deleted.emit();
      fixture.detectChanges();

      expect(host.deletedEntries).toEqual([beta]);
      expect(host.deletedEntries[0]).toBe(beta);
    });

    it("forwards a list item's refresh output", async () => {
      await loadFirstPage(
        vi
          .fn<LoadMoreFunction>()
          .mockResolvedValue({ entries: [workflowEntry(1, "alpha"), workflowEntry(2, "beta")], more: false })
      );

      const listItems = fixture.debugElement.queryAll(By.directive(ListItemComponent));
      expect(host.refreshCount).toBe(0);

      (listItems[0].componentInstance as ListItemComponent).refresh.emit();
      (listItems[1].componentInstance as ListItemComponent).refresh.emit();
      fixture.detectChanges();

      expect(host.refreshCount).toBe(2);
    });

    it("notifies only once ticking a checkbox leaves every entry selected", async () => {
      // alpha starts selected, beta does not
      const alpha = workflowEntry(1, "alpha", true);
      const beta = workflowEntry(2, "beta", false);
      await loadFirstPage(vi.fn<LoadMoreFunction>().mockResolvedValue({ entries: [alpha, beta], more: false }));

      const checkboxes = el().querySelectorAll<HTMLInputElement>("input.large-checkbox");
      expect(checkboxes.length).toBe(2);

      // ticking beta completes the selection
      checkboxes[1].click();
      fixture.detectChanges();
      expect(beta.checked).toBe(true);
      expect(host.notifyCount).toBe(1);

      // un-ticking alpha breaks it again, so nothing further is notified
      checkboxes[0].click();
      fixture.detectChanges();
      expect(alpha.checked).toBe(false);
      expect(host.notifyCount).toBe(1);
    });

    it("passes currentUid down, so the owner badge follows the current user", async () => {
      // alpha belongs to the host's current user (7), beta to someone else (99)
      await loadFirstPage(
        vi.fn<LoadMoreFunction>().mockResolvedValue({
          entries: [workflowEntry(1, "alpha", false, 7), workflowEntry(2, "beta", false, 99)],
          more: false,
        })
      );

      /** Names of the entries whose list item shows the owner badge. */
      const ownerBadgedNames = (): string[] =>
        Array.from(el().querySelectorAll("texera-list-item"))
          .filter(item => item.querySelector(".owner-badge") !== null)
          .map(item => (item.querySelector(".resource-name")?.textContent ?? "").trim());

      expect(ownerBadgedNames()).toEqual(["alpha"]);

      // re-pointing the current user moves the badge, so the binding is not a constant
      host.currentUid = 99;
      fixture.detectChanges();

      expect(ownerBadgedNames()).toEqual(["beta"]);
    });

    it("passes isPrivateSearch down to each list item", async () => {
      host.isPrivateSearch = false;
      await loadFirstPage(
        vi.fn<LoadMoreFunction>().mockResolvedValue({ entries: [workflowEntry(1, "alpha")], more: false })
      );

      // the checkbox and the button group are private-search only
      expect(el().querySelectorAll("input.large-checkbox").length).toBe(0);
      expect(el().querySelectorAll('button[title="Copy"]').length).toBe(0);
      expect(el().querySelectorAll("texera-list-item").length).toBe(1);
    });
  });

  describe("card view", () => {
    beforeEach(() => {
      host.viewMode = "card";
      host.cardTemplateInput = host.cardTemplate;
    });

    it("renders the supplied card template per entry and appends the next page on Load more", async () => {
      const loadMoreFunction = vi
        .fn<LoadMoreFunction>()
        .mockResolvedValueOnce({ entries: [workflowEntry(1, "alpha"), workflowEntry(2, "beta")], more: true })
        .mockResolvedValueOnce({ entries: [workflowEntry(3, "gamma")], more: false });

      await loadFirstPage(loadMoreFunction);

      expect(renderedCards()).toEqual(["card:alpha", "card:beta"]);
      // the card view is used instead of, not alongside, the list view
      expect(el().querySelectorAll("texera-list-item").length).toBe(0);
      expect(loadMoreButton()?.textContent?.trim()).toBe("Load more");

      await clickLoadMore();

      expect(loadMoreFunction).toHaveBeenLastCalledWith(2, 20);
      expect(renderedCards()).toEqual(["card:alpha", "card:beta", "card:gamma"]);
      expect(loadMoreButton()).toBeNull();
    });

    it("renders nothing when no card template is supplied", async () => {
      host.cardTemplateInput = undefined;

      await loadFirstPage(
        vi.fn<LoadMoreFunction>().mockResolvedValue({ entries: [workflowEntry(1, "alpha")], more: true })
      );

      expect(renderedCards()).toEqual([]);
      expect(el().querySelector(".card-grid")).toBeNull();
      // the list view is not used as a fallback either
      expect(el().querySelectorAll("texera-list-item").length).toBe(0);
      expect(loadMoreButton()).toBeNull();
    });
  });
});

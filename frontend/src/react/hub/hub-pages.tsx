/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { useCallback, useEffect, useMemo, useRef, useState, type FormEvent, type ReactNode } from "react";
import type { ApiClient } from "../core/api-client";
import type { SessionUser } from "../core/auth";
import { RichText } from "../common/rich-text";
import {
  HubRepository,
  activityKey,
  type HubActivityMap,
  type HubItem,
  type HubKind,
  type WorkflowRecord,
  type WorkflowSearch,
  type WorkflowSort,
} from "./hub-repository";
import "../workflows/workflows.css";
import "./hub.css";

interface HubPageProps {
  api: ApiClient;
  user?: SessionUser;
  onOpen(kind: HubKind, id: number): void;
}
function message(error: unknown): string {
  return error instanceof Error ? error.message : "The Hub request failed.";
}
function split(value: string): string[] {
  return value
    .split(/[,;\n]/)
    .map(part => part.trim())
    .filter(Boolean);
}
const titles: Record<HubKind, string> = { workflow: "Workflows", dataset: "Datasets", model: "Models" };

export function HubSearchPage({
  api,
  user,
  kind,
  onOpen,
  onOpenPrivate,
  scope = "public",
  actions,
  renderActions,
  refreshKey,
  query,
  onQuerySubmit,
}: HubPageProps & {
  kind?: HubKind;
  onOpenPrivate?: HubPageProps["onOpen"];
  scope?: "public" | "private";
  actions?: ReactNode;
  renderActions?: (item: HubItem) => ReactNode;
  refreshKey?: string | number;
  query?: string;
  onQuerySubmit?: (query: string) => void;
}) {
  const repository = useMemo(() => new HubRepository(api), [api]);
  const [items, setItems] = useState<HubItem[]>([]);
  const [activity, setActivity] = useState<HubActivityMap>({});
  const [search, setSearch] = useState(query ?? "");
  const incomingQuery = useRef(query),
    appliedQuery = useRef(query ?? "");
  incomingQuery.current = query;
  const [owners, setOwners] = useState("");
  const [operator, setOperator] = useState("");
  const [createdStart, setCreatedStart] = useState("");
  const [createdEnd, setCreatedEnd] = useState("");
  const [orderBy, setOrderBy] = useState<WorkflowSort>(
    kind === "workflow" || !kind ? "EditTimeDesc" : "CreateTimeDesc"
  );
  const [view, setView] = useState<"list" | "card">(() =>
    localStorage.getItem("texera.hub.dataset.viewMode") === "card" ? "card" : "list"
  );
  const [ownerOptions, setOwnerOptions] = useState<string[]>([]);
  const [more, setMore] = useState(false);
  const nextStart = useRef(0);
  const [mismatch, setMismatch] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>();
  const [pendingLike, setPendingLike] = useState<string>();
  const sequence = useRef(0);
  const controller = useRef<AbortController | undefined>(undefined);
  const activeQuery = useRef<WorkflowSearch>({});
  const load = useCallback(
    async (reset = true, query = activeQuery.current) => {
      controller.current?.abort();
      const abort = new AbortController();
      controller.current = abort;
      const request = ++sequence.current;
      if (reset) activeQuery.current = query;
      setLoading(true);
      setError(undefined);
      try {
        const page = await repository.search(
          kind,
          { ...query, scope, start: reset ? 0 : nextStart.current },
          abort.signal
        );
        if (request !== sequence.current || abort.signal.aborted) return;
        setItems(previous =>
          reset
            ? page.items
            : [
                ...previous,
                ...page.items.filter(item => !previous.some(existing => activityKey(item) === activityKey(existing))),
              ]
        );
        if (reset) setActivity({});
        setMore(page.more);
        nextStart.current = page.nextStart;
        setMismatch(page.hasMismatch);
        void repository
          .activity(page.items, user?.uid, abort.signal)
          .then(values => {
            if (request === sequence.current && !abort.signal.aborted)
              setActivity(previous => (reset ? values : { ...previous, ...values }));
          })
          .catch(reason => {
            if (!abort.signal.aborted) setError(message(reason));
          });
      } catch (reason) {
        if (!abort.signal.aborted) setError(message(reason));
      } finally {
        if (request === sequence.current) setLoading(false);
      }
    },
    [repository, kind, scope, user?.uid]
  );
  useEffect(() => {
    const sort: WorkflowSort = kind === "workflow" || !kind ? "EditTimeDesc" : "CreateTimeDesc";
    setOrderBy(sort);
    const initialQuery = incomingQuery.current;
    if (initialQuery !== undefined) {
      setSearch(initialQuery);
      appliedQuery.current = initialQuery;
    }
    void load(true, { orderBy: sort, ...(initialQuery !== undefined ? { keywords: split(initialQuery) } : {}) });
    const abort = new AbortController();
    if (kind && scope === "public")
      void repository
        .owners(kind, abort.signal)
        .then(values => setOwnerOptions(Array.isArray(values) ? values : []))
        .catch(() => undefined);
    return () => {
      controller.current?.abort();
      abort.abort();
    };
  }, [repository, kind, user?.uid, scope, refreshKey, load]);
  useEffect(() => {
    if (query === undefined || query === appliedQuery.current) return;
    setSearch(query);
    appliedQuery.current = query;
    void load(true, { ...activeQuery.current, keywords: split(query) });
  }, [query, load]);
  function submit(event: FormEvent) {
    event.preventDefault();
    appliedQuery.current = search;
    void load(true, {
      keywords: split(search),
      owners: split(owners),
      operators: split(operator),
      orderBy,
      createDateStart: createdStart || undefined,
      createDateEnd: createdEnd || undefined,
    });
    onQuerySubmit?.(search);
  }
  function chooseView(next: "list" | "card") {
    setView(next);
    localStorage.setItem("texera.hub.dataset.viewMode", next);
  }
  function open(item: HubItem) {
    const details = activity[activityKey(item)];
    const accessible = user && (item.ownerId === user.uid || details?.accessibleUserIds.includes(user.uid));
    if (accessible && onOpenPrivate) onOpenPrivate(item.kind, item.id);
    else onOpen(item.kind, item.id);
  }
  async function toggleLike(item: HubItem) {
    if (!user) return;
    const key = activityKey(item),
      previous = activity[key];
    if (!previous) return;
    setPendingLike(key);
    setError(undefined);
    try {
      const result = await repository.toggleLike(item.kind, item.id, previous.isLiked);
      setActivity(current => ({ ...current, [key]: { ...previous, isLiked: result.liked, likes: result.likeCount } }));
    } catch (reason) {
      setError(message(reason));
    } finally {
      setPendingLike(undefined);
    }
  }
  return (
    <main className="workflow-page hub-page">
      <header className="workflow-page-heading">
        <div>
          <h1>
            {kind ? (scope === "private" ? titles[kind] : `Public ${titles[kind].toLowerCase()}`) : "Search Texera"}
          </h1>
          <p>Discover shared workflows, datasets and models.</p>
        </div>
        <div>
          {actions}
          <button
            type="button"
            aria-pressed={view === "list"}
            onClick={() => chooseView("list")}>
            List view
          </button>{" "}
          <button
            type="button"
            aria-pressed={view === "card"}
            onClick={() => chooseView("card")}>
            Card view
          </button>
        </div>
      </header>
      <form
        className="workflow-search"
        onSubmit={submit}>
        <label>
          {scope === "private" ? "Search resources" : "Search public resources"}
          <input
            value={search}
            onChange={event => setSearch(event.target.value)}
            placeholder="Keywords, separated by commas"
          />
        </label>
        <label>
          {scope === "private" ? "Sort resources" : "Sort public resources"}
          <select
            value={orderBy}
            onChange={event => setOrderBy(event.target.value as WorkflowSort)}>
            <option value="NameAsc">Name A–Z</option>
            <option value="NameDesc">Name Z–A</option>
            <option value="CreateTimeDesc">Recently created</option>
            {(kind === "workflow" || !kind) && (
              <>
                <option value="EditTimeDesc">Recently edited</option>
                <option value="ExecutionTimeDesc">Recently executed</option>
              </>
            )}
          </select>
        </label>
        <button
          type="submit"
          disabled={loading}>
          Search
        </button>
        <details className="workflow-filters">
          <summary>Filters</summary>
          <div>
            <label>
              Public owners
              <input
                list="hub-owner-options"
                value={owners}
                onChange={event => setOwners(event.target.value)}
              />
            </label>
            <datalist id="hub-owner-options">
              {ownerOptions.map(owner => (
                <option
                  key={owner}
                  value={owner}
                />
              ))}
            </datalist>
            {(kind === "workflow" || !kind) && (
              <label>
                Operator types
                <input
                  value={operator}
                  onChange={event => setOperator(event.target.value)}
                />
              </label>
            )}
            <label>
              Created from
              <input
                type="date"
                value={createdStart}
                onChange={event => setCreatedStart(event.target.value)}
              />
            </label>
            <label>
              Created until
              <input
                type="date"
                value={createdEnd}
                onChange={event => setCreatedEnd(event.target.value)}
              />
            </label>
          </div>
        </details>
      </form>
      {error && <p role="alert">{error}</p>}
      {mismatch && <p role="status">Some repositories are unavailable and have been omitted.</p>}
      {loading && <p role="status">Searching {scope === "public" ? "public " : ""}resources…</p>}
      {!loading && !items.length && !error && (
        <p className="workflow-empty">No {scope === "public" ? "public " : ""}resources match your search.</p>
      )}
      <section
        className={`workflow-results workflow-results-${view}`}
        aria-label="Public search results"
        aria-busy={loading}>
        {items.map(item => (
          <HubCard
            key={activityKey(item)}
            item={item}
            activity={activity[activityKey(item)]}
            card={view === "card"}
            onOpen={() => open(item)}
            like={() => void toggleLike(item)}
            canLike={Boolean(user)}
            pending={pendingLike === activityKey(item)}
            actions={renderActions?.(item)}
          />
        ))}
      </section>
      {more && (
        <button
          className="workflow-load-more"
          type="button"
          disabled={loading}
          onClick={() => void load(false)}>
          Load more
        </button>
      )}
    </main>
  );
}

function HubCard({
  item,
  activity,
  card,
  onOpen,
  like,
  canLike,
  pending,
  actions,
}: {
  item: HubItem;
  activity?: HubActivityMap[string];
  card?: boolean;
  onOpen(): void;
  like?: () => void;
  canLike?: boolean;
  pending?: boolean;
  actions?: ReactNode;
}) {
  return (
    <article className="workflow-entry hub-card">
      {card && (
        <img
          className="workflow-cover"
          src={item.cover || "/assets/card_background.svg"}
          alt=""
        />
      )}
      <div className="workflow-entry-description">
        <button
          type="button"
          className="workflow-name"
          aria-label={`Open ${item.name}`}
          onClick={onOpen}>
          {item.name}
        </button>
        <RichText
          content={item.description || "No description available"}
          collapsedHeight={160}
        />
        <small>
          {item.kind} ·{" "}
          {activity?.ownerName || item.ownerName || item.ownerEmail || (item.ownerId ? `Owner #${item.ownerId}` : "")}
        </small>
        {activity && (
          <small className="workflow-activity">
            {activity.views} views · {activity.clones} clones · {activity.likes} likes
            {activity.size !== undefined ? ` · ${activity.size.toLocaleString()} bytes` : ""}
          </small>
        )}
      </div>
      {actions}
      {like && (
        <button
          type="button"
          disabled={!canLike || pending || !activity}
          aria-label={`${activity?.isLiked ? "Unlike" : "Like"} ${item.name}`}
          onClick={like}>
          {activity?.isLiked ? "Unlike" : "Like"}
        </button>
      )}
    </article>
  );
}

export function HubHomePage({ api, user, onOpen, onBrowse }: HubPageProps & { onBrowse(kind: HubKind): void }) {
  const repository = useMemo(() => new HubRepository(api), [api]);
  const [counts, setCounts] = useState<Partial<Record<HubKind, number>>>({});
  const [sections, setSections] = useState<{ title: string; kind: HubKind; items: HubItem[] }[]>([]);
  const [error, setError] = useState<string>();
  const [loading, setLoading] = useState(true);
  const [retry, setRetry] = useState(0);
  useEffect(() => {
    const abort = new AbortController();
    setLoading(true);
    setError(undefined);
    void (async () => {
      try {
        const kinds: HubKind[] = ["workflow", "dataset", "model"];
        const totals = await Promise.all(kinds.map(kind => repository.count(kind, abort.signal)));
        const [workflows, datasets, models] = await Promise.all([
          repository.tops("workflow", ["like", "clone"], user?.uid, 8, abort.signal),
          repository.tops("dataset", ["like"], user?.uid, 8, abort.signal),
          repository.tops("model", ["like"], user?.uid, 8, abort.signal),
        ]);
        if (abort.signal.aborted) return;
        setCounts(Object.fromEntries(kinds.map((kind, index) => [kind, totals[index]])));
        setSections([
          { title: "Most loved workflows", kind: "workflow", items: workflows.like },
          { title: "Most cloned workflows", kind: "workflow", items: workflows.clone },
          { title: "Most loved datasets", kind: "dataset", items: datasets.like },
          { title: "Most loved models", kind: "model", items: models.like },
        ]);
      } catch (reason) {
        if (!abort.signal.aborted) setError(message(reason));
      } finally {
        if (!abort.signal.aborted) setLoading(false);
      }
    })();
    return () => abort.abort();
  }, [repository, user?.uid, retry]);
  return (
    <main className="workflow-page hub-page">
      <header className="hub-hero">
        <h1>Discover what you can build together</h1>
        <p>Explore workflows, datasets and models shared by the Texera community.</p>
        <nav aria-label="Browse public resources">
          {(["workflow", "dataset", "model"] as HubKind[]).map(kind => (
            <button
              type="button"
              key={kind}
              onClick={() => onBrowse(kind)}>
              {titles[kind]}
              {counts[kind] !== undefined ? ` (${counts[kind]})` : ""}
            </button>
          ))}
        </nav>
      </header>
      {loading && <p role="status">Loading community resources…</p>}
      {error && (
        <p role="alert">
          {error}{" "}
          <button
            type="button"
            onClick={() => setRetry(retry + 1)}>
            Retry
          </button>
        </p>
      )}
      {sections.map(section => (
        <section
          className="hub-home-section"
          key={section.title}>
          <header>
            <h2>{section.title}</h2>
            <button
              type="button"
              onClick={() => onBrowse(section.kind)}>
              Browse all {titles[section.kind].toLowerCase()}
            </button>
          </header>
          <div className="workflow-results workflow-results-card">
            {section.items.map(item => (
              <HubCard
                key={activityKey(item)}
                item={item}
                card
                onOpen={() => onOpen(item.kind, item.id)}
              />
            ))}
          </div>
          {!section.items.length && <p>No public resources in this section yet.</p>}
        </section>
      ))}
    </main>
  );
}

export interface HubWorkflowDetailPageProps {
  api: ApiClient;
  user?: SessionUser;
  wid: number;
  onClone(wid: number, defaultView?: "CANVAS" | "FORM"): void;
  onBack(): void;
  renderPreview?: (workflow: WorkflowRecord) => ReactNode;
}
export function HubWorkflowDetailPage({ api, user, wid, onClone, onBack, renderPreview }: HubWorkflowDetailPageProps) {
  const repository = useMemo(() => new HubRepository(api), [api]);
  const [detail, setDetail] = useState<{ workflow: WorkflowRecord; ownerName: string }>();
  const [activity, setActivity] = useState<HubActivityMap[string]>();
  const [views, setViews] = useState<number>();
  const [preciseViews, setPreciseViews] = useState(false);
  const [loading, setLoading] = useState(true);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string>();
  useEffect(() => {
    const abort = new AbortController();
    setLoading(true);
    setError(undefined);
    setDetail(undefined);
    setActivity(undefined);
    setViews(undefined);
    if (!Number.isSafeInteger(wid) || wid <= 0) {
      setError("A valid workflow ID is required.");
      setLoading(false);
      return () => abort.abort();
    }
    void repository
      .workflowDetail(wid, abort.signal)
      .then(value => {
        if (!abort.signal.aborted) setDetail(value);
      })
      .catch(reason => {
        if (!abort.signal.aborted) setError(message(reason));
      })
      .finally(() => {
        if (!abort.signal.aborted) setLoading(false);
      });
    void repository
      .recordView("workflow", wid, user?.uid ?? 0)
      .then(count => {
        if (!abort.signal.aborted) setViews(count);
      })
      .catch(reason => {
        if (!abort.signal.aborted) setError(message(reason));
      });
    void repository
      .activity([{ kind: "workflow", id: wid, name: "", description: "" }], user?.uid, abort.signal)
      .then(values => {
        if (!abort.signal.aborted) setActivity(values[`workflow:${wid}`]);
      })
      .catch(reason => {
        if (!abort.signal.aborted) setError(message(reason));
      });
    return () => abort.abort();
  }, [repository, wid, user?.uid]);
  async function perform(action: () => Promise<void>) {
    setPending(true);
    setError(undefined);
    try {
      await action();
    } catch (reason) {
      setError(message(reason));
    } finally {
      setPending(false);
    }
  }
  const canClone = user?.role === "ADMIN" || user?.role === "REGULAR";
  return (
    <main className="workflow-page hub-page">
      <button
        type="button"
        onClick={onBack}>
        Back to public workflows
      </button>
      {loading && <p role="status">Loading published workflow…</p>}
      {error && <p role="alert">{error}</p>}
      {detail && (
        <>
          <header className="workflow-page-heading">
            <div>
              <h1>{detail.workflow.name}</h1>
              <p>
                Workflow #{wid} · Created by {detail.ownerName}
              </p>
            </div>
            <div className="workflow-toolbar">
              <button
                type="button"
                onClick={() => setPreciseViews(!preciseViews)}>
                {views !== undefined
                  ? preciseViews
                    ? views.toLocaleString()
                    : Intl.NumberFormat(undefined, { notation: "compact" }).format(views)
                  : "…"}{" "}
                views
              </button>
              <button
                type="button"
                aria-label={activity?.isLiked ? "Unlike workflow" : "Like workflow"}
                disabled={!user || pending}
                onClick={() =>
                  void perform(async () => {
                    const result = await repository.toggleLike("workflow", wid, activity?.isLiked ?? false);
                    setActivity(previous => ({
                      views: previous?.views ?? 0,
                      clones: previous?.clones ?? 0,
                      accessibleUserIds: previous?.accessibleUserIds ?? [],
                      ...previous,
                      likes: result.likeCount,
                      isLiked: result.liked,
                    }));
                  })
                }>
                {activity?.likes ?? "…"} likes
              </button>
              <span>{activity?.clones ?? "…"} clones</span>
              <button
                type="button"
                className="primary"
                aria-label="Clone workflow"
                disabled={!canClone || pending}
                onClick={() =>
                  void perform(async () => {
                    const clone = await repository.cloneWorkflow(wid);
                    onClone(clone, detail.workflow.defaultView === "FORM" ? "FORM" : "CANVAS");
                  })
                }>
                Clone workflow
              </button>
            </div>
          </header>
          <section className="hub-workflow-description">
            <h2>Description</h2>
            <RichText
              content={detail.workflow.description || "No description available"}
              collapsedHeight={320}
            />
          </section>
          <section>
            <h2>Preview</h2>
            {renderPreview ? renderPreview(detail.workflow) : <ReadonlyWorkflowPreview workflow={detail.workflow} />}
          </section>
        </>
      )}
    </main>
  );
}

export function ReadonlyWorkflowPreview({ workflow }: { workflow: WorkflowRecord }) {
  const operators = workflow.content?.operators ?? [];
  const positions = workflow.content?.operatorPositions ?? {};
  const points = operators.map((operator, index) => ({
    operator,
    position: positions[operator.operatorID] ?? { x: (index % 4) * 230, y: Math.floor(index / 4) * 150 },
  }));
  const minX = Math.min(0, ...points.map(point => point.position.x)) - 40;
  const minY = Math.min(0, ...points.map(point => point.position.y)) - 40;
  const width = Math.max(600, ...points.map(point => point.position.x - minX + 220));
  const height = Math.max(350, ...points.map(point => point.position.y - minY + 120));
  const [zoom, setZoom] = useState(1);
  const byId = new Map(points.map(point => [point.operator.operatorID, point.position]));
  return (
    <div className="hub-graph-preview">
      <nav aria-label="Preview zoom">
        <button
          type="button"
          onClick={() => setZoom(Math.min(3, zoom + 0.25))}>
          Zoom in
        </button>
        <button
          type="button"
          onClick={() => setZoom(Math.max(0.5, zoom - 0.25))}>
          Zoom out
        </button>
        <button
          type="button"
          onClick={() => setZoom(1)}>
          Fit graph
        </button>
      </nav>
      <div className="hub-graph-scroll">
        <svg
          role="img"
          aria-label="Workflow preview"
          viewBox={`${minX} ${minY} ${width} ${height}`}
          style={{ minWidth: `${zoom * 100}%`, minHeight: `${zoom * 350}px` }}>
          <defs>
            <marker
              id="hub-arrow"
              markerWidth="9"
              markerHeight="9"
              refX="8"
              refY="4"
              orient="auto">
              <path
                d="M0,0 L8,4 L0,8"
                fill="#617a99"
              />
            </marker>
          </defs>
          {(workflow.content?.links ?? []).map(link => {
            const source = byId.get(link.source.operatorID),
              target = byId.get(link.target.operatorID);
            return source && target ? (
              <path
                key={link.linkID}
                d={`M${source.x + 170},${source.y + 30} C${source.x + 215},${source.y + 30} ${target.x - 45},${target.y + 30} ${target.x},${target.y + 30}`}
                fill="none"
                stroke="#617a99"
                strokeWidth="2"
                markerEnd="url(#hub-arrow)"
              />
            ) : null;
          })}
          {points.map(({ operator, position }) => (
            <g
              key={operator.operatorID}
              transform={`translate(${position.x},${position.y})`}>
              <title>{JSON.stringify(operator.operatorProperties, null, 2)}</title>
              <rect
                width="170"
                height="60"
                rx="9"
                fill={operator.isDisabled ? "#edf0f4" : "#f1f7ff"}
                stroke="#8ca8c8"
              />
              <text
                x="12"
                y="27"
                fontSize="13"
                fill="#173d6c">
                {(operator.customDisplayName || operator.operatorType).slice(0, 23)}
              </text>
              <text
                x="12"
                y="45"
                fontSize="10"
                fill="#53657d">
                {operator.operatorType.slice(0, 26)}
              </text>
            </g>
          ))}
          {!operators.length && (
            <text
              x="20"
              y="70"
              fill="#617a99">
              This workflow has no operators.
            </text>
          )}
        </svg>
      </div>
    </div>
  );
}

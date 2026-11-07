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
import { useCallback, useEffect, useRef, useState } from "react";
import type { AdminExecution, AdminUser, ExecutionQuery, UserUpdate } from "./api";
import { Dialog, LoadState, Notices, useOperation, useResource, type ResourcePageProps } from "./pages";
import { QuotaPage } from "./quota";

const ROLES: AdminUser["role"][] = ["INACTIVE", "REGULAR", "ADMIN", "RESTRICTED"];

export function FeedbackPage({ resources, onNotice, userId }: ResourcePageProps & { userId?: number }) {
  const load = useCallback((signal: AbortSignal) => resources.feedback({ signal }, userId), [resources, userId]);
  const state = useResource(load);
  const operation = useOperation(onNotice);
  const [message, setMessage] = useState("");
  return (
    <main className="resource-page">
      <header>
        <h1>{userId === undefined ? "Feedback" : `Feedback for user ${userId}`}</h1>
      </header>
      <LoadState
        loading={state.loading}
        error={state.error}
        retry={state.refresh}
      />
      <Notices
        error={operation.error}
        message={operation.message}
      />
      {userId === undefined && (
        <form
          className="resource-panel"
          onSubmit={event => {
            event.preventDefault();
            if (!message.trim()) {
              operation.fail("Feedback cannot be empty.");
              return;
            }
            void operation.run(
              () => resources.submitFeedback(message.trim()),
              () => {
                setMessage("");
                state.refresh();
              },
              "Thank you for your feedback!"
            );
          }}>
          <label>
            Feedback message
            <textarea
              rows={5}
              value={message}
              disabled={operation.busy}
              onChange={event => setMessage(event.target.value)}
            />
          </label>
          <footer>
            <button
              type="submit"
              disabled={operation.busy}>
              Submit feedback
            </button>
          </footer>
        </form>
      )}
      {state.data &&
        (state.data.length ? (
          <table>
            <thead>
              <tr>
                <th>Date</th>
                <th>Message</th>
              </tr>
            </thead>
            <tbody>
              {state.data.map(item => (
                <tr key={item.fid}>
                  <td>{new Date(item.creationTime).toLocaleString()}</td>
                  <td style={{ whiteSpace: "pre-wrap" }}>{item.message}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p>No feedback yet.</p>
        ))}
    </main>
  );
}

export function AdminUsersPage({
  resources,
  onNotice,
  currentUid,
  activeTimeInMinutes = 5,
}: ResourcePageProps & { currentUid?: number; activeTimeInMinutes?: number }) {
  const load = useCallback((signal: AbortSignal) => resources.adminUsers({ signal }), [resources]);
  const countsLoad = useCallback((signal: AbortSignal) => resources.feedbackCounts({ signal }), [resources]);
  const state = useResource(load);
  const counts = useResource(countsLoad);
  const operation = useOperation(onNotice);
  const [search, setSearch] = useState("");
  const [searchField, setSearchField] = useState<"name" | "email" | "comment">("name");
  const [roles, setRoles] = useState<string[]>([]);
  const [sort, setSort] = useState("uid");
  const [descending, setDescending] = useState(false);
  const [now, setNow] = useState(Date.now());
  const [draft, setDraft] = useState<UserUpdate>();
  const [quotaUser, setQuotaUser] = useState<AdminUser>();
  const [feedbackUser, setFeedbackUser] = useState<AdminUser>();
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);
  const isActive = (item: AdminUser) => !!item.lastLogin && now - item.lastLogin * 1000 < activeTimeInMinutes * 60000;
  const feedbackCounts = new Map((counts.data ?? []).map(item => [item.uid, item.count]));
  const users = (state.data ?? [])
    .filter(
      item =>
        (item[searchField] ?? "").toLowerCase().includes(search.trim().toLowerCase()) &&
        (!roles.length || roles.includes(item.role))
    )
    .sort((left, right) => {
      let order: number;
      if (sort === "active") order = Number(isActive(right)) - Number(isActive(left));
      else if (sort === "uid" || sort === "accountCreation") order = (left[sort] ?? 0) - (right[sort] ?? 0);
      else
        order = String(left[sort as keyof AdminUser] ?? "").localeCompare(String(right[sort as keyof AdminUser] ?? ""));
      return (order || left.uid - right.uid) * (descending ? -1 : 1);
    });
  return (
    <main className="resource-page">
      <header>
        <h1>Users</h1>
        <button
          type="button"
          disabled={operation.busy}
          onClick={() =>
            void operation.run(
              () => resources.addUser(),
              () => state.refresh(),
              "User added."
            )
          }>
          Add user
        </button>
      </header>
      <LoadState
        loading={state.loading}
        error={state.error}
        retry={state.refresh}
      />
      <Notices
        error={operation.error}
        message={operation.message}
      />
      {counts.error && (
        <p>
          Feedback counts are unavailable.{" "}
          <button
            type="button"
            onClick={counts.refresh}>
            Retry feedback counts
          </button>
        </p>
      )}
      <section className="resource-panel resource-input-grid">
        <label>
          Search field
          <select
            value={searchField}
            onChange={event => setSearchField(event.target.value as typeof searchField)}>
            <option value="name">Name</option>
            <option value="email">Email</option>
            <option value="comment">Comment</option>
          </select>
        </label>
        <label>
          Search users
          <input
            value={search}
            onChange={event => setSearch(event.target.value)}
          />
        </label>
        <label>
          Role filter
          <select
            multiple
            value={roles}
            onChange={event => setRoles([...event.target.selectedOptions].map(option => option.value).filter(Boolean))}>
            <option value="">All roles</option>
            {ROLES.map(role => (
              <option key={role}>{role}</option>
            ))}
          </select>
        </label>
        <label>
          Sort users by
          <select
            value={sort}
            onChange={event => setSort(event.target.value)}>
            {[
              ["uid", "ID"],
              ["active", "Active"],
              ["name", "Name"],
              ["email", "Email"],
              ["affiliation", "Affiliation"],
              ["joiningReason", "Reason for joining"],
              ["comment", "Comment"],
              ["role", "Role"],
              ["accountCreation", "Account creation"],
            ].map(([key, label]) => (
              <option
                key={key}
                value={key}>
                {label}
              </option>
            ))}
          </select>
        </label>
        <button
          type="button"
          onClick={() => setDescending(!descending)}>
          Sort {descending ? "descending" : "ascending"}
        </button>
        <button
          type="button"
          onClick={() => {
            setSearch("");
            setRoles([]);
          }}>
          Reset filters
        </button>
      </section>
      {state.data &&
        (users.length ? (
          <table>
            <thead>
              <tr>
                <th>Active</th>
                <th>ID</th>
                <th>Name</th>
                <th>Email</th>
                <th>Affiliation</th>
                <th>Reason for joining</th>
                <th>Comment</th>
                <th>Role</th>
                <th>Created</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {users.map(item => (
                <tr key={item.uid}>
                  <td>{isActive(item) ? "Active" : "Inactive"}</td>
                  <td>{item.uid}</td>
                  <td>{item.name}</td>
                  <td>{item.email}</td>
                  <td>{item.affiliation}</td>
                  <td>{item.joiningReason}</td>
                  <td>{item.comment}</td>
                  <td>{item.role}</td>
                  <td>{item.accountCreation ? new Date(item.accountCreation * 1000).toLocaleString() : "—"}</td>
                  <td className="resource-row-actions">
                    <button
                      type="button"
                      aria-label={`Edit user ${item.name}`}
                      disabled={operation.busy}
                      onClick={() => {
                        operation.clear();
                        setDraft({
                          uid: item.uid,
                          name: item.name,
                          email: item.email,
                          role: item.role,
                          comment: item.comment,
                        });
                      }}>
                      Edit
                    </button>
                    <button
                      type="button"
                      aria-label={`Quota for ${item.name}`}
                      onClick={() => setQuotaUser(item)}>
                      Quota
                    </button>
                    <button
                      type="button"
                      aria-label={`Feedback for ${item.name}`}
                      disabled={!(feedbackCounts.get(item.uid) ?? 0)}
                      onClick={() => setFeedbackUser(item)}>
                      Feedback ({feedbackCounts.get(item.uid) ?? 0})
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p>No users match the selected filters.</p>
        ))}
      {draft && (
        <Dialog
          title={`Edit user ${draft.uid}`}
          close={() => setDraft(undefined)}
          busy={operation.busy}>
          <form
            onSubmit={event => {
              event.preventDefault();
              const persisted = state.data?.find(item => item.uid === draft.uid);
              if (!persisted) return;
              const update = { ...draft, role: draft.uid === currentUid ? persisted.role : draft.role };
              void operation.run(
                () => resources.updateUser(update),
                () => {
                  state.setData(items => items?.map(item => (item.uid === update.uid ? { ...item, ...update } : item)));
                  setDraft(undefined);
                },
                "User updated."
              );
            }}>
            <label>
              User name
              <input
                value={draft.name}
                disabled={operation.busy}
                onChange={event => setDraft({ ...draft, name: event.target.value })}
              />
            </label>
            <label>
              User email
              <input
                type="email"
                value={draft.email}
                disabled={operation.busy}
                onChange={event => setDraft({ ...draft, email: event.target.value })}
              />
            </label>
            <label>
              User role
              <select
                value={draft.role}
                disabled={operation.busy || draft.uid === currentUid}
                onChange={event => setDraft({ ...draft, role: event.target.value as AdminUser["role"] })}>
                {ROLES.map(role => (
                  <option key={role}>{role}</option>
                ))}
              </select>
            </label>
            <label>
              User comment
              <textarea
                value={draft.comment ?? ""}
                disabled={operation.busy}
                onChange={event => setDraft({ ...draft, comment: event.target.value })}
              />
            </label>
            <footer>
              <button
                type="button"
                disabled={operation.busy}
                onClick={() => setDraft(undefined)}>
                Cancel
              </button>
              <button
                type="submit"
                disabled={operation.busy}>
                Save user
              </button>
            </footer>
          </form>
        </Dialog>
      )}
      {quotaUser && (
        <Dialog
          title={`Quota for ${quotaUser.name}`}
          close={() => setQuotaUser(undefined)}>
          <QuotaPage
            resources={resources}
            onNotice={onNotice}
            userId={quotaUser.uid}
          />
          <button
            type="button"
            onClick={() => setQuotaUser(undefined)}>
            Close quota
          </button>
        </Dialog>
      )}
      {feedbackUser && (
        <Dialog
          title={`Feedback for ${feedbackUser.name}`}
          close={() => setFeedbackUser(undefined)}>
          <FeedbackPage
            resources={resources}
            userId={feedbackUser.uid}
          />
          <button
            type="button"
            onClick={() => setFeedbackUser(undefined)}>
            Close feedback
          </button>
        </Dialog>
      )}
    </main>
  );
}

export type ExecutionControlRequest = "WorkflowKillRequest" | "WorkflowPauseRequest" | "WorkflowResumeRequest";
export function executionElapsedSeconds(item: AdminExecution, now = Date.now()): number {
  if (!Number.isFinite(item.startTime) || item.startTime < 0) return -1;
  const end = ["RUNNING", "READY", "PAUSED"].includes(item.executionStatus) ? now : item.endTime;
  return Number.isFinite(end) ? Math.floor((end - item.startTime) / 1000) : -1;
}
export function executionDisplayStatus(item: AdminExecution, now = Date.now()): string {
  if (["COMPLETED", "JUST COMPLETED"].includes(item.executionStatus))
    return now - item.endTime <= 5000 ? "JUST COMPLETED" : "COMPLETED";
  return item.executionStatus;
}
function formatDuration(seconds: number): string {
  if (seconds < 0) return "Not available";
  return [Math.floor(seconds / 3600), Math.floor(seconds / 60) % 60, seconds % 60]
    .map(value => String(value).padStart(2, "0"))
    .join(":");
}

export interface AdminExecutionsProps extends ResourcePageProps {
  control: (workflowId: number, request: ExecutionControlRequest) => Promise<void>;
  onHistory: (workflowId: number, name: string) => void;
  refreshMs?: number;
}
export function AdminExecutionsPage({
  resources,
  onNotice,
  control,
  onHistory,
  refreshMs = 5000,
}: AdminExecutionsProps) {
  const [query, setQuery] = useState<ExecutionQuery>({
    pageSize: 5,
    pageIndex: 0,
    sortField: "NO_SORTING",
    sortDirection: "NO_SORTING",
    filter: [],
  });
  const [rows, setRows] = useState<AdminExecution[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>();
  const [revision, setRevision] = useState(0);
  const [now, setNow] = useState(Date.now());
  const generation = useRef(0);
  const operation = useOperation(onNotice);
  const refresh = () => setRevision(value => value + 1);
  useEffect(() => {
    let active = true;
    let current: AbortController | undefined;
    const fetchPage = (foreground: boolean) => {
      const request = ++generation.current;
      current?.abort();
      current = new AbortController();
      const signal = current.signal;
      if (foreground) setLoading(true);
      void Promise.all([resources.executionList(query, { signal }), resources.totalExecutions({ signal })])
        .then(
          ([executions, count]) => {
            if (active && request === generation.current) {
              setRows(executions);
              setTotal(count);
              setError(undefined);
            }
          },
          reason => {
            if (active && !signal.aborted && request === generation.current)
              setError(reason instanceof Error ? reason.message : "Failed to load executions.");
          }
        )
        .finally(() => {
          if (active && request === generation.current) setLoading(false);
        });
    };
    fetchPage(true);
    const timer = setInterval(() => fetchPage(false), Math.max(10, refreshMs));
    return () => {
      active = false;
      current?.abort();
      clearInterval(timer);
    };
  }, [resources, query, revision, refreshMs]);
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);
  const sort = (field: string) =>
    setQuery(current => ({
      ...current,
      sortField: current.sortField === field && current.sortDirection === "desc" ? "NO_SORTING" : field,
      sortDirection: current.sortField !== field ? "asc" : current.sortDirection === "asc" ? "desc" : "NO_SORTING",
    }));
  const statuses = ["READY", "RUNNING", "PAUSED", "COMPLETED", "FAILED", "KILLED", "JUST COMPLETED", "UNKNOWN"];
  return (
    <main className="resource-page">
      <header>
        <h1>Executions</h1>
        <button
          type="button"
          onClick={refresh}>
          Refresh
        </button>
      </header>
      <LoadState
        loading={loading}
        error={error && !operation.error ? error : undefined}
        retry={refresh}
      />
      <Notices
        error={operation.error}
        message={operation.message}
      />
      <fieldset className="resource-filter-list">
        <legend>Status filters</legend>
        {statuses.map(status => (
          <label key={status}>
            <input
              type="checkbox"
              aria-label={`Filter ${status}`}
              checked={query.filter.includes(status)}
              onChange={event =>
                setQuery(current => ({
                  ...current,
                  filter: event.target.checked
                    ? [...current.filter, status]
                    : current.filter.filter(item => item !== status),
                }))
              }
            />
            {status}
          </label>
        ))}
      </fieldset>
      <table>
        <thead>
          <tr>
            {[
              ["workflow_name", "Workflow"],
              ["execution_name", "Execution"],
              ["initiator", "Initiator"],
            ].map(([field, label]) => (
              <th key={field}>
                <button
                  type="button"
                  aria-label={`Sort by ${label}`}
                  onClick={() => sort(field)}>
                  {label}
                </button>
              </th>
            ))}
            <th>Status</th>
            <th>Time used</th>
            <th>
              <button
                type="button"
                aria-label="Sort by End time"
                onClick={() => sort("end_time")}>
                End time
              </button>
            </th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(item => {
            const status = executionDisplayStatus(item, now);
            const ended = ["COMPLETED", "JUST COMPLETED", "FAILED", "KILLED"].includes(status);
            const action = (request: ExecutionControlRequest) =>
              void operation.run(() => control(item.workflowId, request), refresh, "Execution control request sent.");
            return (
              <tr key={item.executionId}>
                <td>
                  {item.access ? (
                    <a href={`/user/workflow/${item.workflowId}`}>
                      {item.workflowName} ({item.workflowId})
                    </a>
                  ) : (
                    `${item.workflowName} (${item.workflowId})`
                  )}
                </td>
                <td>
                  {item.executionName} ({item.executionId})
                </td>
                <td>{item.userName}</td>
                <td>{status}</td>
                <td>{formatDuration(executionElapsedSeconds(item, now))}</td>
                <td>{item.endTime ? new Date(item.endTime).toLocaleString() : "Not available"}</td>
                <td className="resource-row-actions">
                  <button
                    type="button"
                    aria-label={`Kill ${item.executionName}`}
                    disabled={operation.busy || ended}
                    onClick={() => action("WorkflowKillRequest")}>
                    Kill
                  </button>
                  <button
                    type="button"
                    aria-label={`Pause ${item.executionName}`}
                    disabled={operation.busy || status !== "RUNNING"}
                    onClick={() => action("WorkflowPauseRequest")}>
                    Pause
                  </button>
                  <button
                    type="button"
                    aria-label={`Resume ${item.executionName}`}
                    disabled={operation.busy || status !== "PAUSED"}
                    onClick={() => action("WorkflowResumeRequest")}>
                    Resume
                  </button>
                  <button
                    type="button"
                    aria-label={`History for ${item.workflowName}`}
                    onClick={() => onHistory(item.workflowId, item.workflowName)}>
                    History
                  </button>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
      {!loading && !error && !rows.length && <p>No executions match the selected filters.</p>}
      <footer>
        <button
          type="button"
          aria-label="Previous page"
          disabled={query.pageIndex <= 0 || loading}
          onClick={() => setQuery(current => ({ ...current, pageIndex: current.pageIndex - 1 }))}>
          Previous
        </button>
        <span>
          Page {query.pageIndex + 1} of {Math.max(1, Math.ceil(total / query.pageSize))}
        </span>
        <button
          type="button"
          aria-label="Next page"
          disabled={loading || (query.pageIndex + 1) * query.pageSize >= total}
          onClick={() => setQuery(current => ({ ...current, pageIndex: current.pageIndex + 1 }))}>
          Next
        </button>
        <label>
          Executions per page
          <select
            value={query.pageSize}
            onChange={event => {
              const size = Number(event.target.value);
              setQuery(current => ({
                ...current,
                pageSize: size,
                pageIndex: Math.min(current.pageIndex, Math.max(0, Math.ceil(total / size) - 1)),
              }));
            }}>
            {[5, 10, 20, 50].map(size => (
              <option key={size}>{size}</option>
            ))}
          </select>
        </label>
      </footer>
    </main>
  );
}

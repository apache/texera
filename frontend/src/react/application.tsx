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

import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import {
  BrowserRouter,
  Link,
  NavLink,
  Navigate,
  Outlet,
  Route,
  Routes,
  useLocation,
  useNavigate,
  useOutletContext,
  useParams,
  useSearchParams,
} from "react-router-dom";
import type { ApiClient } from "./core/api-client";
import { AuthStore, safeReturnUrl, type SessionUser } from "./core/auth";
import { SocialAuth } from "./core/social-auth";
import { LoginPage } from "./login-page";
import { GoogleButton } from "./core/google-button";
import { accessRedirect } from "./navigation";
import { WorkflowListPage } from "./workflows/workflow-list-page";
import { ResourceApi } from "./resources/api";
import { AdminMailPage, AdminSettingsPage, Dialog, PythonEnvironmentsPage, WarehousePage } from "./resources/pages";
import { AdminExecutionsPage, AdminUsersPage, FeedbackPage } from "./resources/administration";
import { ComputingImagesPage, ComputingUnitsPage } from "./resources/computing";
import { QuotaPage } from "./resources/quota";
import { StorageApi, type ResourceKind } from "./resources/storage-api";
import { ResourceCreatePage, ResourceDetailPage, StorageResourceListPage } from "./resources/storage-pages";
import { ResourceShareDialog, type SharedResourceKind } from "./resources/sharing";
import { ExecutionControl } from "./resources/execution-control";
import { ExecutionHistoryPage } from "./results/execution-history";
import { WorkspacePage } from "./workspace/workspace-page";
import { HubHomePage, HubSearchPage, HubWorkflowDetailPage } from "./hub/hub-pages";
import type { HubKind } from "./hub/hub-repository";
import { RichText } from "./common/rich-text";
import { DeploymentUpdateNotice } from "./core/deployment-update-notice";
import "./shell.css";

export interface ApplicationProps {
  api: ApiClient;
  auth: AuthStore;
  social?: SocialAuth;
}

export function Application(props: ApplicationProps) {
  const [ready, setReady] = useState(false);
  const [error, setError] = useState<string>();
  const [retry, setRetry] = useState(0);
  useEffect(() => {
    let active = true;
    setError(undefined);
    void props.auth.bootstrap().then(
      () => {
        if (active) setReady(true);
      },
      reason => {
        if (active) setError(reason instanceof Error ? reason.message : "Texera could not be loaded.");
      }
    );
    return () => {
      active = false;
    };
  }, [props.auth, retry]);
  if (error)
    return (
      <main className="login-page">
        <section className="login-card">
          <h1>Texera</h1>
          <p role="alert">{error}</p>
          <button onClick={() => setRetry(retry + 1)}>Retry</button>
        </section>
      </main>
    );
  if (!ready)
    return (
      <main className="login-page">
        <p role="status">Loading Texera…</p>
      </main>
    );
  return (
    <BrowserRouter>
      <ApplicationRoutes {...props} />
    </BrowserRouter>
  );
}

export function ApplicationRoutes({ api, auth, social }: ApplicationProps) {
  const user = useSyncExternalStore(auth.subscribe, auth.getSnapshot, auth.getSnapshot);
  const resources = useMemo(() => new ResourceApi(api, auth.getToken), [api, auth]);
  const storage = useMemo(() => new StorageApi(api, { getToken: auth.getToken }), [api, auth]);
  const [sharing, setSharing] = useState<{ kind: SharedResourceKind; id: number }>();
  const [shareRevision, setShareRevision] = useState(0);
  const share = useCallback((kind: SharedResourceKind, id: number) => setSharing({ kind, id }), []);
  const navigate = useNavigate();
  const openWorkflow = useCallback(
    (wid: number, view?: string, autoLayout?: boolean) =>
      navigate(`/user/workflow/${wid}${view === "FORM" ? "/form" : ""}${autoLayout ? "?autolayout=1" : ""}`),
    [navigate]
  );
  const openHub = useCallback(
    (kind: Exclude<HubKind, "all">, id: number) => navigate(`/hub/${kind}/result/detail/${id}`),
    [navigate]
  );
  const browse = useCallback((kind: HubKind) => navigate(`/hub/${kind}/result`), [navigate]);
  return (
    <>
      <Routes>
        <Route
          path="login"
          element={
            <SignInRoute
              auth={auth}
              social={social}
            />
          }
        />
        <Route
          path="callback/orcid"
          element={
            <OrcidRoute
              auth={auth}
              social={social}
            />
          }
        />
        <Route
          element={
            <ApplicationShell
              resources={resources}
              auth={auth}
            />
          }>
          <Route
            index
            element={
              <Navigate
                to="/about"
                replace
              />
            }
          />
          <Route
            path="about"
            element={<AboutPage />}
          />
          <Route
            path="home"
            element={
              <HubHomePage
                api={api}
                user={user.user}
                onBrowse={browse}
                onOpen={openHub}
              />
            }
          />
          <Route
            path="hub/:kind/result"
            element={
              <HubSearchRoute
                api={api}
                auth={auth}
                onOpen={openHub}
              />
            }
          />
          <Route
            path="hub/workflow/result/detail/:id"
            element={
              <HubWorkflowRoute
                api={api}
                auth={auth}
                onClone={openWorkflow}
              />
            }
          />
          <Route
            path="hub/dataset/result/detail/:id"
            element={
              <StorageDetailRoute
                api={api}
                auth={auth}
                storage={storage}
                kind="dataset"
                publicView
              />
            }
          />
          <Route
            path="hub/model/result/detail/:id"
            element={
              <StorageDetailRoute
                api={api}
                auth={auth}
                storage={storage}
                kind="model"
                publicView
              />
            }
          />
          <Route
            path="search"
            element={
              <HubSearchRoute
                api={api}
                auth={auth}
                onOpen={openHub}
              />
            }
          />
          <Route
            path="user"
            element={<RequireAccount auth={auth} />}>
            <Route
              path="workflow"
              element={
                user.user ? (
                  <WorkflowListPage
                    api={api}
                    user={user.user}
                    config={user.config}
                    onOpen={openWorkflow}
                    onOpenVersion={(wid, version) => navigate(`/user/workflow/${wid}?version=${version}`)}
                  />
                ) : null
              }
            />
            <Route
              path="workflow/:id/*"
              element={
                <WorkspaceRoute
                  api={api}
                  auth={auth}
                />
              }
            />
            <Route
              path="dataset"
              element={
                <StorageResourceListPage
                  key={shareRevision}
                  api={api}
                  storage={storage}
                  kind="dataset"
                  user={user.user}
                  onNavigate={navigate}
                  onShare={share}
                />
              }
            />
            <Route
              path="dataset/create"
              element={
                <ResourceCreatePage
                  api={api}
                  storage={storage}
                  kind="dataset"
                  user={user.user}
                  onCreated={entry => navigate(`/user/dataset/${entry.id}`)}
                />
              }
            />
            <Route
              path="dataset/:id"
              element={
                <StorageDetailRoute
                  key={shareRevision}
                  api={api}
                  auth={auth}
                  storage={storage}
                  kind="dataset"
                  onShare={share}
                />
              }
            />
            <Route
              path="model"
              element={
                <StorageResourceListPage
                  key={shareRevision}
                  api={api}
                  storage={storage}
                  kind="model"
                  user={user.user}
                  onNavigate={navigate}
                  onShare={share}
                />
              }
            />
            <Route
              path="model/:id"
              element={
                <StorageDetailRoute
                  key={shareRevision}
                  api={api}
                  auth={auth}
                  storage={storage}
                  kind="model"
                  onShare={share}
                />
              }
            />
            <Route
              path="compute"
              element={
                <ComputingUnitsPage
                  key={shareRevision}
                  resources={resources}
                  sharingEnabled={!!user.config.sharingComputingUnitEnabled}
                  onShare={id => share("computing-unit", id)}
                />
              }
            />
            <Route
              path="warehouse"
              element={<WarehousePage resources={resources} />}
            />
            <Route
              path="python-venv"
              element={<PythonEnvironmentsPage resources={resources} />}
            />
            <Route
              path="quota"
              element={<QuotaPage resources={resources} />}
            />
            <Route
              path="feedback"
              element={<FeedbackPage resources={resources} />}
            />
            <Route
              path="discussion"
              element={
                <section className="texera-discussion">
                  <h1>Discussion</h1>
                  <iframe
                    title="Texera discussion forum"
                    src="/forum"
                  />
                </section>
              }
            />
            <Route
              path="*"
              element={
                <Navigate
                  to="/user/workflow"
                  replace
                />
              }
            />
          </Route>
          <Route
            path="admin"
            element={
              <RequireAccount
                auth={auth}
                admin
              />
            }>
            <Route
              path="user"
              element={
                <AdminUsersPage
                  resources={resources}
                  currentUid={user.user?.uid}
                  activeTimeInMinutes={user.config.activeTimeInMinutes}
                />
              }
            />
            <Route
              path="execution"
              element={
                user.user ? (
                  <AdminExecutionsRoute
                    resources={resources}
                    api={api}
                    auth={auth}
                    user={user.user}
                  />
                ) : null
              }
            />
            <Route
              path="compute"
              element={
                <ComputingUnitsPage
                  key={shareRevision}
                  resources={resources}
                  admin
                  sharingEnabled={!!user.config.sharingComputingUnitEnabled}
                  onShare={id => share("computing-unit", id)}
                />
              }
            />
            <Route
              path="cu-image"
              element={<ComputingImagesPage resources={resources} />}
            />
            <Route
              path="gmail"
              element={<AdminMailPage resources={resources} />}
            />
            <Route
              path="settings"
              element={<AdminSettingsPage resources={resources} />}
            />
            <Route
              path="*"
              element={
                <Navigate
                  to="/user/workflow"
                  replace
                />
              }
            />
          </Route>
          <Route
            path="*"
            element={
              <Navigate
                to="/user/workflow"
                replace
              />
            }
          />
        </Route>
      </Routes>
      {sharing && user.user && (
        <ResourceShareDialog
          api={api}
          user={user.user}
          {...sharing}
          onClose={(changed, selfRevoked) => {
            setSharing(undefined);
            if (changed) setShareRevision(revision => revision + 1);
            if (selfRevoked) navigate(`/user/${sharing.kind === "computing-unit" ? "compute" : sharing.kind}`);
          }}
        />
      )}
    </>
  );
}

function RequireAccount({ auth, admin }: { auth: AuthStore; admin?: boolean }) {
  const user = useSyncExternalStore(auth.subscribe, auth.getSnapshot, auth.getSnapshot);
  const location = useLocation();
  const redirect = accessRedirect(location.pathname + location.search + location.hash, user.status, user.user?.role);
  if (redirect)
    return (
      <Navigate
        to={redirect}
        replace
      />
    );
  if (admin && user.user?.role !== "ADMIN")
    return (
      <Navigate
        to="/user/workflow"
        replace
      />
    );
  return <Outlet />;
}
function SignInRoute({ auth, social }: Pick<ApplicationProps, "auth" | "social">) {
  const navigate = useNavigate();
  const [query] = useSearchParams();
  const go = useCallback((url: string) => navigate(url, { replace: true }), [navigate]);
  return (
    <LoginPage
      auth={auth}
      onNavigate={go}
      returnUrl={safeReturnUrl(query.get("returnUrl"))}
      googleButton={social ? <GoogleButton social={social} /> : undefined}
      signInWithProvider={
        social ? provider => (provider === "apple" ? social.signInWithApple() : social.signInWithOrcid()) : undefined
      }
    />
  );
}
function OrcidRoute({ auth, social }: Pick<ApplicationProps, "auth" | "social">) {
  const [error, setError] = useState<string>();
  const navigate = useNavigate();
  const started = useRef(false);
  const [params] = useSearchParams();
  useEffect(() => {
    if (started.current) return;
    started.current = true;
    if (!social) {
      setError("ORCID sign-in is unavailable.");
      return;
    }
    void social.completeOrcid(params).then(
      () => navigate(auth.getSnapshot().status === "authenticated" ? "/user/workflow" : "/login", { replace: true }),
      reason => setError(reason instanceof Error ? reason.message : "ORCID sign-in failed.")
    );
  }, [social, auth, params, navigate]);
  return (
    <main className="login-page">
      <section className="login-card">
        {error ? (
          <>
            <p role="alert">{error}</p>
            <Link
              to="/login"
              replace>
              Return to sign-in
            </Link>
          </>
        ) : (
          <p role="status">Completing ORCID sign-in…</p>
        )}
      </section>
    </main>
  );
}

function ApplicationShell({ resources, auth }: { resources: ResourceApi; auth: AuthStore }) {
  const user = useSyncExternalStore(auth.subscribe, auth.getSnapshot, auth.getSnapshot);
  const [settings, setSettings] = useState<Record<string, string>>({});
  const [collapsed, setCollapsed] = useState(false);
  const location = useLocation();
  const navigate = useNavigate();
  const [search, setSearch] = useState(() => new URLSearchParams(location.search).get("q") ?? "");
  useEffect(() => setSearch(new URLSearchParams(location.search).get("q") ?? ""), [location.pathname, location.search]);
  useEffect(() => {
    let active = true;
    void resources
      .publicSettings()
      .then(values => {
        if (active) setSettings(values);
      })
      .catch(() => undefined);
    return () => {
      active = false;
    };
  }, [resources]);
  const enabled = (key: string) => settings[key] === "true";
  const links = [
    ["workflows_enabled", "/user/workflow", "Workflows"],
    ["datasets_enabled", "/user/dataset", "Datasets"],
    ["models_enabled", "/user/model", "Models"],
    ["compute_enabled", "/user/compute", "Computing units"],
    ["quota_enabled", "/user/quota", "Quota"],
    ["forum_enabled", "/user/discussion", "Discussion"],
  ];
  return (
    <div className={`texera-shell${collapsed ? " is-collapsed" : ""}`}>
      <aside className="texera-sidebar">
        <Link
          to="/about"
          className="texera-brand">
          <img
            src={(collapsed ? settings.mini_logo : settings.logo) || "/assets/logos/logo.png"}
            alt="Texera"
          />
        </Link>
        <nav aria-label="Main navigation">
          {enabled("hub_enabled") && (
            <section>
              <strong>Hub</strong>
              {(["workflow", "dataset", "model"] as const).map(
                kind =>
                  enabled(`${kind}_enabled`) && (
                    <NavLink
                      key={kind}
                      to={`/hub/${kind}/result`}>
                      {kind === "workflow" ? "Workflows" : kind === "dataset" ? "Datasets" : "Models"}
                    </NavLink>
                  )
              )}
            </section>
          )}
          {user.status === "authenticated" && enabled("your_work_enabled") && (
            <section>
              <strong>Your Work</strong>
              {links.map(
                ([flag, path, label]) =>
                  enabled(flag) && (
                    <NavLink
                      key={path}
                      to={path}>
                      {label}
                    </NavLink>
                  )
              )}
              {user.config.warehouseEnabled && <NavLink to="/user/warehouse">Warehouses</NavLink>}
              {enabled("compute_enabled") && <NavLink to="/user/python-venv">Python environments</NavLink>}
              <NavLink to="/user/feedback">Feedback</NavLink>
            </section>
          )}
          {user.user?.role === "ADMIN" && (
            <section>
              <strong>Administration</strong>
              {[
                ["user", "Users"],
                ["gmail", "Email"],
                ["execution", "Executions"],
                ["compute", "Computing units"],
                ["settings", "Settings"],
                ["cu-image", "Computing-unit images"],
              ].map(([path, label]) => (
                <NavLink
                  key={path}
                  to={`/admin/${path}`}>
                  {label}
                </NavLink>
              ))}
            </section>
          )}
          {enabled("home_enabled") && <NavLink to="/home">Home</NavLink>}
          {enabled("about_enabled") && <NavLink to="/about">About</NavLink>}
        </nav>
        <button
          aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
          onClick={() => setCollapsed(!collapsed)}>
          {collapsed ? "›" : "‹"}
        </button>
      </aside>
      <div className="texera-main">
        <DeploymentUpdateNotice enabled={user.config.deploymentVersionCheckEnabled === true} />
        {!/^\/user\/workflow\/\d+/.test(location.pathname) && (
          <header className="texera-header">
            <form
              role="search"
              aria-label="Global search"
              className="texera-global-search"
              onSubmit={event => {
                event.preventDefault();
                navigate("/search?" + new URLSearchParams({ q: search }));
              }}>
              <input
                type="search"
                aria-label="Search Texera"
                placeholder="Search Texera"
                value={search}
                onChange={event => setSearch(event.target.value)}
              />
              <button
                type="submit"
                aria-label="Submit global search">
                Search
              </button>
            </form>
            <div>
              {user.status === "authenticated" ? (
                <>
                  <span>{user.user?.name}</span>
                  <button onClick={auth.logout}>Sign out</button>
                </>
              ) : (
                <Link to="/login">Sign in</Link>
              )}
            </div>
          </header>
        )}
        <main className="texera-content">
          <Outlet context={{ settings }} />
        </main>
      </div>
    </div>
  );
}
function AboutPage() {
  const { settings } = useOutletContext<{ settings: Record<string, string> }>();
  return (
    <article className="texera-about">
      <h1>About Texera</h1>
      <p>
        Apache Texera (Incubating) is an open-source platform for human-AI collaborative data science using visual
        workflows. It enables analysts to construct, execute, and refine data analysis tasks through an intuitive GUI,
        assisted by AI agents that understand natural-language instructions. Texera is well-suited for a wide range of
        applications, including “AI for Science,” by making advanced AI and data science capabilities accessible to a
        broader community. It can run on a laptop for local use or be deployed in the cloud to support scalable
        processing of large datasets.
      </p>
      <h2>The platform has the following key features:</h2>
      <ul>
        {[
          "Natural-language data science through AI agents",
          "Intuitive GUI-based workflows for data science",
          "Real-time collaboration for workflow editing and execution",
          "Parallel backend engine for scalable big-data processing",
          "Language-agnostic workflow runtime, native support for Python and Java",
          "Separation of compute and storage for flexible cloud deployment",
          "Runtime debugging and interactive workflow execution",
        ].map(feature => (
          <li key={feature}>{feature}</li>
        ))}
      </ul>
      <img
        className="workflow-gui-screenshot"
        src="/assets/workflow_gui.png"
        alt="workflow gui screenshot"
      />
      {settings.about_text && (
        <RichText
          content={settings.about_text}
          format="html"
        />
      )}
      <h2>Learn More</h2>
      <p>
        To learn more, please visit{" "}
        <a
          href="https://texera.apache.org/"
          target="_blank"
          rel="noopener noreferrer">
          https://texera.apache.org/
        </a>
        . The{" "}
        <a
          href="https://github.com/apache/texera"
          target="_blank"
          rel="noopener noreferrer">
          source code
        </a>{" "}
        of the platform is available on GitHub.
      </p>
    </article>
  );
}

function StorageDetailRoute({
  api,
  auth,
  storage,
  kind,
  publicView = false,
  onShare,
}: Pick<ApplicationProps, "api" | "auth"> & {
  storage: StorageApi;
  kind: ResourceKind;
  publicView?: boolean;
  onShare?: (kind: ResourceKind, id: number) => void;
}) {
  const { id } = useParams();
  const user = useSyncExternalStore(auth.subscribe, auth.getSnapshot, auth.getSnapshot);
  const navigate = useNavigate();
  const resourceId = routeIdentifier(id);
  if (resourceId === undefined) return <p role="alert">Invalid resource identifier.</p>;
  return (
    <ResourceDetailPage
      api={api}
      storage={storage}
      kind={kind}
      id={resourceId}
      user={user.user}
      publicView={publicView}
      onShare={onShare}
      onNavigate={navigate}
    />
  );
}
function WorkspaceRoute({ api, auth }: Pick<ApplicationProps, "api" | "auth">) {
  const params = useParams();
  const location = useLocation();
  const navigate = useNavigate();
  const user = useSyncExternalStore(auth.subscribe, auth.getSnapshot, auth.getSnapshot);
  const query = new URLSearchParams(location.search);
  const workflowId = routeIdentifier(params.id);
  const requestedVersion = query.get("version");
  const versionId = requestedVersion === null ? undefined : routeIdentifier(requestedVersion);
  if (workflowId === undefined || (requestedVersion !== null && versionId === undefined))
    return <p role="alert">Invalid workflow identifier.</p>;
  if (!user.user) return null;
  if (params["*"] && params["*"] !== "form")
    return (
      <Navigate
        to={`/user/workflow/${workflowId}${location.search}`}
        replace
      />
    );
  return (
    <WorkspacePage
      api={api}
      user={user.user}
      config={user.config}
      getToken={auth.getToken}
      workflowId={workflowId}
      versionId={versionId}
      autoLayout={query.get("autolayout") === "1"}
      onAutoLayoutApplied={() => {
        const next = new URLSearchParams(location.search);
        next.delete("autolayout");
        navigate(`${location.pathname}${next.size ? `?${next}` : ""}${location.hash}`, { replace: true });
      }}
      view={params["*"] === "form" ? "form" : "canvas"}
      onView={view =>
        navigate(`/user/workflow/${workflowId}${view === "form" ? "/form" : ""}${location.search}${location.hash}`)
      }
      onBack={() => navigate("/user/workflow")}
      onVersion={version => {
        const next = new URLSearchParams(location.search);
        next.set("version", String(version));
        navigate(`/user/workflow/${workflowId}${params["*"] === "form" ? "/form" : ""}?${next}${location.hash}`);
      }}
      onOpenWorkflow={wid => navigate(`/user/workflow/${wid}`)}
    />
  );
}
function AdminExecutionsRoute({
  api,
  resources,
  auth,
  user,
}: Pick<ApplicationProps, "api" | "auth"> & { resources: ResourceApi; user: SessionUser }) {
  const control = useMemo(() => new ExecutionControl({ uid: user.uid, getToken: auth.getToken }), [user.uid, auth]);
  const [history, setHistory] = useState<{ wid: number; name: string }>();
  const navigate = useNavigate();
  useEffect(() => () => control.dispose(), [control]);
  return (
    <>
      <AdminExecutionsPage
        resources={resources}
        control={(wid, request) => control.send(wid, request)}
        onHistory={(wid, name) => setHistory({ wid, name })}
      />
      {history && (
        <Dialog
          title={`Execution results of Workflow: ${history.name}`}
          close={() => setHistory(undefined)}>
          <ExecutionHistoryPage
            api={api}
            wid={history.wid}
            onOpenWorkflow={(wid, vid) =>
              navigate(`/user/workflow/${wid}${vid === undefined ? "" : `?version=${vid}`}`)
            }
            onOpenExecution={entry => navigate(`/user/workflow/${history.wid}?version=${entry.vId}`)}
          />
          <button
            type="button"
            onClick={() => setHistory(undefined)}>
            Close execution history
          </button>
        </Dialog>
      )}
    </>
  );
}
function routeIdentifier(value: string | undefined): number | undefined {
  if (!value || !/^\d+$/.test(value)) return undefined;
  const result = Number(value);
  return Number.isSafeInteger(result) ? result : undefined;
}
function HubSearchRoute({
  api,
  auth,
  onOpen,
}: Pick<ApplicationProps, "api" | "auth"> & { onOpen: (kind: Exclude<HubKind, "all">, id: number) => void }) {
  const params = useParams();
  const user = useSyncExternalStore(auth.subscribe, auth.getSnapshot, auth.getSnapshot);
  const kind = ["workflow", "dataset", "model"].includes(params.kind ?? "") ? (params.kind as HubKind) : undefined;
  const [searchParams, setSearchParams] = useSearchParams();
  return (
    <HubSearchPage
      api={api}
      user={user.user}
      kind={kind}
      onOpen={onOpen}
      query={searchParams.get("q") ?? ""}
      onQuerySubmit={query =>
        setSearchParams(previous => {
          const next = new URLSearchParams(previous);
          next.set("q", query);
          return next;
        })
      }
    />
  );
}
function HubWorkflowRoute({
  api,
  auth,
  onClone,
}: Pick<ApplicationProps, "api" | "auth"> & { onClone: (wid: number, view?: string) => void }) {
  const params = useParams();
  const navigate = useNavigate();
  const user = useSyncExternalStore(auth.subscribe, auth.getSnapshot, auth.getSnapshot);
  return (
    <HubWorkflowDetailPage
      api={api}
      user={user.user}
      wid={Number(params.id)}
      onClone={onClone}
      onBack={() => navigate("/hub/workflow/result")}
    />
  );
}

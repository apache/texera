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

import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from "react";
import { AuthStore, safeReturnUrl } from "./core/auth";
import { SocialAuth } from "./core/social-auth";
import { LoginPage } from "./login-page";

/** Local development surface. It is not the production application or a parity claim. */
export function ReactPreview({ auth, social }: { auth: AuthStore; social?: SocialAuth }) {
  const session = useSyncExternalStore(auth.subscribe, auth.getSnapshot, auth.getSnapshot);
  const [ready, setReady] = useState(false);
  const [error, setError] = useState<string>();
  const [retry, setRetry] = useState(0);
  const [destination, setDestination] = useState("/user/workflow");
  const navigating = useCallback((url: string) => setDestination(url), []);

  useEffect(() => {
    let active = true;
    setReady(false);
    setError(undefined);
    void (async () => {
      try {
        await auth.bootstrap();
        if (active && social && window.location.pathname === "/callback/orcid") {
          const params = new URLSearchParams(window.location.search);
          // Consume the one-time authorization code before making a provider request.
          window.history.replaceState(null, "", "/login");
          await social.completeOrcid(params);
        }
        if (active) setReady(true);
      } catch (reason) {
        if (active) setError(reason instanceof Error ? reason.message : "Configuration could not be loaded.");
      }
    })();
    return () => {
      active = false;
    };
  }, [auth, social, retry]);

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
    <>
      <aside className="preview-banner">
        Local React migration preview · authentication only · full frontend replacement is unfinished
      </aside>
      {session.status === "authenticated" ? (
        <main className="login-page">
          <section className="login-card">
            <img
              className="session-logo"
              src="/assets/logos/logo.png"
              alt="Texera"
            />
            <h1>Signed in as {session.user?.name}</h1>
            <p>
              The requested destination is <code>{destination}</code>. The workflow pages are not part of this preview.
            </p>
            <button
              type="button"
              className="primary"
              onClick={auth.logout}>
              Sign out
            </button>
          </section>
        </main>
      ) : (
        <LoginPage
          auth={auth}
          onNavigate={navigating}
          returnUrl={safeReturnUrl(new URLSearchParams(window.location.search).get("returnUrl"))}
          googleButton={social ? <GoogleButton social={social} /> : undefined}
          signInWithProvider={
            social
              ? provider => (provider === "apple" ? social.signInWithApple() : social.signInWithOrcid())
              : undefined
          }
        />
      )}
    </>
  );
}

function GoogleButton({ social }: { social: SocialAuth }) {
  const element = useRef<HTMLDivElement>(null);
  const [error, setError] = useState<string>();
  useEffect(() => {
    let active = true;
    let unmount: (() => void) | undefined;
    void social
      .mountGoogle(element.current!, reason => {
        if (active) setError(reason.message);
      })
      .then(dispose => {
        if (active) unmount = dispose;
        else dispose();
      })
      .catch(reason => {
        if (active) setError(reason instanceof Error ? reason.message : "Google sign-in is unavailable.");
      });
    return () => {
      active = false;
      unmount?.();
    };
  }, [social]);
  return (
    <>
      <div ref={element} />
      {error && <p role="alert">{error}</p>}
    </>
  );
}

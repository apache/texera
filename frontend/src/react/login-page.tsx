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

import { useEffect, useState, useSyncExternalStore, type FormEvent, type ReactNode } from "react";
import { AuthStore, safeReturnUrl } from "./core/auth";

type Provider = "google" | "apple" | "orcid";

export interface LoginPageProps {
  auth: AuthStore;
  onNavigate: (url: string) => void;
  returnUrl?: string;
  signInWithProvider?: (provider: Provider) => Promise<void>;
  googleButton?: ReactNode;
}

const ACCOUNT_CREATED =
  "Your account has been created. Please contact the Texera administrator to activate your account.";

export function LoginPage({ auth, onNavigate, returnUrl, signInWithProvider, googleButton }: LoginPageProps) {
  const session = useSyncExternalStore(auth.subscribe, auth.getSnapshot, auth.getSnapshot);
  const config = session.config ?? {};
  const [mode, setMode] = useState<"signin" | "signup">("signin");
  const [username, setUsername] = useState(config.defaultLocalUser?.username ?? "");
  const [password, setPassword] = useState(config.defaultLocalUser?.password ?? "");
  const [email, setEmail] = useState("");
  const [confirm, setConfirm] = useState("");
  const [code, setCode] = useState("");
  const [affiliation, setAffiliation] = useState("");
  const [joiningReason, setJoiningReason] = useState("");
  const [visible, setVisible] = useState(false);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string>();
  const [message, setMessage] = useState<string>();

  useEffect(() => {
    if (session.status === "authenticated") onNavigate(safeReturnUrl(returnUrl));
  }, [session.status, onNavigate, returnUrl]);

  async function perform(action: () => Promise<unknown>) {
    setError(undefined);
    setMessage(undefined);
    setPending(true);
    try {
      await action();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "The request failed. Please try again.");
    } finally {
      setPending(false);
    }
  }

  function submit(event: FormEvent) {
    event.preventDefault();
    void perform(async () => {
      if (session.status === "email-required") {
        await auth.completeEmail(email.trim(), config.emailVerification ? code.trim() : undefined);
      } else if (session.status === "approval-required") {
        await auth.submitJoiningReason(affiliation.trim(), joiningReason.trim());
        setMessage(ACCOUNT_CREATED);
      } else if (mode === "signin") {
        await auth.login(username.trim(), password);
      } else {
        if (password !== confirm) throw new Error("Two passwords are inconsistent.");
        if (config.emailVerification) {
          if (!code.trim()) throw new Error("Enter the code that was emailed to you.");
          await auth.registerVerify(username.trim(), email.trim(), password, code.trim());
        } else {
          const result = await auth.register(username.trim(), email.trim(), password);
          if (result.verificationRequired) {
            setMessage(`A verification code has been sent to ${email.trim()}.`);
            return;
          }
        }
        setMessage(ACCOUNT_CREATED);
      }
    });
  }

  function sendCode() {
    void perform(async () => {
      if (session.status === "email-required") await auth.requestEmailCode(email.trim());
      else {
        const result = await auth.register(username.trim(), email.trim(), password);
        if (!result.verificationRequired) {
          setMessage(ACCOUNT_CREATED);
          return;
        }
      }
      setMessage(`A verification code has been sent to ${email.trim()}.`);
    });
  }

  const emailRequired = session.status === "email-required";
  const approvalRequired = session.status === "approval-required";
  const incomplete = emailRequired || approvalRequired;

  return (
    <main className="login-page">
      <section
        className="login-card"
        aria-label="Texera account">
        <header className="login-brand">
          <img
            src="/assets/logos/logo.png"
            alt="Texera"
          />
          <p>Data science, together</p>
        </header>
        {error && (
          <p
            role="alert"
            className="error-message">
            {error}
          </p>
        )}
        {message && (
          <p
            role="status"
            className="success-message">
            {message}
          </p>
        )}
        {config.localLogin && !incomplete && (
          <nav
            className="login-tabs"
            aria-label="Account action">
            <button
              type="button"
              aria-label="Use an existing account"
              aria-pressed={mode === "signin"}
              onClick={() => {
                setMode("signin");
                setCode("");
                setError(undefined);
              }}>
              Sign in
            </button>
            <button
              type="button"
              aria-pressed={mode === "signup"}
              onClick={() => {
                setMode("signup");
                setCode("");
                setError(undefined);
              }}>
              Create account
            </button>
          </nav>
        )}
        {!incomplete && (
          <div className="social-buttons">
            {config.googleLogin && googleButton}
            {(["google", "apple", "orcid"] as const)
              .filter(provider => config[`${provider}Login`] && !(provider === "google" && googleButton))
              .map(provider => (
                <button
                  type="button"
                  key={provider}
                  disabled={pending || !signInWithProvider}
                  onClick={() => void perform(() => signInWithProvider!(provider))}>
                  Continue with {provider === "orcid" ? "ORCID" : provider[0].toUpperCase() + provider.slice(1)}
                </button>
              ))}
          </div>
        )}
        {config.localLogin || incomplete ? (
          <form
            onSubmit={submit}
            noValidate>
            {emailRequired ? (
              <h1>Add your email address</h1>
            ) : approvalRequired ? (
              <h1>Request account activation</h1>
            ) : null}
            {approvalRequired ? (
              <>
                {session.registrationRequired ? (
                  <>
                    <label>
                      Affiliation
                      <input
                        value={affiliation}
                        onChange={event => setAffiliation(event.target.value)}
                        autoComplete="organization"
                      />
                    </label>
                    <label>
                      Reason for joining
                      <textarea
                        value={joiningReason}
                        onChange={event => setJoiningReason(event.target.value)}
                        required
                      />
                    </label>
                  </>
                ) : (
                  <p>Your account is awaiting approval from the Texera administrator.</p>
                )}
              </>
            ) : (
              <>
                {!emailRequired && (
                  <label>
                    Username
                    <input
                      value={username}
                      onChange={event => setUsername(event.target.value)}
                      autoComplete="username"
                      required
                    />
                  </label>
                )}
                {(mode === "signup" || emailRequired) && (
                  <label>
                    Email
                    <input
                      type="email"
                      value={email}
                      onChange={event => setEmail(event.target.value)}
                      autoComplete="email"
                      required
                    />
                  </label>
                )}
                {!emailRequired && (
                  <>
                    <label>
                      Password
                      <input
                        type={visible ? "text" : "password"}
                        value={password}
                        onChange={event => setPassword(event.target.value)}
                        autoComplete={mode === "signup" ? "new-password" : "current-password"}
                        required
                      />
                    </label>
                    <button
                      className="text-button"
                      type="button"
                      aria-pressed={visible}
                      onClick={() => setVisible(!visible)}>
                      {visible ? "Hide password" : "Show password"}
                    </button>
                  </>
                )}
                {mode === "signup" && !emailRequired && (
                  <label>
                    Confirm password
                    <input
                      type={visible ? "text" : "password"}
                      value={confirm}
                      onChange={event => setConfirm(event.target.value)}
                      autoComplete="new-password"
                      required
                    />
                  </label>
                )}
                {(emailRequired || mode === "signup") && config.emailVerification && (
                  <>
                    <label>
                      Verification code
                      <input
                        value={code}
                        onChange={event => setCode(event.target.value)}
                        autoComplete="one-time-code"
                        inputMode="numeric"
                      />
                    </label>
                    <button
                      type="button"
                      disabled={pending}
                      onClick={sendCode}>
                      Send verification code
                    </button>
                  </>
                )}
              </>
            )}
            {(!approvalRequired || session.registrationRequired) && (
              <button
                className="primary"
                type="submit"
                disabled={pending}>
                {pending
                  ? "Please wait…"
                  : emailRequired
                    ? "Save email"
                    : approvalRequired
                      ? "Request activation"
                      : mode === "signin"
                        ? "Sign in"
                        : "Register"}
              </button>
            )}
            {incomplete && (
              <button
                type="button"
                disabled={pending}
                onClick={() => auth.logout()}>
                Sign out
              </button>
            )}
          </form>
        ) : (
          <p>Local sign-in is disabled.</p>
        )}
        <footer className="login-footer">
          Apache Texera (Incubating) ·{" "}
          <a
            href="https://texera.apache.org/"
            target="_blank"
            rel="noopener noreferrer">
            texera.apache.org
          </a>
        </footer>
      </section>
    </main>
  );
}

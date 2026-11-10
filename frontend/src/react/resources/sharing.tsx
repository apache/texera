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
import { useCallback, useMemo, useRef, useState } from "react";
import type { ApiClient } from "../core/api-client";
import type { SessionUser } from "../core/auth";
import { validateEmailFormat } from "../../core/common/util/email";
import { StorageApi } from "./storage-api";
import { ResourceApi } from "./api";
import { Dialog, LoadState, Notices, useResource, useOperation } from "./pages";

export type SharedResourceKind = "dataset" | "model" | "computing-unit";
interface Access {
  email: string;
  name: string;
  privilege: "NONE" | "READ" | "WRITE";
}
export interface ResourceShareDialogProps {
  api: ApiClient;
  kind: SharedResourceKind;
  id: number;
  user: SessionUser;
  onClose: (changed?: boolean, revokedOwnAccess?: boolean) => void;
}
export function ResourceShareDialog({ api, kind, id, user, onClose }: ResourceShareDialogProps) {
  const storage = useMemo(() => new StorageApi(api), [api]);
  const resources = useMemo(() => new ResourceApi(api), [api]);
  const changed = useRef(false);
  const [emails, setEmails] = useState("");
  const [privilege, setPrivilege] = useState<"READ" | "WRITE">("WRITE");
  const [confirm, setConfirm] = useState<{
    action: "revoke" | "downgrade" | "publish";
    email?: string;
    publish?: boolean;
  }>();
  const [warning, setWarning] = useState<string>();
  const operation = useOperation();
  const load = useCallback(
    async (signal: AbortSignal) => {
      if (!Number.isSafeInteger(id) || id < 0) throw new Error("Invalid resource identifier.");
      const [entries, owner, published] = await Promise.all([
        api.get<Access[]>(`access/${kind}/list/${id}`, { signal }),
        api.get<string>(`access/${kind}/owner/${id}`, { signal, responseType: "text" }),
        kind === "computing-unit"
          ? Promise.resolve(undefined)
          : storage
              .get(kind, id, false, { signal })
              .then(entry => entry.resource.isPublic)
              .catch(() => undefined),
      ]);
      if (!Array.isArray(entries)) throw new Error("Invalid access-list response.");
      return { entries, owner, published };
    },
    [api, storage, kind, id]
  );
  const state = useResource(load);
  const data = state.data;
  const writable =
    !!data &&
    !!user.email &&
    (data.owner === user.email ||
      data.entries.some(entry => entry.email === user.email && entry.privilege === "WRITE"));
  const close = () => onClose(changed.current);
  function grant(email: string, level: "READ" | "WRITE"): Promise<unknown> {
    return api.put(`access/${kind}/grant/${id}/${encodeURIComponent(email)}/${level}`, null);
  }
  function mutate(action: () => Promise<unknown>, success = "Access updated.") {
    return operation.run(
      action,
      () => {
        changed.current = true;
        state.refresh();
      },
      success
    );
  }
  return (
    <Dialog
      title={`Share ${kind === "computing-unit" ? "computing unit" : kind}`}
      close={close}
      busy={operation.busy}>
      <LoadState
        loading={state.loading}
        error={state.error}
        retry={state.refresh}
      />
      <Notices
        error={operation.error}
        message={operation.message}
      />
      {warning && <p role="alert">{warning}</p>}
      {data && (
        <>
          <p>Owner: {data.owner}</p>
          {kind !== "computing-unit" &&
            (data.published === undefined ? (
              <p role="alert">Could not read whether this {kind} is public, so that choice is not shown.</p>
            ) : (
              <div>
                <p>{data.published ? "Public" : "Private"}</p>
                {writable && (
                  <button
                    type="button"
                    disabled={operation.busy}
                    onClick={() => setConfirm({ action: "publish", publish: !data.published })}>
                    {data.published ? "Unpublish" : "Publish"}
                  </button>
                )}
                {data.published && (
                  <label>
                    Public link
                    <input
                      readOnly
                      value={`${location.origin}/hub/${kind}/result/detail/${id}`}
                    />
                  </label>
                )}
              </div>
            ))}
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Email</th>
                <th>Access</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {data.entries.map(entry => (
                <tr key={entry.email}>
                  <td>{entry.name}</td>
                  <td>{entry.email}</td>
                  <td>
                    {writable && entry.email !== data.owner ? (
                      <select
                        aria-label={`Access for ${entry.email}`}
                        value={entry.privilege}
                        disabled={operation.busy}
                        onChange={event => {
                          const level = event.target.value as "READ" | "WRITE";
                          if (entry.email === user.email && entry.privilege === "WRITE" && level === "READ")
                            setConfirm({ action: "downgrade", email: entry.email });
                          else void mutate(() => grant(entry.email, level));
                        }}>
                        <option value="READ">Read</option>
                        <option value="WRITE">Write</option>
                      </select>
                    ) : (
                      entry.privilege
                    )}
                  </td>
                  <td>
                    {writable && entry.email !== data.owner && (
                      <button
                        type="button"
                        disabled={operation.busy}
                        onClick={() => setConfirm({ action: "revoke", email: entry.email })}>
                        Revoke {entry.email}
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {writable && (
            <form
              onSubmit={event => {
                event.preventDefault();
                const recipients = [
                  ...new Set(
                    emails
                      .split(/[\s,;]+/)
                      .map(email => email.trim())
                      .filter(Boolean)
                  ),
                ];
                if (!recipients.length || recipients.some(email => !validateEmailFormat(email).result)) {
                  operation.fail("Enter valid email addresses separated by commas, semicolons or spaces.");
                  return;
                }
                setWarning(undefined);
                void operation.run(
                  async () => {
                    for (const email of recipients) {
                      await grant(email, privilege);
                      changed.current = true;
                      state.setData(
                        current =>
                          current && {
                            ...current,
                            entries: [
                              ...current.entries.filter(entry => entry.email !== email),
                              {
                                email,
                                name: current.entries.find(entry => entry.email === email)?.name ?? email,
                                privilege,
                              },
                            ],
                          }
                      );
                      setEmails(current =>
                        current
                          .split(/[\s,;]+/)
                          .filter(value => value && value !== email)
                          .join("; ")
                      );
                      const sender = user.email ?? user.name;
                      let content = `${sender} shared a ${kind} with you`;
                      if (kind !== "computing-unit")
                        content += `, access the ${kind} at ${location.origin}/user/${kind}/${id}`;
                      try {
                        await resources.sendEmail(`Texera: ${sender} shared a ${kind} with you`, content, email);
                      } catch {
                        setWarning("Access was granted, but an invitation email could not be sent.");
                      }
                    }
                  },
                  () => {
                    setEmails("");
                    state.refresh();
                  },
                  "Access granted."
                );
              }}>
              <label>
                Invite email addresses
                <textarea
                  value={emails}
                  disabled={operation.busy}
                  onChange={event => setEmails(event.target.value)}
                />
              </label>
              <label>
                New access
                <select
                  value={privilege}
                  disabled={operation.busy}
                  onChange={event => setPrivilege(event.target.value as "READ" | "WRITE")}>
                  <option value="READ">Read</option>
                  <option value="WRITE">Write</option>
                </select>
              </label>
              <button
                type="submit"
                disabled={operation.busy}>
                Grant access
              </button>
            </form>
          )}
        </>
      )}
      {confirm && (
        <section
          role="alertdialog"
          aria-label="Confirm access change">
          <p>
            {confirm.action === "publish"
              ? confirm.publish
                ? `Publishing this ${kind} grants all Texera users read access.`
                : `Unpublishing this ${kind} removes public access.`
              : confirm.action === "downgrade"
                ? `Downgrade your access to read only? You will no longer be able to edit this ${kind} or manage access.`
                : confirm.email === user.email
                  ? `Revoke your own access? You will no longer be able to view or edit this ${kind}.`
                  : `Revoke ${confirm.email}'s access?`}
          </p>
          <button
            type="button"
            disabled={operation.busy}
            onClick={() => setConfirm(undefined)}>
            Cancel change
          </button>
          <button
            type="button"
            disabled={operation.busy}
            onClick={() =>
              void operation.run(
                async () => {
                  if (confirm.action === "publish" && kind !== "computing-unit") {
                    await storage.toggle(kind, id, "publicity");
                    state.setData(current => current && { ...current, published: confirm.publish });
                  } else if (confirm.action === "downgrade") await grant(confirm.email!, "READ");
                  else await api.delete(`access/${kind}/revoke/${id}/${encodeURIComponent(confirm.email!)}`);
                },
                () => {
                  changed.current = true;
                  const selfRevoke = confirm.action === "revoke" && confirm.email === user.email;
                  setConfirm(undefined);
                  if (selfRevoke) onClose(true, true);
                  else state.refresh();
                },
                "Access updated."
              )
            }>
            Confirm change
          </button>
        </section>
      )}
      <footer>
        <button
          type="button"
          disabled={operation.busy}
          onClick={close}>
          Close sharing
        </button>
      </footer>
    </Dialog>
  );
}

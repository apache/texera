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

import { useEffect, useId, useRef, useState, type PointerEvent } from "react";
import { coeditorCursorCss, mountCodeEditor, type CodeEditorOptions, type MountedCodeEditor } from "./runtime";
import { createUiUdfParameterSync, type UiUdfParameterSyncOptions } from "../forms/udf-parameters-sync";
import type { PropertyWidgetProps } from "../forms/schema-form";
import { attachCodeDebugger, type CodeDebugSource } from "./debugger";
import { attachAnnotationActions, type AnnotationProposal, type CodeEditorAi } from "./ai-assistant";
import "./code-editor.css";

export interface CodeEditorSession extends Omit<CodeEditorOptions, "host" | "signal"> {
  title?: string;
  onFocus?: () => void;
  onOpenChange?: (open: boolean) => void;
  uiParameters?: Pick<UiUdfParameterSyncOptions, "getRows" | "onRows" | "onError">;
  debug?: CodeDebugSource;
  ai?: CodeEditorAi;
}

export function CodeareaField({ session, disabled }: PropertyWidgetProps & { session: CodeEditorSession }) {
  const [open, setOpen] = useState(false);
  return (
    <div className="react-codearea-field">
      <button
        type="button"
        disabled={open}
        onClick={() => {
          setOpen(true);
          session.onOpenChange?.(true);
        }}>
        Edit code
      </button>
      {open && (
        <CodeEditorDialog
          session={{ ...session, readOnly: session.readOnly || disabled }}
          onClose={() => {
            setOpen(false);
            session.onOpenChange?.(false);
          }}
        />
      )}
    </div>
  );
}

export function CodeEditorDialog({ session, onClose }: { session: CodeEditorSession; onClose: () => void }) {
  const container = useRef<HTMLDivElement>(null);
  const host = useRef<HTMLDivElement>(null);
  const mounted = useRef<MountedCodeEditor | undefined>(undefined);
  const latest = useRef(session);
  latest.current = session;
  const [ready, setReady] = useState(false);
  const [error, setError] = useState<string>();
  const [warning, setWarning] = useState<string>();
  const [cursorCss, setCursorCss] = useState("");
  const [retry, setRetry] = useState(0);
  const [condition, setCondition] = useState<{ line: number; value: string }>();
  const [annotation, setAnnotation] = useState<AnnotationProposal>();
  const annotationReply = useRef<((accepted: boolean) => void) | undefined>(undefined);
  const conditionContainer = useRef<HTMLDivElement>(null);
  const id = useId();
  const languageTitle = session.language[0].toUpperCase() + session.language.slice(1);
  const title = `${languageTitle} UDF: ${session.title ?? session.operatorId}`;

  useEffect(() => {
    const node = container.current!;
    const saved = localStorage.getItem(session.operatorId);
    if (saved) node.style.cssText = saved;
    const layout = () => {
      const rect = node.getBoundingClientRect();
      if (rect.right > window.innerWidth && rect.left < window.innerWidth)
        node.style.width = `${window.innerWidth - rect.left}px`;
      if (rect.bottom > window.innerHeight && rect.top < window.innerHeight)
        node.style.height = `${window.innerHeight - rect.top}px`;
      mounted.current?.layout();
    };
    window.addEventListener("resize", layout);
    const observer = typeof ResizeObserver === "undefined" ? undefined : new ResizeObserver(layout);
    observer?.observe(node);
    return () => {
      localStorage.setItem(session.operatorId, node.style.cssText);
      window.removeEventListener("resize", layout);
      observer?.disconnect();
    };
  }, [session.operatorId]);

  useEffect(() => {
    const awareness = session.awareness;
    if (!awareness) return;
    awareness.setLocalStateField("editingCode", true);
    const update = () =>
      setCursorCss(
        coeditorCursorCss(
          Array.from(awareness.getStates().values()).flatMap(state => {
            const user = state.user;
            return user && typeof user.clientId === "string" && typeof user.color === "string" ? [user] : [];
          })
        )
      );
    awareness.on("change", update);
    update();
    return () => {
      awareness.off("change", update);
      awareness.setLocalStateField("editingCode", false);
    };
  }, [session.awareness]);

  useEffect(() => {
    // Capture a session only when its document identity changes; live permission checks use latest.
    const initialSession = latest.current;
    const controller = new AbortController();
    let active = true;
    let detachParameters: ReturnType<typeof createUiUdfParameterSync> | undefined;
    const editorBindings: { dispose(): void }[] = [];
    setError(undefined);
    setReady(false);
    setWarning(undefined);
    void mountCodeEditor({
      ...initialSession,
      host: host.current!,
      signal: controller.signal,
      onLanguageServerError: message => {
        if (active) setWarning(message);
      },
    })
      .then(editor => {
        if (!active) {
          void editor.dispose();
          return;
        }
        mounted.current = editor;
        editor.updateReadOnly(latest.current.readOnly === true);
        setReady(true);
        if (
          initialSession.uiParameters &&
          (initialSession.language === "python" || initialSession.language === "r") &&
          initialSession.originalCode === undefined
        ) {
          detachParameters = createUiUdfParameterSync({
            ...initialSession.uiParameters,
            code: initialSession.code,
            language: initialSession.language,
          });
        }
        if (editor.editor && initialSession.originalCode === undefined) {
          const bindingError = (message: string) => {
            if (active) setError(message);
          };
          if (initialSession.debug)
            void attachCodeDebugger(
              editor.editor,
              initialSession.debug,
              (line, value) => {
                if (active) setCondition({ line, value });
              },
              undefined,
              bindingError,
              controller.signal
            )
              .then(binding => {
                if (active) editorBindings.push(binding);
                else binding.dispose();
              })
              .catch(reason =>
                bindingError(reason instanceof Error ? reason.message : "Debugger could not be initialized.")
              );
          if (initialSession.ai)
            void attachAnnotationActions(
              editor.editor,
              initialSession.code,
              initialSession.ai,
              proposal => {
                if (!active) return Promise.resolve(false);
                return new Promise<boolean>(resolve => {
                  annotationReply.current = resolve;
                  setAnnotation(proposal);
                });
              },
              () => latest.current.readOnly === true,
              bindingError,
              controller.signal
            )
              .then(binding => {
                if (active) editorBindings.push(binding);
                else binding.dispose();
              })
              .catch(reason =>
                bindingError(reason instanceof Error ? reason.message : "AI actions could not be initialized.")
              );
        }
      })
      .catch(reason => {
        if (active) setError(reason instanceof Error ? reason.message : "Code editor could not be opened.");
      });
    return () => {
      active = false;
      controller.abort();
      detachParameters?.dispose();
      annotationReply.current?.(false);
      annotationReply.current = undefined;
      editorBindings.forEach(binding => binding.dispose());
      const editor = mounted.current;
      mounted.current = undefined;
      void editor?.dispose();
    };
  }, [session.operatorId, session.code, session.language, session.originalCode, retry]);

  useEffect(() => {
    mounted.current?.updateReadOnly(session.readOnly === true);
  }, [session.readOnly]);

  const drag = (event: PointerEvent<HTMLHeadingElement>) => {
    if ((event.target as HTMLElement).closest("button")) return;
    const node = container.current!;
    const rect = node.getBoundingClientRect();
    const startX = event.clientX;
    const startY = event.clientY;
    const move = (pointer: globalThis.PointerEvent) => {
      node.style.left = `${Math.max(0, Math.min(window.innerWidth - Math.min(rect.width, window.innerWidth), rect.left + pointer.clientX - startX))}px`;
      node.style.top = `${Math.max(0, Math.min(window.innerHeight - 40, rect.top + pointer.clientY - startY))}px`;
      node.style.right = "auto";
    };
    const end = () => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", end);
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", end, { once: true });
  };
  const answerAnnotation = (accepted: boolean) => {
    annotationReply.current?.(accepted);
    annotationReply.current = undefined;
    setAnnotation(undefined);
  };
  const saveCondition = async () => {
    if (!condition || !latest.current.debug) return;
    try {
      await latest.current.debug.setCondition(condition.line, condition.value.trim());
      setCondition(undefined);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Breakpoint condition could not be saved.");
    }
  };

  return (
    <div
      ref={container}
      className="react-code-editor-window"
      role="dialog"
      aria-labelledby={id}
      onFocus={() => latest.current.onFocus?.()}>
      <h2 onPointerDown={drag}>
        <span id={id}>{title}</span>
        <button
          type="button"
          aria-label="Close code editor"
          onClick={onClose}>
          ×
        </button>
      </h2>
      <div className="code-editor-toolbar">
        <button
          type="button"
          aria-label="Undo code edit"
          disabled={!ready || session.readOnly || session.originalCode !== undefined}
          onClick={() => mounted.current?.undo()}>
          Undo
        </button>
        <button
          type="button"
          aria-label="Redo code edit"
          disabled={!ready || session.readOnly || session.originalCode !== undefined}
          onClick={() => mounted.current?.redo()}>
          Redo
        </button>
        {session.readOnly && <span>Read only</span>}
      </div>
      {!ready && !error && <p role="status">Loading code editor…</p>}
      {error && (
        <p role="alert">
          {error}{" "}
          <button
            type="button"
            onClick={() => setRetry(retry + 1)}>
            Retry editor
          </button>
        </p>
      )}
      {warning && (
        <p
          role="status"
          className="code-editor-lsp-warning">
          {warning}. Editing and syntax highlighting remain available.
        </p>
      )}
      <div
        ref={host}
        className="react-monaco-host"
      />
      {condition && (
        <div
          ref={conditionContainer}
          className="code-condition-popup"
          role="dialog"
          aria-label={`Breakpoint condition at line ${condition.line}`}>
          <label>
            Breakpoint condition
            <textarea
              value={condition.value}
              onChange={event => setCondition({ ...condition, value: event.currentTarget.value })}
              onKeyDown={event => {
                if (event.key === "Enter" && !event.shiftKey) {
                  event.preventDefault();
                  void saveCondition();
                }
              }}
              onBlur={event => {
                if (!conditionContainer.current?.contains(event.relatedTarget as Node | null)) void saveCondition();
              }}
            />
          </label>
          <button
            type="button"
            onClick={() => void saveCondition()}>
            Save condition
          </button>
          <button
            type="button"
            onClick={() => setCondition(undefined)}>
            Cancel condition
          </button>
        </div>
      )}
      {annotation && (
        <section
          className="code-annotation-popup"
          role="dialog"
          aria-label="Type annotation suggestion">
          <h3>Type annotation suggestion</h3>
          <pre>{annotation.code}</pre>
          <pre>{annotation.suggestion}</pre>
          <button
            type="button"
            disabled={session.readOnly}
            onClick={() => answerAnnotation(true)}>
            Accept annotation
          </button>
          <button
            type="button"
            onClick={() => answerAnnotation(false)}>
            Reject annotation
          </button>
        </section>
      )}
      <style>{cursorCss}</style>
    </div>
  );
}

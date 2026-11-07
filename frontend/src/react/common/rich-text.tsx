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

import DOMPurify from "dompurify";
import { marked } from "marked";
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import "./rich-text.css";

export interface RichTextProps {
  content?: string | null;
  format?: "markdown" | "html";
  className?: string;
  label?: string;
  collapsedHeight?: number;
}
export function sanitizeRichText(content: string | null | undefined, format: "markdown" | "html" = "markdown"): string {
  const source = content ?? "";
  const html = format === "html" ? source : marked.parse(source, { async: false, gfm: true });
  return DOMPurify.sanitize(html, {
    USE_PROFILES: { html: true },
    FORBID_ATTR: ["style"],
    FORBID_TAGS: [
      "style",
      "iframe",
      "object",
      "embed",
      "form",
      "input",
      "textarea",
      "select",
      "button",
      "base",
      "meta",
      "link",
    ],
  });
}

/** User-authored markdown and deployment HTML share one sanitization boundary. */
export function RichText({ content, format = "markdown", className = "", label, collapsedHeight }: RichTextProps) {
  const html = useMemo(() => sanitizeRichText(content, format), [content, format]);
  const element = useRef<HTMLDivElement>(null);
  const [expanded, setExpanded] = useState(false);
  const [overflow, setOverflow] = useState(false);
  useEffect(() => setExpanded(false), [content]);
  useLayoutEffect(() => {
    if (!element.current || !collapsedHeight) {
      setOverflow(false);
      return;
    }
    const measure = () => setOverflow(Boolean(element.current && element.current.scrollHeight > collapsedHeight));
    measure();
    if (typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver(measure);
    observer.observe(element.current);
    return () => observer.disconnect();
  }, [html, collapsedHeight]);
  return (
    <div className={`rich-text-container ${className}`}>
      <div
        ref={element}
        className="rich-text"
        aria-label={label}
        style={collapsedHeight && !expanded ? { maxHeight: collapsedHeight, overflow: "hidden" } : undefined}
        dangerouslySetInnerHTML={{ __html: html }}
      />
      {collapsedHeight && overflow && (
        <button
          className="rich-text-expand"
          type="button"
          aria-expanded={expanded}
          onClick={() => setExpanded(!expanded)}>
          {expanded ? "View less" : "View more"}
        </button>
      )}
    </div>
  );
}

const TOOLS = [
  { label: "Bold", prefix: "**", suffix: "**", fallback: "bold" },
  { label: "Italic", prefix: "_", suffix: "_", fallback: "italic" },
  { label: "Strikethrough", prefix: "~~", suffix: "~~", fallback: "text" },
  { label: "Heading", prefix: "### ", suffix: "", fallback: "Heading" },
  { label: "Code", prefix: "`", suffix: "`", fallback: "code" },
  { label: "Code block", prefix: "\n```\n", suffix: "\n```\n", fallback: "code" },
  { label: "Quote", prefix: "> ", suffix: "", fallback: "quote" },
  { label: "Bullet list", prefix: "- ", suffix: "", fallback: "item" },
  { label: "Numbered list", prefix: "1. ", suffix: "", fallback: "item" },
  { label: "Link", prefix: "[", suffix: "](url)", fallback: "text" },
  { label: "Image", prefix: "![", suffix: "](url)", fallback: "alt text" },
  {
    label: "Table",
    prefix: "\n| Col 1 | Col 2 | Col 3 |\n| --- | --- | --- |\n| ",
    suffix: " |  |  |\n",
    fallback: "",
  },
  { label: "Divider", prefix: "\n---\n", suffix: "", fallback: "" },
] as const;

export interface MarkdownEditorProps {
  value?: string;
  onChange(value: string): void | Promise<void>;
  label?: string;
  disabled?: boolean;
  initiallyEditing?: boolean;
  onDraftChange?: (value: string) => void;
}
export function MarkdownEditor({
  value = "",
  onChange,
  label = "Description",
  disabled = false,
  initiallyEditing = false,
  onDraftChange,
}: MarkdownEditorProps) {
  const [editing, setEditing] = useState(initiallyEditing);
  const [draft, setDraft] = useState(value);
  const [preview, setPreview] = useState(false);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string>();
  const input = useRef<HTMLTextAreaElement>(null);
  useEffect(() => {
    if (!editing) setDraft(value);
  }, [value, editing]);
  function insert(tool: (typeof TOOLS)[number]) {
    const textarea = input.current;
    if (!textarea) return;
    const start = textarea.selectionStart,
      end = textarea.selectionEnd;
    const selected = draft.slice(start, end) || tool.fallback;
    const next = draft.slice(0, start) + tool.prefix + selected + tool.suffix + draft.slice(end);
    setDraft(next);
    onDraftChange?.(next);
    queueMicrotask(() => {
      textarea.focus();
      textarea.setSelectionRange(start + tool.prefix.length, start + tool.prefix.length + selected.length);
    });
  }
  async function save() {
    setPending(true);
    setError(undefined);
    try {
      await onChange(draft);
      setEditing(false);
      setPreview(false);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "The description could not be saved.");
    } finally {
      setPending(false);
    }
  }
  if (!editing)
    return (
      <section className="markdown-editor">
        <RichText
          content={value}
          collapsedHeight={320}
        />
        <button
          type="button"
          disabled={disabled}
          onClick={() => {
            setDraft(value);
            setEditing(true);
            setError(undefined);
          }}>
          Edit {label}
        </button>
      </section>
    );
  return (
    <section className="markdown-editor">
      {error && <p role="alert">{error}</p>}
      <div
        className="markdown-toolbar"
        role="toolbar"
        aria-label="Markdown formatting">
        {TOOLS.map(tool => (
          <button
            type="button"
            disabled={disabled || pending || preview}
            key={tool.label}
            onClick={() => insert(tool)}>
            {tool.label}
          </button>
        ))}
      </div>
      <button
        type="button"
        disabled={pending}
        aria-pressed={preview}
        onClick={() => setPreview(!preview)}>
        {preview ? "Write markdown" : "Preview markdown"}
      </button>
      {preview ? (
        <RichText content={draft} />
      ) : (
        <label>
          {label}
          <textarea
            ref={input}
            value={draft}
            onChange={event => {
              setDraft(event.target.value);
              onDraftChange?.(event.target.value);
            }}
            disabled={disabled || pending}
            rows={10}
          />
        </label>
      )}
      <footer>
        <button
          type="button"
          disabled={pending}
          onClick={() => {
            setDraft(value);
            setEditing(false);
            setPreview(false);
          }}>
          Cancel editing
        </button>
        <button
          type="button"
          disabled={disabled || pending}
          onClick={() => void save()}>
          Save {label}
        </button>
      </footer>
    </section>
  );
}

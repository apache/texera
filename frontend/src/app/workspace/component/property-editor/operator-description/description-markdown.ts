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

import { Marked, Tokens } from "marked";

/**
 * Markdown for operator descriptions. A description is untrusted (any editor, an imported workflow or
 * an agent can write it) and is shown to everyone who opens the operator, so the renderer is stricter
 * than the app's chat markdown:
 *  - raw HTML is shown as text, so a note cannot inject elements or reuse the app's CSS classes to
 *    draw over the page;
 *  - images are shown as links, so opening an operator never makes the browser fetch a remote URL
 *    (no tracking pixels);
 *  - only http(s) and mailto links become anchors, and they open in a new tab without a referrer.
 * Angular's sanitizer still runs on the result where it is bound with [innerHTML].
 */
const SAFE_URL = /^(https?:|mailto:)/i;

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function safeAnchor(href: string, label: string, title?: string | null): string {
  const url = href.trim();
  if (!SAFE_URL.test(url)) return label;
  const titleAttr = title ? ` title="${escapeHtml(title)}"` : "";
  return `<a href="${escapeHtml(url)}"${titleAttr} target="_blank" rel="noopener noreferrer nofollow">${label}</a>`;
}

const descriptionMarked = new Marked({
  async: false,
  gfm: true,
  breaks: true,
  renderer: {
    html(token: Tokens.HTML | Tokens.Tag): string {
      return escapeHtml(token.text);
    },
    image(token: Tokens.Image): string {
      const label = escapeHtml(token.text || token.href);
      return safeAnchor(token.href, `🖼 ${label}`, token.title);
    },
    link(this: { parser: { parseInline(tokens: Tokens.Generic[]): string } }, token: Tokens.Link): string {
      return safeAnchor(token.href, this.parser.parseInline(token.tokens), token.title);
    },
  },
});

export function renderDescriptionMarkdown(text: string): string {
  if (!text) return "";
  return descriptionMarked.parse(text) as string;
}

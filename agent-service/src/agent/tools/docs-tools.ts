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

import { z } from "zod";
import { tool } from "ai";
import type { Logger } from "pino";
import { createErrorResult, createToolResult } from "./tools-utility";
import { existsSync } from "node:fs";
import { cutText, loadDocsIndex, type DocsIndex } from "./docs-index";
import { createLogger } from "../../logger";

export const TOOL_NAME_SEARCH_DOCS = "searchDocs";
export const TOOL_NAME_READ_DOC = "readDoc";

/** Largest readDoc output; the rest is read by calling again with the offset the last line gives. */
export const DOC_RESULT_CHAR_LIMIT = 4000;
const DEFAULT_SEARCH_LIMIT = 5;
const MAX_SEARCH_LIMIT = 10;
/** Most characters of a query or path repeated back in a "nothing found" message. */
const ECHO_CHAR_LIMIT = 80;
/** A clip ends at the last line break when one lies this close to the limit, so no line is cut in two. */
const LINE_CUT_WINDOW = 500;
/** Most entries listed for one directory. */
const DIRECTORY_LISTING_LIMIT = 50;
const BROWSE_HINT = "call readDoc with one of these paths to browse that area";
const TOOLS_OFF = "docs tools stay off until the service restarts";

const moduleLog = createLogger("DocsTools");
const sharedIndexes = new Map<string, DocsIndex | null>();

/**
 * Loads the index for a directory once per process and reuses it for every agent. A directory
 * that is missing, has no page or cannot be read gives null, logged once.
 */
export function getSharedDocsIndex(dir: string, log: Pick<Logger, "info" | "warn"> = moduleLog): DocsIndex | null {
  if (!sharedIndexes.has(dir)) {
    let index: DocsIndex | null = null;
    try {
      index = loadDocsIndex(dir);
      if (index) log.info({ dir, pages: index.pages().length }, "docs index loaded");
      else if (!existsSync(dir)) log.warn({ dir }, `docs directory not found, ${TOOLS_OFF}`);
      else log.warn({ dir }, `no documentation pages in the docs directory, ${TOOLS_OFF}`);
    } catch (error) {
      log.warn({ dir, err: error }, `failed to load the docs index, ${TOOLS_OFF}`);
    }
    sharedIndexes.set(dir, index);
  }
  return sharedIndexes.get(dir) ?? null;
}

function echo(text: string): string {
  const trimmed = text.trim();
  return trimmed.length <= ECHO_CHAR_LIMIT ? trimmed : `${cutText(trimmed, ECHO_CHAR_LIMIT)}…`;
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** The part of text from offset on, clipped at the limit with a line that says how to read on. */
function clip(text: string, offset: number, hasSection: boolean): string {
  // An offset inside a surrogate pair starts at the pair.
  const code = text.charCodeAt(offset);
  const start = offset > 0 && code >= 0xdc00 && code <= 0xdfff ? offset - 1 : offset;
  const rest = text.slice(start);
  if (rest.length <= DOC_RESULT_CHAR_LIMIT) return rest;
  let shown = cutText(rest, DOC_RESULT_CHAR_LIMIT);
  let next = start + shown.length;
  const lineEnd = shown.lastIndexOf("\n");
  if (lineEnd >= DOC_RESULT_CHAR_LIMIT - LINE_CUT_WINDOW) {
    shown = shown.slice(0, lineEnd);
    next = start + lineEnd + 1;
  }
  const same = hasSection ? "the same path and section" : "the same path";
  return `${shown}\n[truncated, ${text.length - next} more characters. Call readDoc again with ${same} and offset ${next} to continue.]`;
}

/** The pages and sub-directories directly under a directory, or undefined when it holds none. */
function directoryListing(index: DocsIndex, dir: string): string | undefined {
  const { pages, dirs } = index.children(dir);
  const entries = [
    ...pages.map(page => `- ${page.path}: ${page.title}`),
    ...dirs.map(sub => {
      const count = sub.pageCount === 1 ? "1 page" : `${sub.pageCount} pages`;
      return `- ${sub.path}/ (${count})${sub.title ? `: ${sub.title}` : ""}`;
    }),
  ];
  if (entries.length === 0) return undefined;
  const shown = entries.slice(0, DIRECTORY_LISTING_LIMIT);
  if (entries.length > shown.length) {
    shown.push(`- and ${entries.length - shown.length} more, use searchDocs to find them`);
  }
  const name = dir === "" ? "the documentation root" : `${dir}/`;
  return `Pages under ${name}:\n${shown.join("\n")}`;
}

function unknownPageResult(index: DocsIndex, path: string): string {
  const close = index.closest(path).map(page => `- ${page.path}: ${page.title}`);
  const closest =
    close.length > 0 ? ` Closest pages:\n${close.join("\n")}\nOr ${BROWSE_HINT}:` : ` To find a page, ${BROWSE_HINT}:`;
  return createErrorResult(`Unknown page "${echo(path)}".${closest}\n${index.outline()}`);
}

export function createSearchDocsTool(index: DocsIndex) {
  return tool({
    description:
      "Search the Texera documentation by keywords. Returns the best matching sections with the page path to pass to readDoc and the link to the published page. When nothing matches, returns a short outline of the documentation areas instead.",
    inputSchema: z.object({
      query: z
        .string()
        .describe("Keywords describing what to look up, e.g. 'upload dataset' or 'CSVFileScan delimiter'"),
      // Any number is accepted and clamped, so a value out of range still gets an answer. Null counts as absent.
      limit: z.number().nullish().describe("Maximum number of sections to return, 1 to 10, 5 by default"),
    }),
    execute: async (args: { query: string; limit?: number | null }) => {
      try {
        const given = typeof args.limit === "number" && Number.isFinite(args.limit) ? args.limit : undefined;
        const limit =
          given === undefined ? DEFAULT_SEARCH_LIMIT : Math.min(MAX_SEARCH_LIMIT, Math.max(1, Math.trunc(given)));
        // No indexable term or no section sharing one: an outline to browse is more useful than nothing.
        const hits = index.search(args.query, limit);
        if (hits.length === 0) {
          return createToolResult(
            `No section matched "${echo(args.query)}". Try other keywords, or ${BROWSE_HINT}:\n${index.outline()}`
          );
        }
        const lines = hits.map(
          (h, i) => `${i + 1}. ${h.headingChain.join(" > ")}\n   path: ${h.path}\n   link: ${h.url}\n   ${h.snippet}`
        );
        const changed =
          given === undefined || given === limit
            ? ""
            : `\n(limit ${given} was changed to ${limit}, it has to be a whole number from 1 to ${MAX_SEARCH_LIMIT})`;
        return createToolResult(
          `${lines.join("\n")}\n\nCall readDoc with a path to read the page, or with a path and a section name for one section.${changed}`
        );
      } catch (error) {
        return createErrorResult(errorText(error));
      }
    },
  });
}

export function createReadDocTool(index: DocsIndex) {
  return tool({
    description:
      "Read a Texera documentation page. Without a section name it returns the whole page when it is short, otherwise its section list and introduction, and for a directory also the pages under it. With a section name it returns that section. Output is clipped; its last line then gives the offset to continue from.",
    inputSchema: z.object({
      path: z
        .string()
        .describe(
          "Page or directory path as returned by searchDocs, e.g. 'getting-started/ui-overview' or 'tutorials'"
        ),
      section: z
        .string()
        .nullish()
        .describe("Section name to read, as listed by searchDocs or by readDoc without a section"),
      // Any number is accepted and checked in the tool, so a bad one comes back as tool output. Null counts as absent.
      offset: z
        .number()
        .nullish()
        .describe("Character offset to continue a clipped result from, as given in its last line"),
    }),
    execute: async (args: { path: string; section?: string | null; offset?: number | null }) => {
      try {
        const offset = args.offset ?? 0;
        if (!Number.isInteger(offset) || offset < 0) {
          return createErrorResult("offset must be a whole number of characters, 0 or more.");
        }
        const section = typeof args.section === "string" && args.section.trim() !== "" ? args.section : undefined;
        const path = index.resolve(args.path);
        if (path === undefined) {
          // A directory without an _index page still lists what is under it.
          const dir = section === undefined ? index.directory(args.path) : undefined;
          const listing = dir === undefined ? undefined : directoryListing(index, dir);
          return listing === undefined ? unknownPageResult(index, args.path) : createToolResult(listing);
        }
        let text: string | undefined;
        if (section === undefined) {
          const whole = index.page(path) ?? "";
          text = whole.length <= DOC_RESULT_CHAR_LIMIT ? whole : index.read(path);
          if (text !== undefined && (path === "_index" || path.endsWith("/_index"))) {
            const listing = directoryListing(index, path === "_index" ? "" : path.slice(0, -"/_index".length));
            if (listing !== undefined) text = `${text}\n\n${listing}`;
          }
        } else {
          text = index.read(path, section);
        }
        if (text === undefined) {
          const names = index.sections(path).map(name => `- ${name}`);
          return createErrorResult(
            `Unknown section "${echo(section ?? "")}" on page "${path}". Available sections:\n${names.join("\n")}`
          );
        }
        if (offset > 0 && offset >= text.length) {
          return createErrorResult(`offset ${offset} is past the end of this text (${text.length} characters).`);
        }
        return createToolResult(`${path} (${index.url(path)})\n\n${clip(text, offset, section !== undefined)}`);
      } catch (error) {
        return createErrorResult(errorText(error));
      }
    },
  });
}

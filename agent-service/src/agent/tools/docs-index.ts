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

import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, sep } from "node:path";

/**
 * Base of the page links handed to the model. Each page under it is a stub on the published
 * site that forwards to the same page of the current release.
 */
export const DOCS_SITE_BASE = "https://texera.apache.org/docs/latest";

const SITE_ORIGIN = new URL(DOCS_SITE_BASE).origin;

const DEFAULT_EXCLUDED_DIRS = ["contribution-guidelines"];
const HEADING_WEIGHT = 3;
const SNIPPET_CHAR_LIMIT = 200;
/** A directory with more pages than this is shown in the outline through its sub-directories. */
const OUTLINE_EXPAND_ABOVE = 60;
/** Most pages of an expanded directory the outline names one by one. */
const OUTLINE_DIRECT_PAGES = 8;

export interface DocsPage {
  path: string;
  title: string;
  description: string;
}

export interface DocsSection {
  path: string;
  /** Page title, then the headings above the section, then its own name, unique on the page. */
  headingChain: string[];
  text: string;
}

export interface DocsDirectory {
  path: string;
  pageCount: number;
  title?: string;
}

export interface DocsSearchHit {
  path: string;
  title: string;
  headingChain: string[];
  url: string;
  snippet: string;
  score: number;
}

export interface LoadDocsIndexOptions {
  exclude?: string[];
}

/** The first `limit` UTF-16 units of text, one fewer when that would split a surrogate pair. */
export function cutText(text: string, limit: number): string {
  if (text.length <= limit) return text;
  const last = text.charCodeAt(limit - 1);
  return text.slice(0, last >= 0xd800 && last <= 0xdbff ? limit - 1 : limit);
}

/** Strips common English suffixes so singular and plural or verb forms share a token. */
function stem(token: string): string {
  if (token.length > 4 && token.endsWith("ies")) return `${token.slice(0, -3)}y`;
  if (token.length > 5 && token.endsWith("ing")) return token.slice(0, -3);
  if (token.length > 4 && token.endsWith("ed")) return token.slice(0, -2);
  if (token.length > 3 && token.endsWith("s") && !token.endsWith("ss")) return token.slice(0, -1);
  return token;
}

const STOPWORDS = new Set([
  "a",
  "an",
  "the",
  "is",
  "are",
  "do",
  "does",
  "how",
  "what",
  "where",
  "i",
  "my",
  "to",
  "in",
  "of",
  "for",
  "on",
  "with",
  "and",
  "or",
  "it",
  "this",
  "that",
  "can",
  "you",
]);

/** Splits camelCase, lowercases, stems, drops one-char tokens and stopwords, and keeps each unsplit word too. */
export function tokenize(text: string): string[] {
  const tokens: string[] = [];
  for (const word of text.split(/[^A-Za-z0-9]+/)) {
    if (word.length === 0) continue;
    const lower = word.toLowerCase();
    if (STOPWORDS.has(lower)) continue;
    const parts = word
      .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
      .replace(/([A-Z]+)([A-Z][a-z])/g, "$1 $2")
      .toLowerCase()
      .split(" ")
      .filter(t => t.length > 1 && !STOPWORDS.has(t))
      .map(stem);
    tokens.push(...parts);
    const whole = stem(lower);
    if (whole.length > 1 && !(parts.length === 1 && parts[0] === whole)) tokens.push(whole);
  }
  return tokens;
}

function stripAsfHeader(markdown: string): string {
  const trimmed = markdown.trimStart();
  if (!trimmed.startsWith("<!--")) return markdown;
  const end = trimmed.indexOf("-->");
  return end === -1 ? markdown : trimmed.slice(end + 3);
}

function parseFrontMatter(markdown: string): { title?: string; description?: string; body: string } {
  const trimmed = markdown.trimStart();
  const match = trimmed.match(/^---\r?\n([\s\S]*?)\r?\n---\r?\n?/);
  if (!match) return { body: trimmed };
  const fields: Record<string, string> = {};
  const lines = match[1].split(/\r?\n/);
  for (let i = 0; i < lines.length; i++) {
    const kv = lines[i].match(/^([A-Za-z_]+):\s*(.*)$/);
    if (!kv) continue;
    let value = kv[2].trim();
    if (/^[>|][-+]?$/.test(value)) {
      const folded: string[] = [];
      while (i + 1 < lines.length && /^\s+\S/.test(lines[i + 1])) folded.push(lines[++i].trim());
      value = folded.join(" ");
    }
    fields[kv[1]] = value.replace(/^["'](.*)["']$/, "$1");
  }
  return { title: fields.title, description: fields.description, body: trimmed.slice(match[0].length) };
}

/** Published URL of a page. An _index page is published at its directory. */
export function docsPageUrl(path: string): string {
  if (path === "_index") return `${DOCS_SITE_BASE}/`;
  const dir = path.endsWith("/_index") ? path.slice(0, -"/_index".length) : path;
  return `${DOCS_SITE_BASE}/${dir}/`;
}

function resolveLinkTarget(target: string, pagePath: string): string {
  if (/^[a-z][a-z0-9+.-]*:/i.test(target) || target.startsWith("//")) return target;
  if (target.startsWith("#")) return `${docsPageUrl(pagePath)}${target}`;
  if (target === "/docs/latest" || target.startsWith("/docs/latest/")) return `${SITE_ORIGIN}${target}`;
  if (target.startsWith("/docs/")) return `${DOCS_SITE_BASE}${target.slice("/docs".length)}`;
  if (target.startsWith("/")) return `${SITE_ORIGIN}${target}`;
  try {
    const source = target.match(/^([^#?]*)\.md([#?].*)?$/);
    if (source) {
      // A link to a markdown file is relative to the directory of the file it is written in.
      const sourceDir = pagePath.includes("/") ? pagePath.slice(0, pagePath.lastIndexOf("/") + 1) : "";
      const file = new URL(source[1], `${DOCS_SITE_BASE}/${sourceDir}`).href;
      const page = file.endsWith("/_index") ? file.slice(0, -"_index".length) : `${file}/`;
      return `${page}${source[2] ?? ""}`;
    }
    return new URL(target, docsPageUrl(pagePath)).href;
  } catch {
    return target;
  }
}

const FENCE = /^\s*(```|~~~)/;
const LINK_TARGET = /(\]\(\s*<?)([^)\s>]+)(>?(?:\s+"[^"]*")?\s*\))/g;
/** One or more links that only point up the tree, joined by ">", optionally ending in the page name. */
const BREADCRUMB = /^\[[^\]]+\]\((?:\.\.\/)+\)(?:\s*>\s*\[[^\]]+\]\((?:\.\.\/)+\))*(?:\s*>\s*[^[\]]+)?$/;

/** Applies a rewrite to each run of lines outside fenced code blocks. */
function mapProse(markdown: string, rewrite: (prose: string) => string): string {
  const out: string[] = [];
  let prose: string[] = [];
  let inFence = false;
  const flush = () => {
    if (prose.length > 0) out.push(rewrite(prose.join("\n")));
    prose = [];
  };
  for (const line of markdown.split(/\r?\n/)) {
    if (FENCE.test(line)) {
      if (!inFence) flush();
      inFence = !inFence;
      out.push(line);
    } else if (inFence) {
      out.push(line);
    } else {
      prose.push(line);
    }
  }
  flush();
  return out.join("\n");
}

/** Rewrites relative and site-relative link targets to the published URL they lead to. */
export function resolveDocLinks(markdown: string, pagePath: string): string {
  return mapProse(markdown, prose =>
    prose.replace(LINK_TARGET, (_, open, target, close) => `${open}${resolveLinkTarget(target, pagePath)}${close}`)
  );
}

const REMOVED = "\u0000";

/** Drops HTML comments and img tags, which only a browser can use. Inline code is left as it is. */
function dropHtmlNoise(markdown: string): string {
  return mapProse(markdown, prose =>
    prose
      .replace(/(`+)[^`\n]*\1|<!--[\s\S]*?-->|<img\b[^>]*>/gi, match => (match.startsWith("`") ? match : REMOVED))
      .split("\n")
      // A line that held nothing else goes with what was removed from it.
      .filter(line => !line.includes(REMOVED) || line.split(REMOVED).join("").trim() !== "")
      .join("\n")
      .split(REMOVED)
      .join("")
  );
}

function dropBreadcrumbs(markdown: string): string {
  return mapProse(markdown, prose =>
    prose
      .split("\n")
      .filter(line => !BREADCRUMB.test(line.trim()))
      .join("\n")
  );
}

/** Replaces each link or image by its text, for tokenizing, snippets and heading names. */
function withoutLinkTargets(text: string): string {
  return text.replace(/!?\[([^\]]*)\]\([^)]*\)/g, "$1");
}

/** A heading as it is shown and matched: link text instead of links, no backslash escapes. */
function cleanHeading(raw: string): string {
  return withoutLinkTargets(raw)
    .replace(/\\([\\`*_{}[\]()#+\-.!>~|])/g, "$1")
    .replace(/\s+/g, " ")
    .trim();
}

/** Form in which section names are compared loosely: case, punctuation and symbols other than + and # do not count. */
function sectionKey(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^\p{L}\p{N}+#]+/gu, " ")
    .trim();
}

/** What makes two section names on a page the same name. */
function nameIdentity(name: string): string {
  return sectionKey(name) || name.toLowerCase();
}

function sameName(a: string, b: string): boolean {
  return a.toLowerCase() === b.toLowerCase() || (sectionKey(a) !== "" && sectionKey(a) === sectionKey(b));
}

/** A section as stored: its name before numbering, and whether it is a heading with only sub-sections. */
interface StoredSection extends DocsSection {
  ownName: string;
  container: boolean;
}

function stripShortcodes(markdown: string): string {
  return markdown.replace(/\{\{%\s*\/?\s*pageinfo[^%]*%\}\}/g, "").replace(/\{\{<\s*\/?\s*alert[^>]*>\}\}/g, "");
}

/** Every section of a page in order, the introduction first, including those without text. */
function splitSections(path: string, pageTitle: string, body: string): DocsSection[] {
  const all: DocsSection[] = [];
  const chain: string[] = [pageTitle];
  // Level of each chain entry, so a heading closes every open heading of its own level or deeper.
  const levels: number[] = [0];
  let current: DocsSection = { path, headingChain: [...chain], text: "" };
  let buffer: string[] = [];
  const flush = () => {
    current.text = buffer.join("\n").trim();
    all.push(current);
    buffer = [];
  };
  let inFence = false;
  for (const line of body.split(/\r?\n/)) {
    if (/^\s*(```|~~~)/.test(line)) {
      inFence = !inFence;
      buffer.push(line);
      continue;
    }
    const heading = inFence ? null : line.match(/^(#{1,3})\s+(.+?)(?:\s+#+)?\s*$/);
    if (!heading) {
      buffer.push(line);
      continue;
    }
    flush();
    const level = heading[1].length;
    while (levels.length > 1 && levels[levels.length - 1] >= level) {
      levels.pop();
      chain.pop();
    }
    levels.push(level);
    chain.push(cleanHeading(heading[2]));
    current = { path, headingChain: [...chain], text: "" };
  }
  flush();
  return all;
}

/** Title of the first heading section on a page without front matter, if the page has one. */
function firstHeading(path: string, body: string): string | undefined {
  return splitSections(path, "", body).find(s => s.headingChain.length > 1)?.headingChain[1];
}

function listMarkdownFiles(root: string, excluded: Set<string>): string[] {
  const files: string[] = [];
  const walk = (dir: string) => {
    for (const entry of readdirSync(dir)) {
      const full = join(dir, entry);
      const rel = relative(root, full).split(sep).join("/");
      if (statSync(full).isDirectory()) {
        if (!excluded.has(rel)) walk(full);
      } else if (entry.endsWith(".md")) {
        files.push(rel);
      }
    }
  };
  walk(root);
  return files.sort();
}

/** In-memory tf-idf index over the markdown pages of a docs directory, split by heading. */
export class DocsIndex {
  private readonly pageList: DocsPage[] = [];
  private readonly pageByPath = new Map<string, DocsPage>();
  private readonly pathByKey = new Map<string, string>();
  private readonly directoryByKey = new Map<string, string>([["", ""]]);
  private readonly sectionsByPath = new Map<string, StoredSection[]>();
  private readonly sectionList: DocsSection[] = [];
  private readonly termFrequencies: Map<string, number>[] = [];
  private readonly documentFrequency = new Map<string, number>();
  private readonly sectionLengths: number[] = [];

  constructor(pages: { path: string; title: string; description: string; body: string }[]) {
    for (const page of pages) {
      const entry = { path: page.path, title: page.title, description: page.description };
      this.pageList.push(entry);
      this.pageByPath.set(page.path, entry);
      if (!this.pathByKey.has(page.path.toLowerCase())) this.pathByKey.set(page.path.toLowerCase(), page.path);
      const segments = page.path.split("/");
      for (let depth = 1; depth < segments.length; depth++) {
        const dir = segments.slice(0, depth).join("/");
        this.directoryByKey.set(dir.toLowerCase(), dir);
      }
      // Ranking uses the source text without HTML comments and img tags, so it does not depend on
      // how links and breadcrumbs are shown to the reader.
      const prepared = splitSections(page.path, page.title, page.body).map(source => {
        const isIntroduction = source.headingChain.length === 1;
        const indexed = dropHtmlNoise(source.text);
        let text = resolveDocLinks(dropBreadcrumbs(indexed), page.path).trim();
        if (text.length === 0 && isIntroduction) text = page.description;
        return { source, isIntroduction, indexed, text };
      });
      // A heading without text of its own stays when a section under it stays.
      const kept = prepared.map(entry => entry.text.length > 0);
      for (let i = prepared.length - 1; i >= 0; i--) {
        if (kept[i] || prepared[i].isIntroduction) continue;
        const depth = prepared[i].source.headingChain.length;
        for (let j = i + 1; j < prepared.length && prepared[j].source.headingChain.length > depth; j++) {
          if (kept[j]) kept[i] = true;
        }
      }
      const built = prepared
        .filter((_, i) => kept[i])
        .map(({ source, indexed, text }) => {
          const ownName = source.headingChain[source.headingChain.length - 1];
          const section: StoredSection = { ...source, text, ownName, container: text.length === 0 };
          return { section, indexed };
        });
      // A page with nothing before its first heading is introduced by its description.
      if (built[0]?.section.headingChain.length !== 1 && (page.description || built.length === 0)) {
        const introduction: StoredSection = {
          path: page.path,
          headingChain: [page.title],
          text: page.description,
          ownName: page.title,
          container: false,
        };
        built.unshift({ section: introduction, indexed: "" });
      }
      // A later section with a name already used on the page gets a number, so each can be read.
      const used = new Set<string>();
      for (const { section } of built) {
        let name = section.ownName;
        for (let n = 2; used.has(nameIdentity(name)); n++) name = `${section.ownName} (${n})`;
        used.add(nameIdentity(name));
        section.headingChain = [...section.headingChain.slice(0, -1), name];
      }
      const sections = built.map(entry => entry.section);
      // A heading without text reads as the list of the sections directly under it.
      sections.forEach((section, i) => {
        if (!section.container) return;
        const depth = section.headingChain.length;
        const names: string[] = [];
        for (let j = i + 1; j < sections.length && sections[j].headingChain.length > depth; j++) {
          if (sections[j].headingChain.length === depth + 1) names.push(`- ${DocsIndex.nameOf(sections[j])}`);
        }
        section.text = `Sub-sections:\n${names.join("\n")}`;
      });
      for (const { section, indexed } of built) this.addSection(section, indexed, page.description);
      this.sectionsByPath.set(page.path, sections);
    }
  }

  /** Indexes a section by its headings, the page description for an introduction, and its source text. */
  private addSection(section: DocsSection, sourceText: string, description: string): void {
    const tf = new Map<string, number>();
    const bump = (tokens: string[], weight: number) => {
      for (const t of tokens) tf.set(t, (tf.get(t) ?? 0) + weight);
    };
    bump(tokenize(section.headingChain.join(" ")), HEADING_WEIGHT);
    if (section.headingChain.length === 1) bump(tokenize(description), HEADING_WEIGHT);
    bump(tokenize(sourceText), 1);
    let length = 0;
    for (const [term, count] of tf) {
      length += count;
      this.documentFrequency.set(term, (this.documentFrequency.get(term) ?? 0) + 1);
    }
    this.sectionList.push(section);
    this.termFrequencies.push(tf);
    this.sectionLengths.push(length);
  }

  pages(): DocsPage[] {
    return [...this.pageList];
  }

  private static nameOf(section: DocsSection): string {
    return section.headingChain[section.headingChain.length - 1];
  }

  sections(path: string): string[] {
    return (this.sectionsByPath.get(path) ?? []).map(DocsIndex.nameOf);
  }

  url(path: string): string {
    return docsPageUrl(path);
  }

  /** A path without site prefix, anchor, query, empty or "." segments and .md, in the case it was given. */
  private static clean(input: string): string {
    let path = input.trim();
    if (path.toLowerCase().startsWith(DOCS_SITE_BASE.toLowerCase())) path = path.slice(DOCS_SITE_BASE.length);
    return path
      .replace(/[#?].*$/, "")
      .split("/")
      .filter(segment => segment !== "" && segment !== ".")
      .join("/")
      .replace(/\.md$/i, "");
  }

  /** Canonical path of the page a path, directory, /docs path or published URL names, whatever its case. */
  resolve(input: string): string | undefined {
    const path = DocsIndex.clean(input);
    const candidates = [path, path.replace(/^docs\/latest(\/|$)/i, ""), path.replace(/^docs(\/|$)/i, "")];
    const withIndex = candidates.flatMap(c => [c, c === "" ? "_index" : `${c}/_index`]);
    // A page spelled exactly as asked wins over one that differs only by case.
    return (
      withIndex.find(c => this.pageByPath.has(c)) ??
      withIndex.map(c => this.pathByKey.get(c.toLowerCase())).find(found => found !== undefined)
    );
  }

  /** Canonical name of a directory that holds pages, "" for the root. */
  directory(input: string): string | undefined {
    return this.directoryByKey.get(DocsIndex.clean(input).toLowerCase());
  }

  /** Pages directly inside a directory, without its _index page, and its sub-directories. */
  children(dir: string): { pages: DocsPage[]; dirs: DocsDirectory[] } {
    const prefix = dir === "" ? "" : `${dir}/`;
    const pages: DocsPage[] = [];
    const counts = new Map<string, number>();
    for (const page of this.pageList) {
      if (!page.path.startsWith(prefix)) continue;
      const rest = page.path.slice(prefix.length);
      const slash = rest.indexOf("/");
      if (slash === -1) {
        if (rest !== "_index") pages.push(page);
      } else {
        const sub = `${prefix}${rest.slice(0, slash)}`;
        counts.set(sub, (counts.get(sub) ?? 0) + 1);
      }
    }
    const dirs = [...counts.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([path, pageCount]) => ({ path, pageCount, title: this.pageByPath.get(`${path}/_index`)?.title }));
    return { pages, dirs };
  }

  /** Short map of the documentation: top-level pages and directories with their page counts. */
  outline(expandAbove = OUTLINE_EXPAND_ABOVE): string {
    return this.outlineLines("", expandAbove).join("\n");
  }

  private outlineLines(dir: string, expandAbove: number): string[] {
    const { pages, dirs } = this.children(dir);
    const lines =
      pages.length <= OUTLINE_DIRECT_PAGES
        ? pages.map(page => `- ${page.path}: ${page.title}`)
        : [`- ${dir === "" ? "the top level" : `${dir}/`} (${pages.length} pages of its own)`];
    for (const sub of dirs) {
      if (sub.pageCount > expandAbove && this.children(sub.path).dirs.length > 0) {
        lines.push(...this.outlineLines(sub.path, expandAbove));
      } else {
        const count = sub.pageCount === 1 ? "1 page" : `${sub.pageCount} pages`;
        lines.push(`- ${sub.path}/ (${count})${sub.title ? `: ${sub.title}` : ""}`);
      }
    }
    return lines;
  }

  /** Pages whose path or title shares the most words with a path that names no page. */
  closest(input: string, limit = 5): DocsPage[] {
    const wanted = new Set(tokenize(input));
    if (wanted.size === 0) return [];
    const scored: { page: DocsPage; score: number }[] = [];
    for (const page of this.pageList) {
      const have = new Set(tokenize(`${page.path} ${page.title}`));
      let score = 0;
      for (const token of wanted) if (have.has(token)) score++;
      if (score > 0) scored.push({ page, score });
    }
    scored.sort(
      (a, b) => b.score - a.score || a.page.path.length - b.page.path.length || a.page.path.localeCompare(b.page.path)
    );
    return scored.slice(0, limit).map(entry => entry.page);
  }

  /** Scores every section by tf-idf over the query tokens and returns the best ones. */
  search(query: string, limit = 5): DocsSearchHit[] {
    const queryTerms = [...new Set(tokenize(query))];
    if (queryTerms.length === 0) return [];
    const total = this.sectionList.length;
    const scored: { index: number; score: number }[] = [];
    for (let i = 0; i < total; i++) {
      const tf = this.termFrequencies[i];
      let score = 0;
      for (const term of queryTerms) {
        const count = tf.get(term);
        if (!count) continue;
        const df = this.documentFrequency.get(term) ?? 1;
        const idf = Math.log(1 + total / df);
        score += (count / Math.sqrt(this.sectionLengths[i] || 1)) * idf;
      }
      if (score > 0) scored.push({ index: i, score });
    }
    scored.sort((a, b) => b.score - a.score);
    return scored.slice(0, limit).map(({ index, score }) => {
      const section = this.sectionList[index];
      const page = this.pageByPath.get(section.path)!;
      return {
        path: section.path,
        title: page.title,
        headingChain: section.headingChain,
        url: this.url(section.path),
        snippet: this.snippet(section.text),
        score: Math.round(score * 1000) / 1000,
      };
    });
  }

  /** Returns a section's text, or the section list plus the first section when no section is named. */
  read(path: string, section?: string): string | undefined {
    const sections = this.sectionsByPath.get(path);
    if (!sections) return undefined;
    if (section === undefined || section.trim() === "") {
      const names = this.sections(path).map(name => `- ${name}`);
      return `Sections:\n${names.join("\n")}\n\n${sections[0].text}`;
    }
    return DocsIndex.find(sections, section)?.text;
  }

  /** Sections whose name is the one given, those named exactly so first, then by loose comparison. */
  private static named(sections: StoredSection[], name: string): StoredSection[] {
    const byName = (same: (candidate: string) => boolean) => [
      ...sections.filter(s => same(DocsIndex.nameOf(s))),
      ...sections.filter(s => !same(DocsIndex.nameOf(s)) && same(s.ownName)),
    ];
    const exact = byName(candidate => candidate.toLowerCase() === name.toLowerCase());
    if (exact.length > 0) return exact;
    const key = sectionKey(name);
    return key === "" ? [] : byName(candidate => sectionKey(candidate) === key);
  }

  /**
   * The section a name or a "Page > Heading > Section" chain points at. The longest end of the
   * chain that names a section wins, and the part before it picks among sections of that name.
   */
  private static find(sections: StoredSection[], wanted: string): StoredSection | undefined {
    const parts = wanted.split(" > ").map(part => part.trim());
    for (let start = 0; start < parts.length; start++) {
      const name = parts.slice(start).join(" > ");
      // As given first, so a heading that itself starts with "#" is found; then without heading marks.
      const asGiven = DocsIndex.named(sections, name);
      const candidates = asGiven.length > 0 ? asGiven : DocsIndex.named(sections, name.replace(/^#+\s*/, ""));
      if (candidates.length === 0) continue;
      const parent = start > 0 ? parts[start - 1] : undefined;
      const underParent =
        parent === undefined
          ? undefined
          : candidates.find(
              c => c.headingChain.length > 1 && sameName(c.headingChain[c.headingChain.length - 2], parent)
            );
      return underParent ?? candidates[0];
    }
    return undefined;
  }

  /** The whole page: its introduction, then each section under its heading. */
  page(path: string): string | undefined {
    const sections = this.sectionsByPath.get(path);
    if (!sections) return undefined;
    return sections
      .map(s => {
        const depth = s.headingChain.length;
        if (depth === 1) return s.text;
        const heading = `${"#".repeat(Math.min(6, depth))} ${s.headingChain[depth - 1]}`;
        return s.container ? heading : `${heading}\n\n${s.text}`;
      })
      .join("\n\n");
  }

  private snippet(text: string): string {
    const flat = withoutLinkTargets(text).replace(/\s+/g, " ").trim();
    return flat.length <= SNIPPET_CHAR_LIMIT ? flat : `${cutText(flat, SNIPPET_CHAR_LIMIT)}…`;
  }
}

/** Reads every markdown page under dir into a DocsIndex, or returns null when dir is missing or has no page. */
export function loadDocsIndex(dir: string, options: LoadDocsIndexOptions = {}): DocsIndex | null {
  if (!existsSync(dir) || !statSync(dir).isDirectory()) return null;
  const excluded = new Set(options.exclude ?? DEFAULT_EXCLUDED_DIRS);
  const pages = listMarkdownFiles(dir, excluded).map(rel => {
    const raw = readFileSync(join(dir, rel), "utf8");
    const { title, description, body } = parseFrontMatter(stripAsfHeader(raw));
    const path = rel.slice(0, -".md".length);
    const cleaned = stripShortcodes(body);
    return {
      path,
      title: title ?? firstHeading(path, cleaned) ?? path,
      description: description ?? "",
      body: cleaned,
    };
  });
  return pages.length === 0 ? null : new DocsIndex(pages);
}

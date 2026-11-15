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

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, mkdirSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DOCS_SITE_BASE, DocsIndex, loadDocsIndex } from "./docs-index";
import {
  DOC_RESULT_CHAR_LIMIT,
  TOOL_NAME_READ_DOC,
  TOOL_NAME_SEARCH_DOCS,
  createReadDocTool,
  createSearchDocsTool,
  getSharedDocsIndex,
} from "./docs-tools";

const DATASETS_PAGE = `---
title: "Datasets"
description: "Upload and manage datasets"
---

A dataset is a versioned collection of files.

## Uploading files

Click the Upload button in the dataset page to add files.

## Sharing a dataset

Use the share dialog to grant read access to another user.
`;

const LONG_SECTION_LENGTH = "word ".repeat(2000).trim().length;

const LONG_PAGE = `---
title: "Long Reference"
description: "A page with a long section"
---

## Big section

${"word ".repeat(2000).trim()}
`;

const CSV_PAGE = `---
title: "CSVFileScan"
description: "Reads a CSV file into tuples"
---

## Properties

The delimiter property sets the column separator.
`;

const GETTING_STARTED_INDEX = `---
title: "Getting Started"
description: "Install and launch Texera"
---

Follow the [install guide](install/) first.
`;

const LOGIN_PAGE = `---
title: "Access/Login to Texera"
description: "Sign in"
---

## Sign in

Use the login form.
`;

const EXACT_BODY = "e".repeat(DOC_RESULT_CHAR_LIMIT);

const EXACT_PAGE = `---
title: "Exact"
description: "A section exactly at the clip limit"
---

## At limit

${EXACT_BODY}
`;

let docsDir: string;
let index: DocsIndex;

beforeAll(() => {
  docsDir = mkdtempSync(join(tmpdir(), "texera-docs-tools-"));
  mkdirSync(join(docsDir, "getting-started"), { recursive: true });
  mkdirSync(join(docsDir, "getting-started", "advanced"), { recursive: true });
  writeFileSync(join(docsDir, "getting-started", "_index.md"), GETTING_STARTED_INDEX);
  writeFileSync(join(docsDir, "getting-started", "AccessLogin.md"), LOGIN_PAGE);
  writeFileSync(join(docsDir, "getting-started", "advanced", "tuning.md"), CSV_PAGE);
  writeFileSync(join(docsDir, "getting-started", "datasets.md"), DATASETS_PAGE);
  writeFileSync(join(docsDir, "long.md"), LONG_PAGE);
  writeFileSync(join(docsDir, "csv-file-scan.md"), CSV_PAGE);
  writeFileSync(join(docsDir, "exact.md"), EXACT_PAGE);
  index = loadDocsIndex(docsDir)!;
});

afterAll(() => {
  rmSync(docsDir, { recursive: true, force: true });
});

async function run(toolDef: any, args: Record<string, unknown>): Promise<string> {
  return toolDef.execute(args, { toolCallId: "t1", messages: [] });
}

describe("tool names", () => {
  test("are the names the prompt refers to", () => {
    expect(TOOL_NAME_SEARCH_DOCS).toBe("searchDocs");
    expect(TOOL_NAME_READ_DOC).toBe("readDoc");
  });
});

describe("searchDocs", () => {
  test("lists each hit with heading chain, path, url and snippet", async () => {
    const out = await run(createSearchDocsTool(index), { query: "upload a file" });
    expect(out.startsWith("[ERROR]")).toBe(false);
    expect(out).toContain("Datasets > Uploading files");
    expect(out).toContain("path: getting-started/datasets");
    expect(out).toContain(`${DOCS_SITE_BASE}/getting-started/datasets/`);
    expect(out).toContain("Click the Upload button");
    expect(out).toContain("readDoc");
  });

  test("honours the limit argument", async () => {
    const out = await run(createSearchDocsTool(index), { query: "dataset", limit: 1 });
    expect(out.match(/path: /g)).toHaveLength(1);
  });

  test("falls back to the outline of the documentation when nothing matches", async () => {
    const out = await run(createSearchDocsTool(index), { query: "zzzz qqqq" });
    expect(out.startsWith("[ERROR]")).toBe(false);
    expect(out).toContain('No section matched "zzzz qqqq".');
    expect(out).toContain("- long: Long Reference");
    expect(out).toContain("- getting-started/ (4 pages): Getting Started");
    expect(out).not.toContain("getting-started/datasets");
    expect(out).toContain("readDoc");
  });

  test("falls back to the outline for an empty query", async () => {
    const out = await run(createSearchDocsTool(index), { query: "   " });
    expect(out).toContain('No section matched "".');
    expect(out).toContain("- getting-started/ (4 pages)");
  });

  test("falls back to the outline for a stopword-only query", async () => {
    const out = await run(createSearchDocsTool(index), { query: "how do I" });
    expect(out).toContain("No section matched");
    expect(out).not.toContain("path: ");
  });

  test("falls back to the outline when no section shares a term", async () => {
    const out = await run(createSearchDocsTool(index), { query: "banana smoothie" });
    expect(out).toContain("No section matched");
  });

  test("returns hits, not the outline, for a query with matching terms", async () => {
    const out = await run(createSearchDocsTool(index), { query: "csv scan" });
    expect(out).not.toContain("No section matched");
    expect(out).toContain("path: csv-file-scan");
  });

  describe("limit", () => {
    const many = new DocsIndex(
      Array.from({ length: 12 }, (_, i) => ({
        path: `p${i}`,
        title: `P${i}`,
        description: "",
        body: `needle number ${i}`,
      }))
    );
    const hits = async (args: Record<string, unknown>) =>
      ((await run(createSearchDocsTool(many), { query: "needle", ...args })).match(/path: /g) ?? []).length;

    test("returns five sections when no limit is given", async () => {
      expect(await hits({})).toBe(5);
    });

    test("clamps a limit outside 1 to 10 instead of failing", async () => {
      expect(await hits({ limit: 50 })).toBe(10);
      expect(await hits({ limit: 10 })).toBe(10);
      expect(await hits({ limit: 0 })).toBe(1);
      expect(await hits({ limit: -3 })).toBe(1);
    });

    test("says so when it changed the limit, and nothing when it did not", async () => {
      const note = (given: number, used: number) =>
        `(limit ${given} was changed to ${used}, it has to be a whole number from 1 to 10)`;
      const out = (limit?: number) =>
        run(createSearchDocsTool(many), { query: "needle", ...(limit === undefined ? {} : { limit }) });
      expect((await out(50)).endsWith(`\n${note(50, 10)}`)).toBe(true);
      expect((await out(0)).endsWith(`\n${note(0, 1)}`)).toBe(true);
      expect((await out(2.9)).endsWith(`\n${note(2.9, 2)}`)).toBe(true);
      for (const limit of [undefined, 1, 5, 10]) expect(await out(limit)).not.toContain("was changed to");
    });

    test("rounds a fractional limit down and falls back to five for a value that is not a number", async () => {
      expect(await hits({ limit: 2.9 })).toBe(2);
      expect(await hits({ limit: Number.NaN })).toBe(5);
    });
  });

  test("returns an error result instead of throwing when the index fails", async () => {
    const broken = {
      search: () => {
        throw new Error("boom");
      },
    } as unknown as DocsIndex;
    expect(await run(createSearchDocsTool(broken), { query: "x" })).toBe("[ERROR] boom");
  });

  test("echoes at most 80 characters of a query that matched nothing", async () => {
    const out = await run(createSearchDocsTool(index), { query: "z".repeat(5000) });
    expect(out).toContain(`No section matched "${"z".repeat(80)}…".`);
    expect(out.length).toBeLessThan(600);
  });

  test("keeps the fallback small for a documentation tree with hundreds of pages", async () => {
    const pages = [];
    for (const area of ["data-input", "data-cleaning", "visualization", "machine-learning", "search"]) {
      pages.push({ path: `reference/operators/${area}/_index`, title: area, description: "", body: "x" });
      for (let i = 0; i < 60; i++) {
        pages.push({
          path: `reference/operators/${area}/op-${i}`,
          title: `Operator ${i}`,
          description: "A long description",
          body: "x",
        });
      }
    }
    const big = new DocsIndex(pages);
    const miss = await run(createSearchDocsTool(big), { query: "zzzz" });
    const unknown = await run(createReadDocTool(big), { path: "reference/operators/nope" });
    expect(miss.length).toBeLessThan(2000);
    expect(unknown.length).toBeLessThan(2000);
    expect(miss).toContain("- reference/operators/visualization/ (61 pages): visualization");
  });
});

describe("readDoc", () => {
  test("returns a section with its page link", async () => {
    const out = await run(createReadDocTool(index), { path: "getting-started/datasets", section: "Sharing a dataset" });
    expect(out).toContain("Use the share dialog");
    expect(out).toContain(`${DOCS_SITE_BASE}/getting-started/datasets/`);
    expect(out).not.toContain("truncated");
  });

  test("returns the whole page without a section argument when it fits", async () => {
    const out = await run(createReadDocTool(index), { path: "getting-started/datasets" });
    expect(out).toBe(
      [
        `getting-started/datasets (${DOCS_SITE_BASE}/getting-started/datasets/)`,
        "",
        "A dataset is a versioned collection of files.",
        "",
        "## Uploading files",
        "",
        "Click the Upload button in the dataset page to add files.",
        "",
        "## Sharing a dataset",
        "",
        "Use the share dialog to grant read access to another user.",
      ].join("\n")
    );
  });

  test("returns the section list and the introduction when the page is over the limit", async () => {
    const out = await run(createReadDocTool(index), { path: "long" });
    expect(out).toBe(
      `long (${DOCS_SITE_BASE}/long/)\n\nSections:\n- Long Reference\n- Big section\n\nA page with a long section`
    );
  });

  test("returns a page of exactly the limit whole and one character more as its introduction", async () => {
    const page = (path: string, body: string) => ({ path, title: path, description: "", body });
    // "Intro." + blank line + "## S" + blank line + the body is 14 characters around the body.
    const edge = new DocsIndex([
      page("fits", `Intro.\n\n## S\n${"y".repeat(DOC_RESULT_CHAR_LIMIT - 14)}`),
      page("over", `Intro.\n\n## S\n${"y".repeat(DOC_RESULT_CHAR_LIMIT - 13)}`),
    ]);
    const fits = await run(createReadDocTool(edge), { path: "fits" });
    expect(fits.split("\n").slice(2).join("\n").length).toBe(DOC_RESULT_CHAR_LIMIT);
    expect(fits).toContain("## S");
    expect(fits).not.toContain("Sections:");
    const over = await run(createReadDocTool(edge), { path: "over" });
    expect(over.endsWith("Sections:\n- over\n- S\n\nIntro.")).toBe(true);
  });

  test("treats a null section, offset or limit as absent", async () => {
    const whole = await run(createReadDocTool(index), { path: "getting-started/datasets" });
    expect(await run(createReadDocTool(index), { path: "getting-started/datasets", section: null, offset: null })).toBe(
      whole
    );
    const section = await run(createReadDocTool(index), { path: "long", section: "Big section" });
    expect(await run(createReadDocTool(index), { path: "long", section: "Big section", offset: null })).toBe(section);
    const hits = await run(createSearchDocsTool(index), { query: "dataset" });
    expect(await run(createSearchDocsTool(index), { query: "dataset", limit: null })).toBe(hits);
  });

  test("treats an empty section name as no section", async () => {
    const whole = await run(createReadDocTool(index), { path: "getting-started/datasets" });
    expect(await run(createReadDocTool(index), { path: "getting-started/datasets", section: "" })).toBe(whole);
    expect(await run(createReadDocTool(index), { path: "getting-started/datasets", section: "  " })).toBe(whole);
  });

  test("finds a section by the heading chain that searchDocs prints", async () => {
    const out = await run(createReadDocTool(index), {
      path: "getting-started/datasets",
      section: "Datasets > Sharing a dataset",
    });
    expect(out).toContain("Use the share dialog");
  });

  test("accepts a path with a trailing .md or slash", async () => {
    const a = await run(createReadDocTool(index), { path: "getting-started/datasets.md" });
    const b = await run(createReadDocTool(index), { path: "/getting-started/datasets/" });
    expect(a).toContain("A dataset is a versioned collection of files.");
    expect(b).toContain("A dataset is a versioned collection of files.");
  });

  test("matches the page path whatever its case and keeps the canonical case in the link", async () => {
    const out = await run(createReadDocTool(index), { path: "Getting-Started/accesslogin", section: "sign in" });
    expect(out.startsWith(`getting-started/AccessLogin (${DOCS_SITE_BASE}/getting-started/AccessLogin/)`)).toBe(true);
    expect(out).toContain("Use the login form.");
  });

  test("accepts the published link of a page as its path", async () => {
    const out = await run(createReadDocTool(index), { path: `${DOCS_SITE_BASE}/getting-started/datasets/#uploading` });
    expect(out).toContain("A dataset is a versioned collection of files.");
  });

  test("reads a directory as its _index page followed by the pages under it", async () => {
    for (const path of ["getting-started", "getting-started/", "getting-started/_index"]) {
      const out = await run(createReadDocTool(index), { path });
      expect(out.startsWith(`getting-started/_index (${DOCS_SITE_BASE}/getting-started/)`)).toBe(true);
      expect(out).toContain(`Follow the [install guide](${DOCS_SITE_BASE}/getting-started/install/) first.`);
      expect(out).toContain(
        [
          "Pages under getting-started/:",
          "- getting-started/AccessLogin: Access/Login to Texera",
          "- getting-started/datasets: Datasets",
          "- getting-started/advanced/ (1 page)",
        ].join("\n")
      );
    }
  });

  test("lists a directory that has no _index page", async () => {
    const out = await run(createReadDocTool(index), { path: "getting-started/advanced" });
    expect(out.startsWith("[ERROR]")).toBe(false);
    expect(out).toBe("Pages under getting-started/advanced/:\n- getting-started/advanced/tuning: CSVFileScan");
  });

  test("lists the top level for an empty path when there is no root page", async () => {
    const out = await run(createReadDocTool(index), { path: "" });
    expect(out).toBe(
      [
        "Pages under the documentation root:",
        "- csv-file-scan: CSVFileScan",
        "- exact: Exact",
        "- long: Long Reference",
        "- getting-started/ (4 pages): Getting Started",
      ].join("\n")
    );
  });

  test("lists at most 50 entries of a directory and says how many are left", async () => {
    const pages = Array.from({ length: 61 }, (_, i) => ({
      path: `ops/op-${String(i).padStart(2, "0")}`,
      title: `Operator ${i}`,
      description: "",
      body: "x",
    }));
    const out = await run(createReadDocTool(new DocsIndex(pages)), { path: "ops" });
    const lines = out.split("\n");
    expect(lines).toHaveLength(52);
    expect(lines[50]).toBe("- ops/op-49: Operator 49");
    expect(lines[51]).toBe("- and 11 more, use searchDocs to find them");
  });

  test("does not append a page list when a section of an _index page is read", async () => {
    const out = await run(createReadDocTool(index), { path: "getting-started", section: "Getting Started" });
    expect(out).toContain("Follow the [install guide]");
    expect(out).not.toContain("Pages under");
  });

  test("clips a long section at the limit and says how to continue", async () => {
    const out = await run(createReadDocTool(index), { path: "long", section: "Big section" });
    const body = out
      .split("\n")
      .filter(l => l.startsWith("word"))
      .join("\n");
    expect(body.length).toBe(DOC_RESULT_CHAR_LIMIT);
    const left = LONG_SECTION_LENGTH - DOC_RESULT_CHAR_LIMIT;
    expect(
      out.endsWith(
        `[truncated, ${left} more characters. Call readDoc again with the same path and section and offset ${DOC_RESULT_CHAR_LIMIT} to continue.]`
      )
    ).toBe(true);
  });

  test("continues a clipped section from the offset and stops clipping at its end", async () => {
    const tool = createReadDocTool(index);
    let offset = 0;
    let text = "";
    for (let i = 0; i < 10; i++) {
      const out = await run(tool, { path: "long", section: "Big section", ...(offset > 0 ? { offset } : {}) });
      const [, , ...rest] = out.split("\n");
      const more = rest[rest.length - 1].match(/^\[truncated, \d+ more characters\. .* offset (\d+) to continue\.\]$/);
      text += (more ? rest.slice(0, -1) : rest).join("\n");
      if (!more) break;
      offset = Number(more[1]);
    }
    expect(text).toBe("word ".repeat(2000).trim());
  });

  test("cuts a long directory listing between two entries and continues with the next one", async () => {
    const entry = /^- area\/page-\d\d: Page \d+ t{90}$/;
    const area = new DocsIndex([
      { path: "area/_index", title: "Area", description: "", body: "Intro." },
      ...Array.from({ length: 50 }, (_, i) => ({
        path: `area/page-${String(i).padStart(2, "0")}`,
        title: `Page ${i} ${"t".repeat(90)}`,
        description: "",
        body: "x",
      })),
    ]);
    const tool = createReadDocTool(area);
    const entries: string[] = [];
    let offset = 0;
    let calls = 0;
    for (; calls < 10; calls++) {
      const out = await run(tool, { path: "area", ...(offset > 0 ? { offset } : {}) });
      const lines = out.split("\n").slice(2);
      const more = lines[lines.length - 1].match(
        /^\[truncated, \d+ more characters\. .* offset (\d+) to continue\.\]$/
      );
      const body = more ? lines.slice(0, -1) : lines;
      // A continuation starts with the next entry, not with the line break that was cut at.
      if (offset > 0) expect(body[0]).toMatch(entry);
      for (const line of body.filter(l => l.startsWith("- "))) {
        expect(line).toMatch(entry);
        entries.push(line);
      }
      if (!more) break;
      offset = Number(more[1]);
    }
    expect(calls).toBeGreaterThan(0);
    expect(entries).toHaveLength(50);
    expect(new Set(entries).size).toBe(50);
  });

  test("cuts mid-line when the last line break is far from the limit", async () => {
    const body = `first line\n${"w".repeat(DOC_RESULT_CHAR_LIMIT * 2)}`;
    const wide = new DocsIndex([{ path: "w", title: "W", description: "", body: `## S\n${body}` }]);
    const out = await run(createReadDocTool(wide), { path: "w", section: "S" });
    expect(out).toContain(`offset ${DOC_RESULT_CHAR_LIMIT} to continue.]`);
    const rest = await run(createReadDocTool(wide), { path: "w", section: "S", offset: DOC_RESULT_CHAR_LIMIT });
    const shown = (text: string) =>
      text
        .split("\n")
        .slice(2)
        .filter(l => !l.startsWith("[truncated"))
        .join("\n");
    expect((shown(out) + shown(rest)).startsWith(body.slice(0, DOC_RESULT_CHAR_LIMIT * 2))).toBe(true);
  });

  test("says which arguments to repeat when a page read without a section is clipped", async () => {
    const tall = new DocsIndex([{ path: "tall", title: "Tall", description: "", body: "intro ".repeat(2000) }]);
    const out = await run(createReadDocTool(tall), { path: "tall" });
    expect(out).toMatch(/Call readDoc again with the same path and offset 4000 to continue\.\]$/);
    const rest = await run(createReadDocTool(tall), { path: "tall", offset: 4000 });
    expect(rest).toMatch(/Call readDoc again with the same path and offset 8000 to continue\.\]$/);
  });

  test("rejects an offset at or past the end of the text", async () => {
    const out = await run(createReadDocTool(index), {
      path: "getting-started/datasets",
      section: "Sharing a dataset",
      offset: 5000,
    });
    expect(out.startsWith("[ERROR]")).toBe(true);
    expect(out).toContain("offset 5000 is past the end");
    const atEnd = await run(createReadDocTool(index), {
      path: "exact",
      section: "At limit",
      offset: DOC_RESULT_CHAR_LIMIT,
    });
    expect(atEnd.startsWith("[ERROR]")).toBe(true);
  });

  test("reads the tail of a section from an offset inside it", async () => {
    const out = await run(createReadDocTool(index), {
      path: "exact",
      section: "At limit",
      offset: DOC_RESULT_CHAR_LIMIT - 3,
    });
    expect(out.endsWith("\n\neee")).toBe(true);
    expect(out).not.toContain("truncated");
  });

  test("leaves a section exactly at the limit unclipped", async () => {
    const out = await run(createReadDocTool(index), { path: "exact", section: "At limit" });
    expect(out).toContain(EXACT_BODY);
    expect(out).not.toContain("truncated");
  });

  test("errors on an unknown page with the closest pages and the outline", async () => {
    const out = await run(createReadDocTool(index), { path: "getting-started/login" });
    expect(
      out.startsWith(
        '[ERROR] Unknown page "getting-started/login". Closest pages:\n- getting-started/AccessLogin: Access/Login to Texera'
      )
    ).toBe(true);
    expect(out).toContain("- getting-started/ (4 pages): Getting Started");
    expect(out).toContain("- long: Long Reference");
  });

  test("errors on an unknown page with only the outline when no page name is close", async () => {
    const out = await run(createReadDocTool(index), { path: "../../../../etc/passwd" });
    expect(out.startsWith('[ERROR] Unknown page "../../../../etc/passwd".')).toBe(true);
    expect(out).not.toContain("Closest pages");
    expect(out).toContain("- getting-started/ (4 pages)");
  });

  test("echoes at most 80 characters of an unknown path", async () => {
    const out = await run(createReadDocTool(index), { path: "q".repeat(3000) });
    expect(out).toContain(`Unknown page "${"q".repeat(80)}…".`);
    expect(out.length).toBeLessThan(600);
  });

  test("errors on an unknown section and lists the page's sections", async () => {
    const out = await run(createReadDocTool(index), { path: "getting-started/datasets", section: "Nope" });
    expect(out).toBe(
      '[ERROR] Unknown section "Nope" on page "getting-started/datasets". Available sections:\n- Datasets\n- Uploading files\n- Sharing a dataset'
    );
  });

  test("rejects an offset that is negative or not a whole number", async () => {
    for (const offset of [-5, -1, 1.5, Number.NaN]) {
      const out = await run(createReadDocTool(index), { path: "long", section: "Big section", offset });
      expect(out).toBe("[ERROR] offset must be a whole number of characters, 0 or more.");
    }
  });

  test("does not split an emoji at the clip limit and continues after it", async () => {
    const body = `${"x".repeat(DOC_RESULT_CHAR_LIMIT - 1)}📚tail`;
    const emoji = new DocsIndex([{ path: "e", title: "E", description: "", body: `## S\n${body}` }]);
    const first = await run(createReadDocTool(emoji), { path: "e", section: "S" });
    const [, , chunk, trailer] = first.split("\n");
    expect(chunk).toBe("x".repeat(DOC_RESULT_CHAR_LIMIT - 1));
    expect(trailer).toBe(
      `[truncated, 6 more characters. Call readDoc again with the same path and section and offset ${DOC_RESULT_CHAR_LIMIT - 1} to continue.]`
    );
    const rest = await run(createReadDocTool(emoji), { path: "e", section: "S", offset: DOC_RESULT_CHAR_LIMIT - 1 });
    expect(rest.endsWith("\n\n📚tail")).toBe(true);
    // An offset that points into the middle of the emoji starts at the emoji.
    const inside = await run(createReadDocTool(emoji), { path: "e", section: "S", offset: DOC_RESULT_CHAR_LIMIT });
    expect(inside.endsWith("\n\n📚tail")).toBe(true);
  });

  test("returns an error result instead of throwing when the index fails", async () => {
    const broken = {
      resolve: () => {
        throw new Error("boom");
      },
    } as unknown as DocsIndex;
    expect(await run(createReadDocTool(broken), { path: "x" })).toBe("[ERROR] boom");
    const plain = {
      resolve: () => {
        throw "plain failure";
      },
    } as unknown as DocsIndex;
    expect(await run(createReadDocTool(plain), { path: "x" })).toBe("[ERROR] plain failure");
  });

  test("says in its description what a call without a section returns", () => {
    const description = (createReadDocTool(index) as any).description as string;
    expect(description).toContain("returns the whole page when it is short");
    expect(description).toContain("offset");
  });
});

describe("input schemas", () => {
  const accepts = (toolDef: any, input: unknown): boolean => toolDef.inputSchema.safeParse(input).success;

  test("searchDocs accepts any number as the limit, so a bad one reaches the tool and is clamped", () => {
    const search = createSearchDocsTool(index);
    for (const limit of [1, 10, 50, 0, -1, 2.5]) expect(accepts(search, { query: "x", limit })).toBe(true);
    expect(accepts(search, { query: "x" })).toBe(true);
  });

  test("searchDocs still requires a query string and a numeric limit", () => {
    const search = createSearchDocsTool(index);
    expect(accepts(search, {})).toBe(false);
    expect(accepts(search, { query: 5 })).toBe(false);
    expect(accepts(search, { query: "x", limit: "5" })).toBe(false);
  });

  test("readDoc accepts any number as the offset, so a bad one comes back as tool output", () => {
    const read = createReadDocTool(index);
    for (const offset of [0, 4000, -1, 1.5]) expect(accepts(read, { path: "p", offset })).toBe(true);
    expect(accepts(read, { path: "p", section: "S" })).toBe(true);
  });

  test("both accept null for an optional argument, which some models send instead of leaving it out", () => {
    expect(accepts(createSearchDocsTool(index), { query: "x", limit: null })).toBe(true);
    expect(accepts(createReadDocTool(index), { path: "p", section: null, offset: null })).toBe(true);
    expect(accepts(createSearchDocsTool(index), { query: null })).toBe(false);
    expect(accepts(createReadDocTool(index), { path: null })).toBe(false);
  });

  test("readDoc still requires a path string and a numeric offset", () => {
    const read = createReadDocTool(index);
    expect(accepts(read, {})).toBe(false);
    expect(accepts(read, { path: "p", offset: "4000" })).toBe(false);
    expect(accepts(read, { path: "p", section: 3 })).toBe(false);
  });
});

describe("getSharedDocsIndex — logging and failures", () => {
  const fakeLog = () => {
    const calls: { level: string; fields: any; message: string }[] = [];
    const log = {
      info: (fields: any, message: string) => calls.push({ level: "info", fields, message }),
      warn: (fields: any, message: string) => calls.push({ level: "warn", fields, message }),
    };
    return { log: log as any, calls };
  };
  const scratch: string[] = [];
  const tempDir = (): string => {
    const dir = mkdtempSync(join(tmpdir(), "texera-docs-shared-"));
    scratch.push(dir);
    return dir;
  };

  afterAll(() => {
    for (const dir of scratch) rmSync(dir, { recursive: true, force: true });
  });

  test("logs the page count once when an index loads and reuses the index", () => {
    const dir = tempDir();
    writeFileSync(join(dir, "a.md"), "# A\ntext");
    writeFileSync(join(dir, "b.md"), "# B\ntext");
    const { log, calls } = fakeLog();
    const first = getSharedDocsIndex(dir, log);
    const second = getSharedDocsIndex(dir, log);
    expect(first).not.toBeNull();
    expect(second).toBe(first);
    expect(calls).toEqual([{ level: "info", fields: { dir, pages: 2 }, message: "docs index loaded" }]);
  });

  test("warns once and returns null for a missing directory", () => {
    const dir = join(tempDir(), "missing");
    const { log, calls } = fakeLog();
    expect(getSharedDocsIndex(dir, log)).toBeNull();
    expect(getSharedDocsIndex(dir, log)).toBeNull();
    expect(calls).toEqual([
      {
        level: "warn",
        fields: { dir },
        message: "docs directory not found, docs tools stay off until the service restarts",
      },
    ]);
  });

  test("warns and returns null for a directory without a markdown page", () => {
    const dir = tempDir();
    writeFileSync(join(dir, "notes.txt"), "not markdown");
    const { log, calls } = fakeLog();
    expect(getSharedDocsIndex(dir, log)).toBeNull();
    expect(calls.map(c => c.message)).toEqual([
      "no documentation pages in the docs directory, docs tools stay off until the service restarts",
    ]);
  });

  test("catches a load error, warns once with it and keeps returning null", () => {
    const dir = tempDir();
    writeFileSync(join(dir, "a.md"), "# A\ntext");
    symlinkSync(join(dir, "does-not-exist"), join(dir, "dangling.md"));
    const { log, calls } = fakeLog();
    expect(getSharedDocsIndex(dir, log)).toBeNull();
    expect(calls).toHaveLength(1);
    expect(calls[0].level).toBe("warn");
    expect(calls[0].message).toBe("failed to load the docs index, docs tools stay off until the service restarts");
    expect(calls[0].fields.dir).toBe(dir);
    expect(String(calls[0].fields.err)).toContain("ENOENT");

    // The outcome is cached: fixing the folder later does not bring the tools back in this process.
    rmSync(join(dir, "dangling.md"));
    expect(getSharedDocsIndex(dir, log)).toBeNull();
    expect(calls).toHaveLength(1);
  });
});

describe("getSharedDocsIndex", () => {
  test("returns the same index for the same directory and null for a missing one", () => {
    const a = getSharedDocsIndex(docsDir);
    const b = getSharedDocsIndex(docsDir);
    expect(a).not.toBeNull();
    expect(a).toBe(b);
    expect(getSharedDocsIndex(join(docsDir, "missing"))).toBeNull();
  });
});

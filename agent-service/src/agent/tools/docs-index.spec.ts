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
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DOCS_SITE_BASE, DocsIndex, cutText, loadDocsIndex, resolveDocLinks, tokenize } from "./docs-index";

const ASF_HEADER = `<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.
-->
`;

const ROOT_INDEX = `${ASF_HEADER}
---
title: Texera Documentation
description: Root of the docs
---

Welcome to Texera.
`;

const DATASETS_PAGE = `${ASF_HEADER}
---
title: "Datasets"
description: "Upload and manage datasets"
weight: 20
---

{{% pageinfo %}}
A dataset is a versioned collection of files.
{{% /pageinfo %}}

## Uploading files

Click the Upload button in the dataset page to add files. Files are grouped into a version.

{{< alert color="success" title="Tip" >}}
Drag and drop also works for uploading.
{{< /alert >}}

### Supported formats

CSV, JSON and Parquet files are supported.

## Sharing a dataset

Use the share dialog to grant read access to another user.
`;

const CSV_SCAN_PAGE = `${ASF_HEADER}
---
title: "CSVFileScan"
description: "Reads a CSV file into tuples"
---

## Overview

CSVFileScan reads a comma separated file from a dataset path.

## Properties

The delimiter property sets the column separator.
`;

const LONG_SECTION = "word ".repeat(2000).trim();

const LONG_PAGE = `${ASF_HEADER}
---
title: "Long Reference"
description: "A page with a long section"
---

## Big section

${LONG_SECTION}
`;

const FOLDED_PAGE = `---
title: "Texera UI Overview"
description: >
  Explore the User Dashboard and its components.
categories: [Texera]
---

## User Dashboard

The dashboard lists your workflows.
`;

const FENCED_PAGE = `${ASF_HEADER}
---
title: "Python UDF"
description: "User-defined function operator in Python"
---

## Template

\`\`\`python
# comment line inside the fence
class ProcessTupleOperator(UDFOperatorV2):
    ## not a heading
    def process_tuple(self, tuple_, port):
        yield tuple_
\`\`\`

## Notes

Edit the template in the editor.
`;

const NO_FRONT_MATTER_FENCED_PAGE = `\`\`\`
# fake heading inside a fence
\`\`\`

# Real title

Body text.
`;

const NO_FRONT_MATTER_PAGE = `${ASF_HEADER}
# Plain page

Content without front matter.
`;

const DEV_PAGE = `---
title: "Guide for developers"
description: "How to build Texera from source"
---

## Building

Run sbt compile to build the backend.
`;

let docsDir: string;
let index: DocsIndex;

beforeAll(() => {
  docsDir = mkdtempSync(join(tmpdir(), "texera-docs-index-"));
  mkdirSync(join(docsDir, "getting-started"), { recursive: true });
  mkdirSync(join(docsDir, "reference", "operators", "data-input"), { recursive: true });
  mkdirSync(join(docsDir, "contribution-guidelines"), { recursive: true });
  writeFileSync(join(docsDir, "_index.md"), ROOT_INDEX);
  writeFileSync(join(docsDir, "getting-started", "datasets.md"), DATASETS_PAGE);
  writeFileSync(join(docsDir, "reference", "operators", "data-input", "csv-file-scan.md"), CSV_SCAN_PAGE);
  writeFileSync(join(docsDir, "reference", "long.md"), LONG_PAGE);
  writeFileSync(join(docsDir, "reference", "plain.md"), NO_FRONT_MATTER_PAGE);
  writeFileSync(join(docsDir, "getting-started", "ui-overview.md"), FOLDED_PAGE);
  writeFileSync(join(docsDir, "reference", "udf.md"), FENCED_PAGE);
  writeFileSync(join(docsDir, "reference", "fenced-title.md"), NO_FRONT_MATTER_FENCED_PAGE);
  writeFileSync(join(docsDir, "contribution-guidelines", "guide-for-developers.md"), DEV_PAGE);
  writeFileSync(join(docsDir, "reference", "notes.txt"), "not markdown");
  index = loadDocsIndex(docsDir)!;
});

afterAll(() => {
  rmSync(docsDir, { recursive: true, force: true });
});

describe("tokenize", () => {
  test("splits camelCase, lowercases, drops one-char tokens, and keeps the unsplit word", () => {
    expect(tokenize("CSVFileScan")).toEqual(["csv", "file", "scan", "csvfilescan"]);
  });

  test("splits on punctuation and whitespace and drops stopwords", () => {
    expect(tokenize("Upload a file, then share-it!")).toEqual(["upload", "file", "then", "share"]);
  });

  test("returns nothing for a stopword-only query", () => {
    expect(tokenize("how do I")).toEqual([]);
    expect(tokenize("What is the")).toEqual([]);
  });

  test("keeps the whole word even when camelCase splitting leaves one part", () => {
    expect(tokenize("iOS")).toEqual(["os", "ios"]);
    expect(tokenize("upload")).toEqual(["upload"]);
  });

  test("returns an empty list for empty input", () => {
    expect(tokenize("")).toEqual([]);
  });

  test("stems plural and verb endings so related words share a token", () => {
    expect(tokenize("queries")).toEqual(["query"]);
    expect(tokenize("filtered")).toEqual(["filter"]);
    expect(tokenize("joining")).toEqual(["join"]);
    expect(tokenize("rows")).toEqual(["row"]);
  });

  test("leaves a word ending in ss and a word too short for its suffix alone", () => {
    expect(tokenize("access")).toEqual(["access"]);
    expect(tokenize("class")).toEqual(["class"]);
    expect(tokenize("ties")).toEqual(["tie"]);
    expect(tokenize("used")).toEqual(["used"]);
    expect(tokenize("bring")).toEqual(["bring"]);
    expect(tokenize("gas")).toEqual(["gas"]);
  });
});

describe("cutText", () => {
  test("cuts at the limit and leaves shorter text alone", () => {
    expect(cutText("abcdef", 4)).toBe("abcd");
    expect(cutText("abc", 4)).toBe("abc");
    expect(cutText("abcd", 4)).toBe("abcd");
  });

  test("steps back one unit instead of splitting an emoji", () => {
    expect(cutText("abc📚def", 4)).toBe("abc");
    expect(cutText("abc📚def", 5)).toBe("abc📚");
    expect(cutText("📚", 1)).toBe("");
  });
});

describe("loadDocsIndex", () => {
  test("returns null for a missing directory", () => {
    expect(loadDocsIndex(join(docsDir, "does-not-exist"))).toBeNull();
  });

  test("returns null for a path that is a file and for a folder without markdown pages", () => {
    expect(loadDocsIndex(join(docsDir, "reference", "notes.txt"))).toBeNull();
    const empty = mkdtempSync(join(tmpdir(), "texera-docs-empty-"));
    try {
      expect(loadDocsIndex(empty)).toBeNull();
      writeFileSync(join(empty, "readme.txt"), "not markdown");
      expect(loadDocsIndex(empty)).toBeNull();
    } finally {
      rmSync(empty, { recursive: true, force: true });
    }
  });

  test("indexes every markdown page except the excluded directories and non-markdown files", () => {
    const paths = index
      .pages()
      .map(p => p.path)
      .sort();
    expect(paths).toEqual([
      "_index",
      "getting-started/datasets",
      "getting-started/ui-overview",
      "reference/fenced-title",
      "reference/long",
      "reference/operators/data-input/csv-file-scan",
      "reference/plain",
      "reference/udf",
    ]);
  });

  test("does not treat # lines inside code fences as headings and keeps the code block whole", () => {
    expect(index.sections("reference/udf")).toEqual(["Python UDF", "Template", "Notes"]);
    const template = index.read("reference/udf", "Template")!;
    expect(template).toContain("# comment line inside the fence");
    expect(template).toContain("## not a heading");
    expect(template).toContain("yield tuple_");
    expect(template.startsWith("```python")).toBe(true);
    expect(template.endsWith("```")).toBe(true);
  });

  test("takes the title from the first real heading, skipping fenced ones", () => {
    const page = index.pages().find(p => p.path === "reference/fenced-title")!;
    expect(page.title).toBe("Real title");
  });

  test("includes an excluded directory when the exclude list is overridden", () => {
    const all = loadDocsIndex(docsDir, { exclude: [] })!;
    expect(all.pages().map(p => p.path)).toContain("contribution-guidelines/guide-for-developers");
  });

  test("reads title and description from front matter and strips the ASF header", () => {
    const page = index.pages().find(p => p.path === "getting-started/datasets")!;
    expect(page.title).toBe("Datasets");
    expect(page.description).toBe("Upload and manage datasets");
    expect(index.read(page.path, "Uploading files")).not.toContain("Licensed to the Apache");
    expect(index.read(page.path, "Uploading files")).not.toContain("---");
  });

  test("joins a folded multi-line description into one line", () => {
    const page = index.pages().find(p => p.path === "getting-started/ui-overview")!;
    expect(page.description).toBe("Explore the User Dashboard and its components.");
  });

  test("falls back to the first heading or path as the title without front matter", () => {
    const page = index.pages().find(p => p.path === "reference/plain")!;
    expect(page.title).toBe("Plain page");
    expect(page.description).toBe("");
  });

  test("keeps shortcode inner text and drops the shortcode wrappers", () => {
    const intro = index.read("getting-started/datasets", "Datasets");
    expect(intro).toContain("A dataset is a versioned collection of files.");
    expect(intro).not.toContain("pageinfo");
    const upload = index.read("getting-started/datasets", "Uploading files");
    expect(upload).toContain("Drag and drop also works for uploading.");
    expect(upload).not.toContain("alert");
  });

  test("splits a page into sections by headings with the pre-heading text titled by the page", () => {
    expect(index.sections("getting-started/datasets")).toEqual([
      "Datasets",
      "Uploading files",
      "Supported formats",
      "Sharing a dataset",
    ]);
  });
});

describe("DocsIndex.url", () => {
  test("maps _index.md to its directory and a page to its own directory", () => {
    expect(index.url("_index")).toBe(`${DOCS_SITE_BASE}/`);
    expect(index.url("getting-started/datasets")).toBe(`${DOCS_SITE_BASE}/getting-started/datasets/`);
    expect(index.url("tutorials/_index")).toBe(`${DOCS_SITE_BASE}/tutorials/`);
    expect(index.url("reference/operators/data-input/csv-file-scan")).toBe(
      `${DOCS_SITE_BASE}/reference/operators/data-input/csv-file-scan/`
    );
  });
});

describe("DocsIndex.search", () => {
  test("ranks the section whose heading matches the query first", () => {
    const hits = index.search("how do I upload a file to a dataset");
    expect(hits[0].path).toBe("getting-started/datasets");
    expect(hits[0].headingChain).toEqual(["Datasets", "Uploading files"]);
    expect(hits[0].url).toBe(`${DOCS_SITE_BASE}/getting-started/datasets/`);
    expect(hits[0].title).toBe("Datasets");
    expect(hits[0].snippet).toContain("Click the Upload button");
    expect(hits[0].score).toBeGreaterThan(0);
  });

  test("matches a camelCase operator name from its split words", () => {
    const hits = index.search("csv scan delimiter");
    expect(hits[0].path).toBe("reference/operators/data-input/csv-file-scan");
  });

  test("matches a camelCase operator name typed as one word", () => {
    const hits = index.search("CSVFileScan");
    expect(hits[0].path).toBe("reference/operators/data-input/csv-file-scan");
  });

  test("honours the limit and returns nothing for a query with no known terms", () => {
    expect(index.search("dataset", 1)).toHaveLength(1);
    expect(index.search("zzzz qqqq")).toEqual([]);
    expect(index.search("")).toEqual([]);
  });

  test("returns at most one hit per section and sorts by descending score", () => {
    const hits = index.search("dataset files");
    const keys = hits.map(h => `${h.path}#${h.headingChain.join("/")}`);
    expect(new Set(keys).size).toBe(keys.length);
    for (let i = 1; i < hits.length; i++) {
      expect(hits[i - 1].score).toBeGreaterThanOrEqual(hits[i].score);
    }
  });
});

describe("DocsIndex.read", () => {
  test("returns the named section's text", () => {
    expect(index.read("getting-started/datasets", "Sharing a dataset")).toBe(
      "Use the share dialog to grant read access to another user."
    );
  });

  test("matches the section name case-insensitively", () => {
    expect(index.read("getting-started/datasets", "sharing a dataset")).toContain("share dialog");
  });

  test("returns the section list and the first section when no section is given", () => {
    const text = index.read("getting-started/datasets")!;
    expect(
      text.startsWith("Sections:\n- Datasets\n- Uploading files\n- Supported formats\n- Sharing a dataset\n\n")
    ).toBe(true);
    expect(text).toContain("A dataset is a versioned collection of files.");
    expect(text).not.toContain("Click the Upload button");
  });

  test("returns undefined for an unknown page or section", () => {
    expect(index.read("nope/missing")).toBeUndefined();
    expect(index.read("getting-started/datasets", "Nope")).toBeUndefined();
  });

  test("returns a long section in full so the caller decides how to clip it", () => {
    expect(index.read("reference/long", "Big section")!.length).toBe(LONG_SECTION.length);
  });
});

describe("DocsIndex — headings", () => {
  const page = (path: string, title: string, body: string, description = "") => ({ path, title, description, body });
  const chains = (index: DocsIndex, path: string, query: string) =>
    index
      .search(query, 20)
      .filter(h => h.path === path)
      .map(h => h.headingChain.join(" > "));

  test("gives sibling headings of the same level the same parent", () => {
    const index = new DocsIndex([
      page(
        "ops/cast",
        "Type Casting",
        "### Input Properties\nmarker one\n\n### Output Ports\nmarker two\n\n### Notes\nmarker three"
      ),
    ]);
    expect(chains(index, "ops/cast", "marker").sort()).toEqual([
      "Type Casting > Input Properties",
      "Type Casting > Notes",
      "Type Casting > Output Ports",
    ]);
  });

  test("nests a deeper heading under the nearest shallower one and pops back out", () => {
    const body = [
      "## Install",
      "marker a",
      "### On Linux",
      "marker b",
      "### On macOS",
      "marker c",
      "## Run",
      "marker d",
      "# Appendix",
      "marker e",
      "### Deep under appendix",
      "marker f",
    ].join("\n");
    const index = new DocsIndex([page("guide", "Guide", body)]);
    expect(chains(index, "guide", "marker").sort()).toEqual(
      [
        "Guide > Install",
        "Guide > Install > On Linux",
        "Guide > Install > On macOS",
        "Guide > Run",
        "Guide > Appendix",
        "Guide > Appendix > Deep under appendix",
      ].sort()
    );
  });

  test("does not credit a heading's words to a later sibling", () => {
    const index = new DocsIndex([
      page("ops/cast", "Type Casting", "### Zebra Options\nfirst\n\n### Output Ports\nsecond"),
    ]);
    expect(index.search("zebra", 5).map(h => h.headingChain.join(" > "))).toEqual(["Type Casting > Zebra Options"]);
  });

  test("splits at heading levels one to three only", () => {
    const index = new DocsIndex([page("p", "P", "## Two\nalpha\n#### Four\nbeta\n##### Five\ngamma")]);
    expect(index.sections("p")).toEqual(["Two"]);
    expect(index.read("p", "Two")).toBe("alpha\n#### Four\nbeta\n##### Five\ngamma");
  });

  test("ranks a word in a heading above the same word twice in another section's text", () => {
    const index = new DocsIndex([
      page("p1", "One", "## Alpha\nw x y z"),
      page("p2", "Two", "## Other\nalpha alpha q r"),
    ]);
    expect(index.search("alpha", 2).map(h => h.path)).toEqual(["p1", "p2"]);
  });

  test("names a heading by its link text and without backslash escapes", () => {
    const index = new DocsIndex([
      page(
        "_index",
        "Texera Documentation",
        "Welcome.\n\n## 📚 [Getting Started](/docs/getting-started/)\nNew here.\n\n## 4\\. Accessing the Texera UI\nOpen the browser.\n\n## The `readDoc` tool\nReads."
      ),
    ]);
    expect(index.sections("_index")).toEqual([
      "Texera Documentation",
      "📚 Getting Started",
      "4. Accessing the Texera UI",
      "The `readDoc` tool",
    ]);
    const hit = index.search("new here", 1)[0];
    expect(hit.headingChain).toEqual(["Texera Documentation", "📚 Getting Started"]);
  });

  test("finds a section by its name whatever the case, punctuation or emoji", () => {
    const index = new DocsIndex([
      page(
        "_index",
        "Docs",
        "Welcome.\n\n## 📚 [Getting Started](/docs/getting-started/)\nNew here.\n\n## 4\\. Accessing the Texera UI\nOpen the browser."
      ),
    ]);
    for (const name of [
      "📚 Getting Started",
      "Getting Started",
      "getting started",
      " GETTING-STARTED ",
      "Docs > 📚 Getting Started",
    ]) {
      expect(index.read("_index", name)).toBe("New here.");
    }
    for (const name of ["4. Accessing the Texera UI", "4\\. Accessing the Texera UI", "4 accessing the texera ui"]) {
      expect(index.read("_index", name)).toBe("Open the browser.");
    }
    expect(index.read("_index", "Getting")).toBeUndefined();
    expect(index.read("_index", "xxGetting Started")).toBeUndefined();
    expect(index.read("_index", "Getting Started > Nope")).toBeUndefined();
  });

  test("treats an empty section name as no section", () => {
    const index = new DocsIndex([page("p", "P", "Intro.\n\n## A\nBody.")]);
    expect(index.read("p", "")).toBe(index.read("p"));
    expect(index.read("p", "   ")).toBe(index.read("p"));
  });

  test("numbers later sections that share a name so each can be read", () => {
    const index = new DocsIndex([
      page(
        "ops/join",
        "Hash Join",
        "Joins two inputs.\n\n## Hash Join\nThe operator.\n\n## Example\nFirst.\n\n## Example\nSecond.\n\n## example\nThird."
      ),
    ]);
    expect(index.sections("ops/join")).toEqual(["Hash Join", "Hash Join (2)", "Example", "Example (2)", "example (3)"]);
    expect(index.read("ops/join", "Hash Join")).toBe("Joins two inputs.");
    expect(index.read("ops/join", "Hash Join (2)")).toBe("The operator.");
    expect(index.read("ops/join", "Example")).toBe("First.");
    expect(index.read("ops/join", "Example (2)")).toBe("Second.");
    expect(index.read("ops/join", "example (3)")).toBe("Third.");
    expect(index.search("second", 1)[0].headingChain).toEqual(["Hash Join", "Example (2)"]);
  });
});

describe("DocsIndex — text shown to the reader", () => {
  const page = (path: string, title: string, body: string, description = "") => ({ path, title, description, body });

  test("removes HTML comments and img tags outside code fences", () => {
    const body = [
      "<!-- Generated: 2026-04-25 -->",
      "Intro <!-- inline --> text.",
      "<!--",
      "multi-line",
      "-->",
      '<img width="1600" src="/images/a.png" />',
      'Before <img src="/images/b.png"> after.',
      "```html",
      "<!-- kept -->",
      '<img src="kept.png">',
      "```",
    ].join("\n");
    const text = new DocsIndex([page("p", "P", body)]).read("p", "P")!;
    expect(text).toBe(
      ["Intro  text.", "Before  after.", "```html", "<!-- kept -->", '<img src="kept.png">', "```"].join("\n")
    );
  });

  test("introduces a page whose introduction is only generator comments by its description", () => {
    const index = new DocsIndex([
      page(
        "ops/_index",
        "Operators",
        "<!-- Generated: 2026 -->\n<!-- Total Operators: 162 -->\n\n## Quick Links\nLinks.",
        "All operators"
      ),
    ]);
    expect(index.read("ops/_index", "Operators")).toBe("All operators");
  });

  test("cuts a snippet at 200 characters with an ellipsis and leaves a short one whole", () => {
    const index = new DocsIndex([
      page("long", "Long", "needle " + "x".repeat(400)),
      page("short", "Short", "needle only"),
    ]);
    const snippets = Object.fromEntries(index.search("needle", 5).map(h => [h.path, h.snippet]));
    expect(snippets.long).toBe(`needle ${"x".repeat(193)}…`);
    expect(snippets.short).toBe("needle only");
  });

  test("does not split an emoji at the end of a snippet", () => {
    const index = new DocsIndex([page("e", "E", `needle ${"x".repeat(192)}📚 tail`)]);
    expect(index.search("needle", 1)[0].snippet).toBe(`needle ${"x".repeat(192)}…`);
  });

  test("returns a whole page as its introduction followed by each section under its heading", () => {
    const index = new DocsIndex([
      page("p", "Page", "Intro.\n\n## First\nOne.\n\n### Nested\nTwo.\n\n## Second\nThree."),
    ]);
    expect(index.page("p")).toBe("Intro.\n\n## First\n\nOne.\n\n### Nested\n\nTwo.\n\n## Second\n\nThree.");
    expect(index.page("nope")).toBeUndefined();
  });

  test("starts a whole page with a heading when the page has no introduction", () => {
    const index = new DocsIndex([page("p", "Page", "## Only\nBody.")]);
    expect(index.page("p")).toBe("## Only\n\nBody.");
  });
});

describe("DocsIndex — path lookup details", () => {
  const page = (path: string, title: string) => ({ path, title, description: "", body: "x" });
  const index = new DocsIndex([
    page("overview", "Overview"),
    page("getting-started/install", "Install"),
    page("Guide", "Upper"),
    page("guide", "Lower"),
  ]);

  test("ignores a query string, a ./ prefix and doubled slashes", () => {
    expect(index.resolve("overview?x=1")).toBe("overview");
    expect(index.resolve("./overview")).toBe("overview");
    expect(index.resolve("getting-started//install")).toBe("getting-started/install");
    expect(index.resolve("./getting-started/./install/")).toBe("getting-started/install");
  });

  test("accepts the published link whatever the case of the site prefix", () => {
    expect(index.resolve("HTTPS://TEXERA.APACHE.ORG/docs/latest/overview/")).toBe("overview");
  });

  test("prefers the page whose case matches when two paths differ only by case", () => {
    expect(index.resolve("Guide")).toBe("Guide");
    expect(index.resolve("guide")).toBe("guide");
    expect(index.resolve("GUIDE")).toBe("Guide");
  });
});

describe("DocsIndex.outline — default threshold", () => {
  const filler = (dir: string, count: number) =>
    Array.from({ length: count }, (_, i) => ({
      path: `${dir}/page-${i + 1}`,
      title: `Page ${i + 1}`,
      description: "",
      body: "x",
    }));

  test("expands a directory of 61 pages and keeps one of 60 whole", () => {
    const index = new DocsIndex([
      ...filler("big/a", 31),
      ...filler("big/b", 30),
      ...filler("edge/a", 30),
      ...filler("edge/b", 30),
    ]);
    expect(index.outline().split("\n")).toEqual(["- big/a/ (31 pages)", "- big/b/ (30 pages)", "- edge/ (60 pages)"]);
  });
});

describe("DocsIndex — what is indexed", () => {
  const page = (path: string, title: string, body: string, description = "") => ({ path, title, description, body });

  test("does not index words that appear only in an HTML comment or an img tag", () => {
    const index = new DocsIndex([
      page(
        "p",
        "P",
        '<!-- Generated by tool -->\nIntro text.\n<img alt="Screenshot of the canvas" width="1600" src="/images/shot.png">\n\n## S\nBody.'
      ),
    ]);
    expect(index.search("screenshot canvas", 5)).toEqual([]);
    expect(index.search("generated", 5)).toEqual([]);
    expect(index.search("1600 images", 5)).toEqual([]);
    expect(index.search("intro", 5).map(h => h.path)).toEqual(["p"]);
  });

  test("keeps tag-like text inside an inline code span, for reading and for search", () => {
    const index = new DocsIndex([
      page("p", "P", 'Write `<img src="zebra.png">` or `<!-- quagga -->` in markdown.\n<!-- real comment -->'),
    ]);
    expect(index.read("p", "P")).toBe('Write `<img src="zebra.png">` or `<!-- quagga -->` in markdown.');
    expect(index.search("zebra", 5).map(h => h.path)).toEqual(["p"]);
    expect(index.search("quagga", 5).map(h => h.path)).toEqual(["p"]);
    expect(index.search("real comment", 5)).toEqual([]);
  });
});

describe("DocsIndex — finding a section", () => {
  const page = (path: string, title: string, body: string, description = "") => ({ path, title, description, body });

  test("uses the parent in a heading chain to tell two sections of the same name apart", () => {
    const index = new DocsIndex([
      page("p", "P", "## Install\ni\n### Linux\ninstall on linux\n## Uninstall\nu\n### Linux\nremove on linux"),
    ]);
    expect(index.sections("p")).toEqual(["Install", "Linux", "Uninstall", "Linux (2)"]);
    expect(index.read("p", "Linux")).toBe("install on linux");
    expect(index.read("p", "Linux (2)")).toBe("remove on linux");
    expect(index.read("p", "Install > Linux")).toBe("install on linux");
    expect(index.read("p", "Uninstall > Linux")).toBe("remove on linux");
    expect(index.read("p", "P > Uninstall > Linux")).toBe("remove on linux");
    expect(index.read("p", "uninstall > linux")).toBe("remove on linux");
    expect(index.read("p", "Uninstall! > Linux")).toBe("remove on linux");
  });

  test("falls back to the first section of that name when the parent given matches none", () => {
    const index = new DocsIndex([page("p", "P", "## Install\ni\n### Linux\ninstall on linux")]);
    expect(index.read("p", "Elsewhere > Linux")).toBe("install on linux");
  });

  test("prefers a section named like the longest end of the chain", () => {
    const index = new DocsIndex([page("p", "Page", "## Export\nplain export\n\n## File > Export\nfile menu export")]);
    expect(index.read("p", "Page > File > Export")).toBe("file menu export");
    expect(index.read("p", "File > Export")).toBe("file menu export");
    expect(index.read("p", "Export")).toBe("plain export");
    expect(index.read("p", "Page > Export")).toBe("plain export");
  });

  test("does not take one name for another that differs only in + or #", () => {
    const sharp = new DocsIndex([page("p", "P", "## C#\nsharp")]);
    expect(sharp.read("p", "C++")).toBeUndefined();
    expect(sharp.read("p", "c#")).toBe("sharp");
    const both = new DocsIndex([page("p", "P", "## C#\nsharp\n\n## C++\nplus")]);
    expect(both.sections("p")).toEqual(["C#", "C++"]);
    expect(both.read("p", "C++")).toBe("plus");
    expect(both.read("p", "C#")).toBe("sharp");
  });

  test("matches a name of only symbols exactly or not at all", () => {
    const index = new DocsIndex([page("p", "P", "## 📚\nbooks\n\n## ***\nstars")]);
    expect(index.sections("p")).toEqual(["📚", "***"]);
    expect(index.read("p", "📚")).toBe("books");
    expect(index.read("p", "!!!")).toBeUndefined();
    expect(index.read("p", "🎓")).toBeUndefined();
  });

  test("prefers an exact name over one that only matches without punctuation", () => {
    const index = new DocsIndex([page("p", "P", "## Set-up\nwith a hyphen\n\n## Set up\nwith a space")]);
    expect(index.read("p", "Set up")).toBe("with a space");
    expect(index.read("p", "set-up")).toBe("with a hyphen");
    expect(index.read("p", "SET_UP")).toBe("with a hyphen");
  });

  test("drops a closing run of # from a heading only when a space comes before it", () => {
    const index = new DocsIndex([page("p", "P", "## Closed ##\none\n\n## F#\ntwo")]);
    expect(index.sections("p")).toEqual(["Closed", "F#"]);
  });

  test("accepts a name written with its markdown heading marks", () => {
    const index = new DocsIndex([page("p", "P", "## Getting Started\nbegin")]);
    expect(index.read("p", "## Getting Started")).toBe("begin");
  });

  test("reads a heading whose own name starts with a hash mark", () => {
    const index = new DocsIndex([page("p", "P", "## # of workers\ncount\n\n## Getting Started\nbegin")]);
    expect(index.read("p", "# of workers")).toBe("count");
    expect(index.read("p", "## # of workers")).toBe("count");
    expect(index.read("p", "## Getting Started")).toBe("begin");
  });
});

describe("DocsIndex — a heading with no text of its own", () => {
  const page = (path: string, title: string, body: string, description = "") => ({ path, title, description, body });
  const body = [
    "Intro.",
    "## 5. Troubleshooting",
    "### File Upload Error",
    "forward the port",
    "### Port in use",
    "stop the other process",
    "## Next steps",
    "read more",
  ].join("\n");
  const index = new DocsIndex([page("k8s", "Run on Kubernetes", body)]);

  test("is listed among the sections", () => {
    expect(index.sections("k8s")).toEqual([
      "Run on Kubernetes",
      "5. Troubleshooting",
      "File Upload Error",
      "Port in use",
      "Next steps",
    ]);
  });

  test("reads as the list of the sections directly under it", () => {
    expect(index.read("k8s", "5. Troubleshooting")).toBe("Sub-sections:\n- File Upload Error\n- Port in use");
    expect(index.read("k8s", "Run on Kubernetes > 5. Troubleshooting")).toBe(
      "Sub-sections:\n- File Upload Error\n- Port in use"
    );
  });

  test("is found by search and shown as a heading line in the whole page", () => {
    expect(index.search("troubleshooting", 1)[0].headingChain).toEqual(["Run on Kubernetes", "5. Troubleshooting"]);
    expect(index.page("k8s")).toBe(
      [
        "Intro.",
        "## 5. Troubleshooting",
        "### File Upload Error\n\nforward the port",
        "### Port in use\n\nstop the other process",
        "## Next steps\n\nread more",
      ].join("\n\n")
    );
  });

  test("lists only its direct sub-sections, and those that are headings without text too", () => {
    const nested = new DocsIndex([
      page("n", "N", ["## Outer", "### Middle", "#### not a split", "### Empty middle", "## After", "text"].join("\n")),
    ]);
    expect(nested.sections("n")).toEqual(["Outer", "Middle", "After"]);
    expect(nested.read("n", "Outer")).toBe("Sub-sections:\n- Middle");
    expect(nested.read("n", "Middle")).toBe("#### not a split");
  });

  test("is dropped when nothing under it has text either", () => {
    const empty = new DocsIndex([page("e", "E", ["## Hollow", "### Also hollow", "## Real", "text"].join("\n"))]);
    expect(empty.sections("e")).toEqual(["Real"]);
    expect(empty.search("hollow", 5)).toEqual([]);
  });

  test("keeps a chain of headings without text down to the one that has some", () => {
    const deep = new DocsIndex([page("d", "D", ["# Top", "## Mid", "### Leaf", "leaf text"].join("\n"))]);
    expect(deep.sections("d")).toEqual(["Top", "Mid", "Leaf"]);
    expect(deep.read("d", "Top")).toBe("Sub-sections:\n- Mid");
    expect(deep.read("d", "Mid")).toBe("Sub-sections:\n- Leaf");
    expect(deep.page("d")).toBe("## Top\n\n### Mid\n\n#### Leaf\n\nleaf text");
  });
});

describe("resolveDocLinks", () => {
  const OPERATOR_PAGE = "reference/operators/visualization/nested-table";

  test("maps a site-relative /docs link onto the published base", () => {
    expect(resolveDocLinks("[Guide](/docs/getting-started/run-on-kubernetes/)", "security/_index")).toBe(
      `[Guide](${DOCS_SITE_BASE}/getting-started/run-on-kubernetes/)`
    );
  });

  test("keeps a /docs/latest link and other site-absolute paths on the site origin", () => {
    expect(resolveDocLinks("[a](/docs/latest/tutorials/)", "overview")).toBe(
      "[a](https://texera.apache.org/docs/latest/tutorials/)"
    );
    expect(resolveDocLinks("![shot](/images/github-assets/a.png)", "overview")).toBe(
      "![shot](https://texera.apache.org/images/github-assets/a.png)"
    );
  });

  test("resolves a page-relative link against the page's published URL", () => {
    expect(resolveDocLinks("[Home](../)", "reference/operators/output-modes")).toBe(
      `[Home](${DOCS_SITE_BASE}/reference/operators/)`
    );
    expect(resolveDocLinks("[modes](../../output-modes/#single-snapshot)", OPERATOR_PAGE)).toBe(
      `[modes](${DOCS_SITE_BASE}/reference/operators/output-modes/#single-snapshot)`
    );
  });

  test("resolves a bare relative link on an _index page against its directory", () => {
    expect(resolveDocLinks("[Data Input](data-input/)", "reference/operators/_index")).toBe(
      `[Data Input](${DOCS_SITE_BASE}/reference/operators/data-input/)`
    );
    expect(resolveDocLinks("[Start](getting-started/)", "_index")).toBe(`[Start](${DOCS_SITE_BASE}/getting-started/)`);
  });

  test("resolves an anchor-only link against the page itself", () => {
    expect(resolveDocLinks("[roles](#user-roles)", "security/_index")).toBe(
      `[roles](${DOCS_SITE_BASE}/security/#user-roles)`
    );
  });

  test("resolves a link to a markdown source file relative to the file's directory", () => {
    expect(resolveDocLinks("[udf](guide-to-use-python-udf.md)", "tutorials/first-workflow")).toBe(
      `[udf](${DOCS_SITE_BASE}/tutorials/guide-to-use-python-udf/)`
    );
    expect(resolveDocLinks("[sec](../security/_index.md#roles)", "tutorials/first-workflow")).toBe(
      `[sec](${DOCS_SITE_BASE}/security/#roles)`
    );
  });

  test("leaves external links and other schemes alone", () => {
    const text =
      "[a](https://example.com/x) [b](http://localhost:8080) [c](mailto:dev@texera.apache.org) [d](//cdn.example.com/lib.js)";
    expect(resolveDocLinks(text, OPERATOR_PAGE)).toBe(text);
  });

  test("maps /docs/latest without a trailing slash onto the site origin", () => {
    expect(resolveDocLinks("[root](/docs/latest)", "overview")).toBe("[root](https://texera.apache.org/docs/latest)");
  });

  test("resolves a target written in angle brackets", () => {
    expect(resolveDocLinks("[up](<../>)", "tutorials/first-workflow")).toBe(`[up](<${DOCS_SITE_BASE}/tutorials/>)`);
  });

  test("keeps a link title and resolves every link on a line", () => {
    expect(resolveDocLinks('[a](../ "Up") and [b](#x)', "tutorials/first-workflow")).toBe(
      `[a](${DOCS_SITE_BASE}/tutorials/ "Up") and [b](${DOCS_SITE_BASE}/tutorials/first-workflow/#x)`
    );
  });

  test("does not touch links inside a fenced code block", () => {
    const text = ["before [a](../)", "```md", "[b](../)", "```", "~~~", "[c](../)", "~~~", "after [d](../)"].join("\n");
    const out = resolveDocLinks(text, "tutorials/first-workflow").split("\n");
    expect(out[0]).toBe(`before [a](${DOCS_SITE_BASE}/tutorials/)`);
    expect(out[2]).toBe("[b](../)");
    expect(out[5]).toBe("[c](../)");
    expect(out[7]).toBe(`after [d](${DOCS_SITE_BASE}/tutorials/)`);
  });

  test("returns text without links unchanged", () => {
    expect(resolveDocLinks("plain text (with parens) and [brackets]", "overview")).toBe(
      "plain text (with parens) and [brackets]"
    );
  });
});

describe("DocsIndex — links, paths and outline", () => {
  const page = (path: string, title: string, body: string, description = "") => ({ path, title, description, body });
  const filler = (dir: string, count: number) =>
    Array.from({ length: count }, (_, i) => page(`${dir}/page-${i + 1}`, `Page ${i + 1}`, "Text."));

  const tree = new DocsIndex([
    page("_index", "Texera Documentation", "Start with [Getting Started](/docs/getting-started/)."),
    page("overview", "Overview", "What Texera is."),
    page("getting-started/_index", "Getting Started", "Install and launch Texera."),
    page("getting-started/AccessLogin", "Access/Login to Texera", "## Sign in\nUse the login form."),
    page("getting-started/install", "Install Texera", "Run the installer."),
    page("reference/operators/_index", "Operators", "All operators, see [Data Input](data-input/)."),
    page(
      "reference/operators/output-modes",
      "Output Port Modes",
      "[← Home](../)\n\nPorts advertise a mode. See [files](files/data.csv).\n\n## Set Snapshot\nRe-emits."
    ),
    page("reference/operators/data-input/_index", "Data Input", "Operators that read data."),
    page(
      "reference/operators/data-input/csv-file-scan",
      "CSV File Scan",
      [
        "[Home](../../) > [Data Input](../)",
        "",
        "Reads a CSV file. See [output modes](../../output-modes/#set-snapshot) and the [zebra guide](../zebra-crossing/).",
        "",
        "```python",
        "[Home](../../) > [Data Input](../)",
        "```",
      ].join("\n")
    ),
    ...filler("reference/operators/visualization", 3),
    page("tutorials/guide", "Guide", "A [link](other.md)."),
    page(
      "tutorials/download",
      "Download",
      "[Home](../) > [Tutorials](../) > Download\n\n[Sample data](files/data.csv)"
    ),
  ]);

  test("stores section text with links resolved to published URLs", () => {
    expect(tree.read("_index", "Texera Documentation")).toBe(
      `Start with [Getting Started](${DOCS_SITE_BASE}/getting-started/).`
    );
    expect(tree.read("reference/operators/_index", "Operators")).toContain(
      `[Data Input](${DOCS_SITE_BASE}/reference/operators/data-input/)`
    );
    expect(tree.read("tutorials/guide", "Guide")).toBe(`A [link](${DOCS_SITE_BASE}/tutorials/other/).`);
  });

  test("drops a breadcrumb line but keeps the same text inside a code fence", () => {
    const text = tree.read("reference/operators/data-input/csv-file-scan", "CSV File Scan")!;
    expect(text.startsWith("Reads a CSV file.")).toBe(true);
    expect(text.match(/\[Home\]/g)).toHaveLength(1);
    expect(text).toContain("```python\n[Home](../../) > [Data Input](../)\n```");
  });

  test("drops a lone up link and a breadcrumb that ends in the page name", () => {
    expect(tree.read("reference/operators/output-modes", "Output Port Modes")).toBe(
      `Ports advertise a mode. See [files](${DOCS_SITE_BASE}/reference/operators/output-modes/files/data.csv).`
    );
    expect(tree.read("tutorials/download", "Download")).toBe(
      `[Sample data](${DOCS_SITE_BASE}/tutorials/download/files/data.csv)`
    );
  });

  test("keeps a line whose only link does not point up the tree", () => {
    expect(tree.read("tutorials/download", "Download")).toContain("[Sample data](");
    expect(tree.read("_index", "Texera Documentation")).toContain("[Getting Started](");
  });

  test("shows link text without the URL in a search snippet", () => {
    const hit = tree.search("csv file scan", 1)[0];
    expect(hit.path).toBe("reference/operators/data-input/csv-file-scan");
    expect(hit.snippet).toContain("See output modes and the zebra guide.");
    expect(hit.snippet).not.toContain("https://");
  });

  test("indexes the source text, so the site prefix that link resolution adds is not searchable", () => {
    expect(tree.search("zebra crossing", 5).map(h => h.path)).toEqual(["reference/operators/data-input/csv-file-scan"]);
    expect(tree.search("latest", 5)).toEqual([]);
    expect(tree.search("https apache", 5)).toEqual([]);
  });

  test("still finds a page by a word of the breadcrumb that is no longer shown", () => {
    const hits = tree.search("home", 5).map(h => h.path);
    expect(hits).toContain("reference/operators/data-input/csv-file-scan");
    expect(tree.read("reference/operators/data-input/csv-file-scan", "CSV File Scan")!.startsWith("Reads")).toBe(true);
  });

  describe("a page whose introduction is empty once the breadcrumb is gone", () => {
    const operators = new DocsIndex([
      page(
        "ops/distinct",
        "Distinct",
        "[Home](../../) > [Data Cleaning](../)\n\n### Input Properties\nNo properties.",
        "Remove duplicate rows"
      ),
      page("ops/limit", "Limit", "### Input Properties\nlimit: how many rows to keep.", "Keep the first N rows"),
      page("ops/bare", "Bare", "### Input Properties\nNothing here."),
      page("ops/empty", "Empty", "", "Only a description"),
    ]);

    test("uses the description as the introduction, so the page is found by it", () => {
      expect(operators.search("remove duplicate rows", 1)[0].path).toBe("ops/distinct");
      expect(operators.search("keep first rows", 1)[0].path).toBe("ops/limit");
      expect(operators.sections("ops/distinct")).toEqual(["Distinct", "Input Properties"]);
      expect(operators.read("ops/distinct")).toBe("Sections:\n- Distinct\n- Input Properties\n\nRemove duplicate rows");
      expect(operators.read("ops/empty")).toBe("Sections:\n- Empty\n\nOnly a description");
    });

    test("keeps the breadcrumb's words searchable for a page whose introduction was only the breadcrumb", () => {
      expect(operators.search("data cleaning", 5).map(h => h.path)).toEqual(["ops/distinct"]);
    });

    test("adds no introduction to a page that has neither text nor description before its first heading", () => {
      expect(operators.sections("ops/bare")).toEqual(["Input Properties"]);
    });
  });

  describe("resolve", () => {
    test("returns the canonical path for an exact, slashed, .md or differently cased path", () => {
      expect(tree.resolve("getting-started/install")).toBe("getting-started/install");
      expect(tree.resolve("/getting-started/install/")).toBe("getting-started/install");
      expect(tree.resolve("getting-started/install.md")).toBe("getting-started/install");
      expect(tree.resolve("getting-started/accesslogin")).toBe("getting-started/AccessLogin");
      expect(tree.resolve("GETTING-STARTED/ACCESSLOGIN.MD")).toBe("getting-started/AccessLogin");
    });

    test("resolves a directory to its _index page and the empty path to the root page", () => {
      expect(tree.resolve("getting-started")).toBe("getting-started/_index");
      expect(tree.resolve("getting-started/")).toBe("getting-started/_index");
      expect(tree.resolve("Reference/Operators")).toBe("reference/operators/_index");
      expect(tree.resolve("")).toBe("_index");
      expect(tree.resolve("/")).toBe("_index");
    });

    test("accepts a published URL or a /docs path, with or without an anchor", () => {
      expect(tree.resolve(`${DOCS_SITE_BASE}/getting-started/AccessLogin/`)).toBe("getting-started/AccessLogin");
      expect(tree.resolve(`${DOCS_SITE_BASE}/reference/operators/output-modes/#set-snapshot`)).toBe(
        "reference/operators/output-modes"
      );
      expect(tree.resolve("/docs/getting-started/")).toBe("getting-started/_index");
      expect(tree.resolve("/docs/latest/overview/")).toBe("overview");
      expect(tree.resolve(`${DOCS_SITE_BASE}/`)).toBe("_index");
    });

    test("returns undefined for a page that does not exist", () => {
      expect(tree.resolve("getting-started/quickstart")).toBeUndefined();
      expect(tree.resolve("../../etc/passwd")).toBeUndefined();
      expect(tree.resolve("reference")).toBeUndefined();
      expect(tree.resolve("https://example.com/getting-started/install")).toBeUndefined();
    });

    test("keeps the canonical case in the page URL", () => {
      expect(tree.url(tree.resolve("getting-started/accesslogin")!)).toBe(
        `${DOCS_SITE_BASE}/getting-started/AccessLogin/`
      );
    });
  });

  describe("directory and children", () => {
    test("names a directory without an _index page, whatever the case or slashes", () => {
      expect(tree.directory("reference")).toBe("reference");
      expect(tree.directory("/Reference/")).toBe("reference");
      expect(tree.directory("tutorials")).toBe("tutorials");
      expect(tree.directory("")).toBe("");
      expect(tree.directory("getting-started/install")).toBeUndefined();
      expect(tree.directory("nope")).toBeUndefined();
    });

    test("lists the pages directly under a directory without its _index page", () => {
      const { pages, dirs } = tree.children("getting-started");
      expect(pages.map(p => p.path)).toEqual(["getting-started/AccessLogin", "getting-started/install"]);
      expect(dirs).toEqual([]);
    });

    test("lists sub-directories with their page count and _index title", () => {
      const { pages, dirs } = tree.children("reference/operators");
      expect(pages.map(p => p.path)).toEqual(["reference/operators/output-modes"]);
      expect(dirs).toEqual([
        { path: "reference/operators/data-input", pageCount: 2, title: "Data Input" },
        { path: "reference/operators/visualization", pageCount: 3, title: undefined },
      ]);
    });

    test("lists the top level for the root", () => {
      const { pages, dirs } = tree.children("");
      expect(pages.map(p => p.path)).toEqual(["overview"]);
      expect(dirs.map(d => `${d.path}:${d.pageCount}`)).toEqual(["getting-started:3", "reference:7", "tutorials:2"]);
    });

    test("returns nothing for a directory that does not exist", () => {
      expect(tree.children("nope")).toEqual({ pages: [], dirs: [] });
    });
  });

  describe("outline", () => {
    test("lists top-level pages and directories with page counts and titles", () => {
      expect(tree.outline(100).split("\n")).toEqual([
        "- overview: Overview",
        "- getting-started/ (3 pages): Getting Started",
        "- reference/ (7 pages)",
        "- tutorials/ (2 pages)",
      ]);
    });

    test("writes a single page count in the singular", () => {
      const one = new DocsIndex([page("a/only", "Only", "x")]);
      expect(one.outline()).toBe("- a/ (1 page)");
    });

    test("expands a directory holding more pages than the threshold into its children", () => {
      expect(tree.outline(5).split("\n")).toEqual([
        "- overview: Overview",
        "- getting-started/ (3 pages): Getting Started",
        "- reference/operators/output-modes: Output Port Modes",
        "- reference/operators/data-input/ (2 pages): Data Input",
        "- reference/operators/visualization/ (3 pages)",
        "- tutorials/ (2 pages)",
      ]);
    });

    test("never lists every page of a large tree", () => {
      const big = new DocsIndex([
        page("_index", "Root", "x"),
        ...filler("a", 80),
        ...filler("b/c", 70),
        ...filler("b/d", 70),
      ]);
      const lines = big.outline().split("\n");
      expect(lines.length).toBeLessThan(10);
      expect(lines).toContain("- b/c/ (70 pages)");
    });

    test("counts the own pages of an expanded directory instead of naming more than eight", () => {
      const mixed = new DocsIndex([...filler("b", 9), ...filler("b/c", 70), ...filler("d", 8), ...filler("d/e", 70)]);
      expect(mixed.outline().split("\n")).toEqual([
        "- b/ (9 pages of its own)",
        "- b/c/ (70 pages)",
        ...Array.from({ length: 8 }, (_, i) => `- d/page-${i + 1}: Page ${i + 1}`),
        "- d/e/ (70 pages)",
      ]);
    });

    test("counts the top-level pages instead of naming more than eight", () => {
      const flat = new DocsIndex(Array.from({ length: 9 }, (_, i) => page(`p${i}`, `P${i}`, "x")));
      expect(flat.outline()).toBe("- the top level (9 pages of its own)");
    });
  });

  describe("closest", () => {
    test("ranks pages sharing words with the requested path, best first", () => {
      expect(tree.closest("getting-started/login").map(p => p.path)[0]).toBe("getting-started/AccessLogin");
      expect(tree.closest("operators/csv-scan").map(p => p.path)[0]).toBe(
        "reference/operators/data-input/csv-file-scan"
      );
    });

    test("honours the limit and returns nothing when no word is shared", () => {
      expect(tree.closest("reference/operators/visualization/page", 2)).toHaveLength(2);
      expect(tree.closest("zzzz/qqqq")).toEqual([]);
      expect(tree.closest("")).toEqual([]);
    });
  });
});

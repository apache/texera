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

import {
  buildFolderDocument,
  checkFolderByteSize,
  checkFolderDocument,
  checkFolderFileCount,
  compareFolderPaths,
  fileBanner,
  folderRootName,
  isExcludedPath,
  isMigratablePythonPath,
  MAX_FOLDER_BYTES,
  MAX_FOLDER_CHARACTERS,
  MAX_FOLDER_FILES,
  MAX_LISTED_OTHER_FILES,
  pickedFileObject,
  pickedFilePath,
  renderFolderTree,
  resolveEntryPoint,
  scopeSegmentationToSpan,
} from "./folder-assembly";
import { segmentScript } from "./script-segmentation";

describe("isMigratablePythonPath", () => {
  it("accepts Python source at any depth, whatever the extension's case", () => {
    expect(isMigratablePythonPath("train.py")).toBe(true);
    expect(isMigratablePythonPath("pkg/models/train.py")).toBe(true);
    expect(isMigratablePythonPath("pkg/Train.PY")).toBe(true);
  });

  it("rejects anything that is not a .py file", () => {
    expect(isMigratablePythonPath("notes.ipynb")).toBe(false);
    expect(isMigratablePythonPath("requirements.txt")).toBe(false);
    expect(isMigratablePythonPath("pkg/py")).toBe(false);
    expect(isMigratablePythonPath("")).toBe(false);
  });

  it("rejects hidden files and anything under a hidden directory", () => {
    expect(isMigratablePythonPath(".hidden.py")).toBe(false);
    expect(isMigratablePythonPath(".venv/lib/thing.py")).toBe(false);
    expect(isMigratablePythonPath("pkg/.tox/run.py")).toBe(false);
  });

  it("rejects caches and vendored dependencies wherever they sit in the tree", () => {
    expect(isMigratablePythonPath("__pycache__/train.py")).toBe(false);
    expect(isMigratablePythonPath("pkg/__pycache__/train.py")).toBe(false);
    expect(isMigratablePythonPath("venv/lib/site-packages/numpy/core.py")).toBe(false);
    expect(isMigratablePythonPath("a/node_modules/b/x.py")).toBe(false);
    expect(isMigratablePythonPath("build/lib/train.py")).toBe(false);
    expect(isMigratablePythonPath("dist/train.py")).toBe(false);
  });

  it("keeps a build or dist package that is not at the root", () => {
    // Only conventional build output at the root. Deeper in a tree these are ordinary package
    // names, and silently dropping real code is the failure the mapping exists to catch.
    expect(isMigratablePythonPath("src/build/features.py")).toBe(true);
    expect(isMigratablePythonPath("pkg/dist/writer.py")).toBe(true);
  });

  it("keeps test files, which are logic the user may want represented", () => {
    expect(isMigratablePythonPath("tests/test_train.py")).toBe(true);
  });
});

describe("compareFolderPaths", () => {
  function sorted(paths: string[]): string[] {
    return [...paths].sort(compareFolderPaths);
  }

  it("groups by directory before comparing names", () => {
    expect(sorted(["b/one.py", "a/two.py", "a/one.py"])).toEqual(["a/one.py", "a/two.py", "b/one.py"]);
  });

  it("puts a package initializer ahead of its siblings", () => {
    expect(sorted(["pkg/aaa.py", "pkg/__init__.py"])).toEqual(["pkg/__init__.py", "pkg/aaa.py"]);
    // Both directions, since sort only ever calls the comparator one way round.
    expect(compareFolderPaths("pkg/__init__.py", "pkg/aaa.py")).toBeLessThan(0);
    expect(compareFolderPaths("pkg/aaa.py", "pkg/__init__.py")).toBeGreaterThan(0);
  });

  it("orders root-level files before files in a subdirectory", () => {
    expect(sorted(["pkg/train.py", "main.py"])).toEqual(["main.py", "pkg/train.py"]);
  });

  it("reports equality for the same path", () => {
    expect(compareFolderPaths("a/x.py", "a/x.py")).toBe(0);
  });

  it("keeps a directory adjacent to its own subdirectories", () => {
    // "-" sorts before "/", so comparing directory paths as plain strings would wedge src-old
    // between src and src/utils, and the layout would then nest src/utils under src-old.
    expect(sorted(["src/a.py", "src-old/b.py", "src/utils/c.py"])).toEqual([
      "src/a.py",
      "src/utils/c.py",
      "src-old/b.py",
    ]);
  });

  it("orders nested directories by segment, not by raw string", () => {
    expect(sorted(["a.b/x.py", "a/y.py", "a/b/z.py"])).toEqual(["a/y.py", "a/b/z.py", "a.b/x.py"]);
  });
});

describe("buildFolderDocument", () => {
  const files = [
    { path: "pkg/train.py", source: "train1\ntrain2" },
    { path: "main.py", source: "main1" },
  ];

  it("concatenates files in sorted order, each behind its banner", () => {
    const document = buildFolderDocument(files);

    expect(document.source.split("\n")).toEqual([
      fileBanner("main.py"),
      "main1",
      fileBanner("pkg/train.py"),
      "train1",
      "train2",
    ]);
  });

  it("reports each file's span and its banner as a forced boundary", () => {
    const document = buildFolderDocument(files);

    expect(document.files).toEqual([
      { path: "main.py", startLine: 1, endLine: 2 },
      { path: "pkg/train.py", startLine: 3, endLine: 5 },
    ]);
    expect(document.forcedBoundaries).toEqual([1, 3]);
  });

  it("drops files holding nothing but whitespace, so no cell is a lone banner", () => {
    const document = buildFolderDocument([
      { path: "a.py", source: "a1" },
      { path: "b.py", source: "   \n\n" },
    ]);

    expect(document.files.map(file => file.path)).toEqual(["a.py"]);
    expect(document.source).toBe(`${fileBanner("a.py")}\na1`);
  });

  it("normalizes CRLF and does not count a trailing newline as a line", () => {
    const document = buildFolderDocument([{ path: "a.py", source: "a1\r\na2\r\n" }]);

    expect(document.source).toBe(`${fileBanner("a.py")}\na1\na2`);
    expect(document.files[0]).toEqual({ path: "a.py", startLine: 1, endLine: 3 });
  });

  it("returns an empty document for an empty selection", () => {
    expect(buildFolderDocument([])).toEqual({ source: "", forcedBoundaries: [], files: [], tree: "project/" });
  });

  it("leaves the caller's array untouched", () => {
    const input = [...files];
    buildFolderDocument(input);

    expect(input.map(file => file.path)).toEqual(["pkg/train.py", "main.py"]);
  });
});

describe("buildFolderDocument with segmentScript", () => {
  // Sequential ids keep assertions readable; the real factory is uuidv4.
  function counterIds(): () => string {
    let next = 0;
    return () => `c${++next}`;
  }

  it("keeps every cell inside one file even when a reported range spans two", () => {
    const document = buildFolderDocument([
      { path: "a.py", source: "a1\na2" },
      { path: "b.py", source: "b1\nb2" },
    ]);

    // a.py occupies lines 1-3 and b.py lines 4-6; one UDF claims across the seam.
    const { cells } = segmentScript(
      document.source,
      { UDF1: [[2, 5]] },
      { forcedBoundaries: document.forcedBoundaries, newUuid: counterIds() }
    );

    // The cut at line 4 is the banner of b.py, so no cell holds lines from both files.
    expect(cells.map(cell => [cell.startLine, cell.endLine])).toEqual([
      [1, 1],
      [2, 3],
      [4, 5],
      [6, 6],
    ]);
  });

  it("still maps a straddling range to every cell it covers", () => {
    const document = buildFolderDocument([
      { path: "a.py", source: "a1\na2" },
      { path: "b.py", source: "b1\nb2" },
    ]);

    const { udfToCellUuids } = segmentScript(
      document.source,
      { UDF1: [[2, 5]] },
      { forcedBoundaries: document.forcedBoundaries, newUuid: counterIds() }
    );

    expect(udfToCellUuids["UDF1"]).toEqual(["c2", "c3"]);
  });
});

describe("folder cap checks", () => {
  function documentOf(characters: number, fileCount = 1) {
    return {
      source: "a".repeat(characters),
      forcedBoundaries: [],
      tree: "proj/",
      files: Array.from({ length: fileCount }, (_unused, index) => ({
        path: `f${index}.py`,
        startLine: 1,
        endLine: 1,
      })),
    };
  }

  it("passes a folder within every cap", () => {
    expect(checkFolderFileCount(3)).toBeNull();
    expect(checkFolderFileCount(MAX_FOLDER_FILES)).toBeNull();
    expect(checkFolderByteSize(MAX_FOLDER_BYTES)).toBeNull();
    expect(checkFolderDocument(documentOf(MAX_FOLDER_CHARACTERS))).toBeNull();
  });

  it("reports a selection with no Python files", () => {
    expect(checkFolderFileCount(0)).toContain("No Python files");
  });

  it("reports an all-empty selection separately from an empty one", () => {
    expect(checkFolderDocument(documentOf(0, 0))).toContain("all empty");
  });

  it("reports the count that exceeded each countable cap", () => {
    expect(checkFolderFileCount(MAX_FOLDER_FILES + 1)).toContain((MAX_FOLDER_FILES + 1).toLocaleString());
    expect(checkFolderDocument(documentOf(MAX_FOLDER_CHARACTERS + 1))).toContain(
      (MAX_FOLDER_CHARACTERS + 1).toLocaleString()
    );
  });

  it("states the limit in every message, not just the measured value", () => {
    expect(checkFolderFileCount(MAX_FOLDER_FILES + 1)).toContain(MAX_FOLDER_FILES.toLocaleString());
    expect(checkFolderDocument(documentOf(MAX_FOLDER_CHARACTERS + 1))).toContain(
      MAX_FOLDER_CHARACTERS.toLocaleString()
    );
  });

  it("quotes one limit in one unit, so the byte guard does not read as a second cap", () => {
    const message = checkFolderByteSize(MAX_FOLDER_BYTES + 1);

    expect(message).toContain(MAX_FOLDER_CHARACTERS.toLocaleString());
    // Neither the byte bound nor a measured byte figure: both would mix units against the cap.
    expect(message).not.toContain(MAX_FOLDER_BYTES.toLocaleString());
    expect(message).not.toMatch(/bytes/i);
  });
});

describe("pickedFileObject", () => {
  it("unwraps both shapes an upload control produces", () => {
    const file = new File([""], "main.py");

    expect(pickedFileObject(file as never)).toBe(file);
    expect(pickedFileObject({ originFileObj: file } as never)).toBe(file);
  });

  it("rejects a wrapper carrying no file, rather than letting FileReader fail later", () => {
    // ng-zorro produces only the two shapes above, but nzFileList can be set from anywhere, and
    // an unchecked cast would surface as a TypeError deep inside the read.
    expect(() => pickedFileObject({ uid: "1", name: "main.py" } as never)).toThrow(/readable file/i);
  });
});

describe("folderRootName", () => {
  it("returns the first segment of a reported path", () => {
    expect(folderRootName("proj/pkg/train.py")).toBe("proj");
    expect(folderRootName("proj/main.py")).toBe("proj");
  });

  it("returns null when no path was reported", () => {
    expect(folderRootName("main.py")).toBeNull();
    expect(folderRootName("")).toBeNull();
  });
});

describe("isExcludedPath", () => {
  it("excludes hidden segments, caches and vendored dependencies", () => {
    expect(isExcludedPath(".venv/stub.py")).toBe(true);
    expect(isExcludedPath("pkg/__pycache__/a.py")).toBe(true);
    expect(isExcludedPath("node_modules/b/x.py")).toBe(true);
  });

  it("excludes build output at the root but not a nested package of the same name", () => {
    expect(isExcludedPath("build/lib/x.py")).toBe(true);
    expect(isExcludedPath("dist/x.py")).toBe(true);
    expect(isExcludedPath("src/build/x.py")).toBe(false);
    // A file, not a directory, so the root rule must not fire.
    expect(isExcludedPath("dist.py")).toBe(false);
  });

  it("allows ordinary project paths, Python or not", () => {
    expect(isExcludedPath("pkg/train.py")).toBe(false);
    expect(isExcludedPath("data/churn.csv")).toBe(false);
    expect(isExcludedPath("")).toBe(true);
  });
});

describe("renderFolderTree", () => {
  it("indents subdirectories and lists a directory's files before its subdirectories", () => {
    const tree = renderFolderTree("proj", ["utils/metrics.py", "main.py", "utils/__init__.py", "data/churn.csv"]);

    expect(tree).toBe(
      ["proj/", "  main.py", "  data/", "    churn.csv", "  utils/", "    __init__.py", "    metrics.py"].join("\n")
    );
  });

  it("names each directory once however many files it holds", () => {
    const tree = renderFolderTree("proj", ["a/one.py", "a/two.py", "a/three.py"]);

    expect(tree.split("\n").filter(line => line.trim() === "a/")).toHaveLength(1);
  });

  it("renders a root with no files as the root alone", () => {
    expect(renderFolderTree("proj", [])).toBe("proj/");
  });

  it("nests a subdirectory under its real parent, not a sibling that sorts between", () => {
    const tree = renderFolderTree("proj", ["src/a.py", "src-old/b.py", "src/utils/c.py"]);

    expect(tree).toBe(["proj/", "  src/", "    a.py", "    utils/", "      c.py", "  src-old/", "    b.py"].join("\n"));
  });
});

describe("buildFolderDocument layout", () => {
  const files = [
    { path: "utils/metrics.py", source: "score()" },
    { path: "main.py", source: "main()" },
  ];

  it("lists the Python files it assembled, under the given root", () => {
    const document = buildFolderDocument(files, { rootName: "churn_pipeline" });

    expect(document.tree).toBe(["churn_pipeline/", "  main.py", "  utils/", "    metrics.py"].join("\n"));
  });

  it("names non-Python files without reading them", () => {
    const document = buildFolderDocument(files, {
      rootName: "proj",
      otherPaths: ["requirements.txt", "data/churn.csv"],
    });

    expect(document.tree).toContain("  requirements.txt");
    expect(document.tree).toContain("    churn.csv");
    // Named only: none of their content reaches the document the model reads.
    expect(document.source).not.toContain("churn.csv");
  });

  it("keeps the layout out of the numbered document entirely", () => {
    const document = buildFolderDocument(files, { rootName: "proj", otherPaths: ["requirements.txt"] });

    // Numbering and segmentation both run on `source`, so a layout line inside it would shift
    // every reported line number and surface as a cell of directory listing in the notebook.
    expect(document.source).not.toContain("proj/");
    expect(document.source).not.toContain("requirements.txt");
  });

  it("names the same non-Python files whatever order the browser reported them in", () => {
    const others = Array.from(
      { length: MAX_LISTED_OTHER_FILES + 3 },
      (_unused, index) => `data/file${String(index).padStart(2, "0")}.csv`
    );

    const forward = buildFolderDocument(files, { rootName: "proj", otherPaths: others });
    const reversed = buildFolderDocument(files, { rootName: "proj", otherPaths: [...others].reverse() });

    expect(reversed.tree).toBe(forward.tree);
    expect(forward.tree).toContain("file00.csv");
    expect(forward.tree).not.toContain(`file${MAX_LISTED_OTHER_FILES}.csv`);
  });

  it("caps the non-Python files it names and says how many it left out", () => {
    const others = Array.from({ length: MAX_LISTED_OTHER_FILES + 3 }, (_unused, index) => `data/file${index}.csv`);
    const document = buildFolderDocument(files, { rootName: "proj", otherPaths: others });

    expect(document.tree).toContain("... and 3 more non-Python files");
    expect(document.tree.split("\n").filter(line => line.endsWith(".csv"))).toHaveLength(MAX_LISTED_OTHER_FILES);
  });

  it("defaults the root label when none is given", () => {
    expect(buildFolderDocument(files).tree.split("\n")[0]).toBe("project/");
  });
});

describe("pickedFilePath", () => {
  // ng-zorro's beforeUpload hands over the browser File with a uid attached; it only wraps the
  // File in originFileObj later, when it builds the display list. Both shapes have to be read, or
  // a real directory pick reports no path and looks like an empty selection.
  it("reads the path off the File itself", () => {
    expect(pickedFilePath({ webkitRelativePath: "proj/main.py" })).toBe("proj/main.py");
  });

  it("reads the path off a wrapper holding the File", () => {
    expect(pickedFilePath({ originFileObj: { webkitRelativePath: "proj/main.py" } })).toBe("proj/main.py");
  });

  it("prefers the wrapped File when both are present", () => {
    expect(
      pickedFilePath({ webkitRelativePath: "outer/a.py", originFileObj: { webkitRelativePath: "inner/a.py" } })
    ).toBe("inner/a.py");
  });

  it("returns an empty string when neither shape reports a path", () => {
    expect(pickedFilePath({})).toBe("");
    expect(pickedFilePath(undefined)).toBe("");
    expect(pickedFilePath({ originFileObj: {} })).toBe("");
  });
});

describe("resolveEntryPoint", () => {
  const files = [
    { path: "main.py", startLine: 1, endLine: 10 },
    { path: "pkg/train.py", startLine: 11, endLine: 20 },
  ];

  it("matches the path the model gave", () => {
    expect(resolveEntryPoint(files, "pkg/train.py")).toEqual(files[1]);
  });

  it("tolerates the leading-slash and dot-slash forms models drift between", () => {
    expect(resolveEntryPoint(files, "./main.py")).toEqual(files[0]);
    expect(resolveEntryPoint(files, "/main.py")).toEqual(files[0]);
    expect(resolveEntryPoint(files, "  main.py  ")).toEqual(files[0]);
  });

  it("ignores case", () => {
    expect(resolveEntryPoint(files, "PKG/Train.PY")).toEqual(files[1]);
  });

  it("falls back to matching on the file name alone", () => {
    expect(resolveEntryPoint(files, "train.py")).toEqual(files[1]);
  });

  it("matches a top-level file when the model prefixed the selected folder's name", () => {
    // The layout's first line is the folder name, so the model naming "proj/main.py" for a
    // top-level main.py is the likely answer, not an edge case.
    expect(resolveEntryPoint(files, "proj/main.py")).toEqual(files[0]);
    expect(resolveEntryPoint(files, "churn_pipeline/PKG/train.py")).toEqual(files[1]);
  });

  it("refuses an ambiguous file name rather than guessing between same-named files", () => {
    const ambiguous = [
      { path: "a/main.py", startLine: 1, endLine: 5 },
      { path: "b/main.py", startLine: 6, endLine: 9 },
    ];

    // Showing the wrong file is worse than falling back to the whole folder.
    expect(resolveEntryPoint(ambiguous, "main.py")).toBeNull();
    // An exact path is still unambiguous.
    expect(resolveEntryPoint(ambiguous, "b/main.py")).toEqual(ambiguous[1]);
  });

  it("returns null for an answer it cannot match, so the caller shows everything", () => {
    expect(resolveEntryPoint(files, "nowhere.py")).toBeNull();
    expect(resolveEntryPoint(files, "")).toBeNull();
    expect(resolveEntryPoint(files, undefined)).toBeNull();
    expect(resolveEntryPoint(files, 7)).toBeNull();
    expect(resolveEntryPoint([], "main.py")).toBeNull();
  });
});

describe("scopeSegmentationToSpan", () => {
  const segmentation = {
    cells: [
      { uuid: "c1", source: "a", startLine: 1, endLine: 3 },
      { uuid: "c2", source: "b", startLine: 4, endLine: 5 },
      { uuid: "c3", source: "c", startLine: 6, endLine: 8 },
    ],
    udfToCellUuids: { UDF1: ["c1"], UDF2: ["c2", "c3"], UDF3: ["c2"] },
  };

  it("keeps only the cells inside the span", () => {
    const scoped = scopeSegmentationToSpan(segmentation, { startLine: 4, endLine: 5 });

    expect(scoped.cells.map(cell => cell.uuid)).toEqual(["c2"]);
  });

  it("drops mapping entries whose cells all fell outside, keeping the rest", () => {
    const scoped = scopeSegmentationToSpan(segmentation, { startLine: 4, endLine: 5 });

    // UDF1 lived entirely outside the span, so it names no cell rather than a missing one.
    expect(scoped.udfToCellUuids).toEqual({ UDF2: ["c2"], UDF3: ["c2"] });
  });

  it("warns for each UDF it drops, since the model was asked to stay inside the span", () => {
    // spyOn reuses an existing spy, and this file has no restore hook, so clear it per test.
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    warn.mockClear();

    scopeSegmentationToSpan(segmentation, { startLine: 4, endLine: 5 });

    const dropped = warn.mock.calls.filter(call => String(call[0]).includes("Dropping mapping entry"));
    expect(dropped).toHaveLength(1);
    expect(dropped[0][0]).toContain("UDF1");
  });

  it("warns when every surviving operator lands on one cell", () => {
    // spyOn reuses an existing spy, and this file has no restore hook, so clear it per test.
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    warn.mockClear();

    // A thin launcher ("from app import run" then "run()") puts every operator on one line.
    scopeSegmentationToSpan(segmentation, { startLine: 4, endLine: 5 });

    const collapsed = warn.mock.calls.filter(call => String(call[0]).includes("thin launcher"));
    expect(collapsed).toHaveLength(1);
  });

  it("does not warn about a thin launcher when the operators land on different cells", () => {
    // spyOn reuses an existing spy, and this file has no restore hook, so clear it per test.
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    warn.mockClear();

    scopeSegmentationToSpan(segmentation, { startLine: 1, endLine: 8 });

    expect(warn.mock.calls.filter(call => String(call[0]).includes("thin launcher"))).toHaveLength(0);
  });

  it("is a no-op for a span covering everything", () => {
    const scoped = scopeSegmentationToSpan(segmentation, { startLine: 1, endLine: 8 });

    expect(scoped).toEqual(segmentation);
  });

  it("returns nothing for a span covering no whole cell", () => {
    const scoped = scopeSegmentationToSpan(segmentation, { startLine: 2, endLine: 2 });

    expect(scoped.cells).toEqual([]);
    expect(scoped.udfToCellUuids).toEqual({});
  });
});

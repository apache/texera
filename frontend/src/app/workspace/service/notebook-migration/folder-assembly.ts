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

import { ScriptSegmentation, splitScriptLines } from "./script-segmentation";

/**
 * Assembles a folder of Python sources into the one numbered document the migration LLM sees.
 *
 * Converted as a single input, so the model reports line ranges as it does for a script and
 * nothing downstream learns folders exist; a banner line before each file carries provenance.
 * Lines split through the segmenter's own splitScriptLines, so assembly, numbering and slicing
 * agree on what a line is.
 */

// Never project code, matched per segment so a cache or vendored dependency is caught at any
// depth. Hidden segments are covered separately by the leading-dot rule.
const EXCLUDED_SEGMENTS = new Set(["__pycache__", "venv", "site-packages", "node_modules"]);

// Build output at the root, but ordinary package names deeper in a tree (src/build/ can be real
// code). Dropping real code silently is worse than letting a build tree hit the caps.
const EXCLUDED_ROOT_DIRECTORIES = new Set(["build", "dist"]);

// Non-Python files are named in the layout, never read. Capped because a folder can hold
// thousands of data files and the layout is orientation, not an inventory.
export const MAX_LISTED_OTHER_FILES = 20;

/**
 * Caps on one conversion, refused before any request is sent. Sized by the reply, not the prompt:
 * the conversion asks for every line of code back, so it must fit MAX_CONVERSION_OUTPUT_TOKENS.
 * Raise once a larger folder has been measured.
 */
export const MAX_FOLDER_FILES = 100;
export const MAX_FOLDER_CHARACTERS = 60_000;

// Pre-read guard for the character cap. UTF-8 uses at most four bytes per character, so anything
// past four times the cap is certainly past the cap, and one huge file would otherwise hang the
// tab before the character cap, which needs the files read, could run.
export const MAX_FOLDER_BYTES = MAX_FOLDER_CHARACTERS * 4;

export const FILE_BANNER_PREFIX = "# ===== FILE: ";
export const FILE_BANNER_SUFFIX = " =====";

/** The line that introduces a file in the assembled document. */
export function fileBanner(path: string): string {
  return `${FILE_BANNER_PREFIX}${path}${FILE_BANNER_SUFFIX}`;
}

export interface FolderFile {
  // Path relative to the selected folder, "/" separated, as webkitRelativePath reports it.
  path: string;
  source: string;
}

// 1-indexed and inclusive bounds within the assembled document.
export interface LineSpan {
  startLine: number;
  endLine: number;
}

// Spans the banner line through the file's last line.
export interface FolderFileSpan extends LineSpan {
  path: string;
}

export interface FolderDocument {
  // The concatenated source, which is what gets numbered for the prompt and segmented afterwards.
  source: string;
  // Each file's banner line, passed to segmentScript so no cell straddles a file.
  forcedBoundaries: number[];
  // The files that made it into the document, in the order they appear in it.
  files: FolderFileSpan[];
  /**
   * An indented listing of the folder, sent ahead of the document as prompt text. Never part of
   * `source`: numbering it would shift every line the model reports, and the segmenter would emit
   * it as a cell of directory listing. Also the only place non-Python files are named.
   */
  tree: string;
}

export interface FolderDocumentOptions {
  // The selected folder's own name, used as the layout's root label.
  rootName?: string;
  // Paths in the selection that hold no Python source. Listed in the layout, never read.
  otherPaths?: readonly string[];
}

/** Either shape an upload control hands a picked file over in: the File itself, or a wrapper. */
export interface PickedFile {
  webkitRelativePath?: string;
  originFileObj?: { webkitRelativePath?: string };
}

/**
 * The path a directory picker reported for a picked file. Both shapes must be read: ng-zorro's
 * beforeUpload gets the File itself with a uid attached and only wraps it in `originFileObj`
 * later, so checking one alone silently yields no path.
 */
export function pickedFilePath(file: PickedFile | undefined): string {
  return file?.originFileObj?.webkitRelativePath ?? file?.webkitRelativePath ?? "";
}

/** The browser File behind a picked file, whichever of the two shapes the control used. */
export function pickedFileObject(file: PickedFile): File {
  const candidate = file.originFileObj ?? file;
  if (!(candidate instanceof Blob)) {
    throw new Error("The upload control did not provide a readable file.");
  }
  return candidate as File;
}

// A directory picker reports "<selected folder>/<path within it>" for every file.
function splitReportedPath(webkitRelativePath: string): { root: string | null; rest: string } {
  const separator = webkitRelativePath.indexOf("/");
  return separator === -1
    ? { root: null, rest: webkitRelativePath }
    : { root: webkitRelativePath.slice(0, separator), rest: webkitRelativePath.slice(separator + 1) };
}

/** The selected folder's own name. Null when the picker reported no path. */
export function folderRootName(webkitRelativePath: string): string | null {
  return splitReportedPath(webkitRelativePath).root;
}

/**
 * A picked file's path relative to the folder the user selected. Dropping the root segment keeps
 * the banners relative to the selection and stops a dot-prefixed folder name from excluding
 * everything inside it. Falls back to the file's name when no path was reported.
 */
export function folderRelativePath(file: { webkitRelativePath?: string; name: string }): string {
  const full = file.webkitRelativePath;
  if (!full) {
    return file.name;
  }
  return splitReportedPath(full).rest;
}

function pathSegments(path: string): string[] {
  return path.split("/").filter(segment => segment !== "" && segment !== ".");
}

/** True for a cache, a vendored dependency or anything hidden. Left out of the layout as well as
 * the document: noise, not structure. */
export function isExcludedPath(path: string): boolean {
  const segments = pathSegments(path);
  if (segments.length === 0) {
    return true;
  }
  if (segments.length > 1 && EXCLUDED_ROOT_DIRECTORIES.has(segments[0])) {
    return true;
  }
  return segments.some(segment => segment.startsWith(".") || EXCLUDED_SEGMENTS.has(segment));
}

/** True for a path holding project Python source. Tests are kept: dropping real logic silently is
 * the failure the mapping exists to prevent. */
export function isMigratablePythonPath(path: string): boolean {
  const segments = pathSegments(path);
  if (segments.length === 0) return false;
  return segments[segments.length - 1].toLowerCase().endsWith(".py") && !isExcludedPath(path);
}

/** Renders the folder as an indented listing. Files come before subdirectories, which ordering by
 * directory then name already produces. */
export function renderFolderTree(rootName: string, paths: readonly string[]): string {
  const lines = [`${rootName}/`];
  const seenDirectories = new Set<string>();

  for (const path of [...paths].sort(compareFolderPaths)) {
    const segments = path.split("/");
    segments.slice(0, -1).forEach((directory, index) => {
      const prefix = segments.slice(0, index + 1).join("/");
      if (!seenDirectories.has(prefix)) {
        seenDirectories.add(prefix);
        lines.push(`${"  ".repeat(index + 1)}${directory}/`);
      }
    });
    lines.push(`${"  ".repeat(segments.length)}${segments[segments.length - 1]}`);
  }

  return lines.join("\n");
}

function pathParts(path: string): { dir: string; name: string } {
  const index = path.lastIndexOf("/");
  return index === -1 ? { dir: "", name: path } : { dir: path.slice(0, index), name: path.slice(index + 1) };
}

/**
 * Sort key that keeps a directory adjacent to its own subdirectories. Comparing paths as plain
 * strings fails because "/" (0x2F) sorts after name characters like "-", putting "src-old" between
 * "src" and "src/utils". NUL sorts below everything, restoring segment order.
 */
function directorySortKey(directory: string): string {
  return directory.split("/").join("\u0000");
}

/**
 * Orders files by directory, then by name with a package's `__init__.py` first. Deterministic over
 * clever: the model infers dataflow from the code, so a wrong guess at import order costs more
 * than a stable one. Plain comparison, not localeCompare, so locale cannot shift it.
 */
export function compareFolderPaths(a: string, b: string): number {
  const left = pathParts(a);
  const right = pathParts(b);
  if (left.dir !== right.dir) {
    const leftKey = directorySortKey(left.dir);
    const rightKey = directorySortKey(right.dir);
    return leftKey < rightKey ? -1 : 1;
  }
  const leftIsInit = left.name === "__init__.py";
  const rightIsInit = right.name === "__init__.py";
  if (leftIsInit !== rightIsInit) return leftIsInit ? -1 : 1;
  if (left.name === right.name) return 0;
  return left.name < right.name ? -1 : 1;
}

/**
 * Concatenates the selected files into one document, banner line first for each. Whitespace-only
 * files are dropped, since they would render as a cell holding just a heading; `files` reports
 * what was actually included.
 */
export function buildFolderDocument(files: readonly FolderFile[], options: FolderDocumentOptions = {}): FolderDocument {
  const { rootName = "project", otherPaths = [] } = options;
  const ordered = [...files].sort((a, b) => compareFolderPaths(a.path, b.path));
  const documentLines: string[] = [];
  const spans: FolderFileSpan[] = [];

  for (const file of ordered) {
    const lines = splitScriptLines(file.source);
    if (lines.every(line => line.trim() === "")) continue;
    const startLine = documentLines.length + 1;
    documentLines.push(fileBanner(file.path), ...lines);
    spans.push({ path: file.path, startLine, endLine: documentLines.length });
  }

  // Sorted before slicing, so which files get named does not depend on the browser's ordering.
  const listedOthers = [...otherPaths].sort(compareFolderPaths).slice(0, MAX_LISTED_OTHER_FILES);
  const tree = renderFolderTree(rootName, [...spans.map(span => span.path), ...listedOthers]);
  const hidden = otherPaths.length - listedOthers.length;

  return {
    source: documentLines.join("\n"),
    forcedBoundaries: spans.map(span => span.startLine),
    files: spans,
    tree: hidden > 0 ? `${tree}\n  ... and ${hidden} more non-Python files` : tree,
  };
}

/**
 * The three checks a folder must pass. Separate because they run at different points: file count
 * and byte size before anything is read, character count only once the document is assembled.
 * Refusing up front beats truncating, which would produce a silently incomplete workflow.
 */
export function checkFolderFileCount(fileCount: number): string | null {
  if (fileCount === 0) {
    return "No Python files were found in the selected folder.";
  }
  if (fileCount > MAX_FOLDER_FILES) {
    return `The selected folder has ${fileCount.toLocaleString()} Python files, more than the ${MAX_FOLDER_FILES.toLocaleString()} this tool converts at once. Select a smaller folder.`;
  }
  return null;
}

export function checkFolderByteSize(totalBytes: number): string | null {
  if (totalBytes > MAX_FOLDER_BYTES) {
    // Quotes the character cap and no byte figure: the byte bound guards that same limit, and
    // naming both would read as two limits in two units.
    return `The selected folder is too large to convert. Its Python files hold more than the ${MAX_FOLDER_CHARACTERS.toLocaleString()} characters this tool converts at once. Select a smaller folder.`;
  }
  return null;
}

export function checkFolderDocument(document: FolderDocument): string | null {
  if (document.files.length === 0) {
    // Distinct from finding no Python at all: these were found, and every one held no code.
    return "The Python files in the selected folder are all empty.";
  }
  if (document.source.length > MAX_FOLDER_CHARACTERS) {
    // Counts the assembled document, so it includes the banner line added per file.
    return `The selected folder's Python code totals ${document.source.length.toLocaleString()} characters, more than the ${MAX_FOLDER_CHARACTERS.toLocaleString()} this tool converts at once. Select a smaller folder.`;
  }
  return null;
}

/**
 * Matches the entry point the model named against the files assembled. The reply is untrusted, so
 * matching is tolerant: exact path, then ignoring case, then the file name alone. Null when
 * nothing matches, which the caller reads as "show the whole folder" rather than "show nothing".
 */
export function resolveEntryPoint(files: readonly FolderFileSpan[], reported: unknown): FolderFileSpan | null {
  if (typeof reported !== "string") return null;
  // Models drift between "main.py", "./main.py" and "/main.py" for the same file.
  const wanted = reported.trim().replace(/^\.?\//, "");
  if (wanted === "") return null;

  const lowered = wanted.toLowerCase();
  const exact = files.find(file => file.path === wanted) ?? files.find(file => file.path.toLowerCase() === lowered);
  if (exact) {
    return exact;
  }

  // Name-only fallback, which catches the model prefixing the selected folder's name. The leading
  // "/" is what lets a top-level file match, since a bare "main.py" never ends with "/main.py".
  // Ambiguity returns null: showing the wrong same-named file is worse than showing everything.
  const baseName = lowered.slice(lowered.lastIndexOf("/") + 1);
  const byName = files.filter(file => `/${file.path.toLowerCase()}`.endsWith(`/${baseName}`));
  return byName.length === 1 ? byName[0] : null;
}

/**
 * Narrows a segmentation to one file's span. A UDF whose ranges all fell outside drops out of the
 * mapping rather than naming a cell the notebook does not contain, so the stored mapping and the
 * stored notebook always describe the same thing.
 */
export function scopeSegmentationToSpan(segmentation: ScriptSegmentation, span: LineSpan): ScriptSegmentation {
  const cells = segmentation.cells.filter(cell => cell.startLine >= span.startLine && cell.endLine <= span.endLine);
  const visible = new Set(cells.map(cell => cell.uuid));

  const udfToCellUuids: Record<string, string[]> = {};
  for (const [udfId, uuids] of Object.entries(segmentation.udfToCellUuids)) {
    const kept = uuids.filter(uuid => visible.has(uuid));
    if (kept.length > 0) {
      udfToCellUuids[udfId] = kept;
    } else {
      // Rare by design: the model was asked for ranges inside the entry point. Usually means it
      // numbered lines from 1 per file, which is otherwise invisible.
      console.warn(`Dropping mapping entry for UDF id ${udfId}: none of its lines fall inside the entry point`);
    }
  }

  // A thin launcher (one import, one call) puts every operator on the same line, so highlighting
  // cannot tell them apart. The conversion is fine; only the highlighting is uninformative.
  const mappedUdfs = Object.keys(udfToCellUuids);
  const distinctCells = new Set(Object.values(udfToCellUuids).flat());
  if (mappedUdfs.length > 1 && distinctCells.size === 1) {
    console.warn(
      `All ${mappedUdfs.length} operators mapped to the same cell: the entry point looks like a thin launcher, so cell highlighting will not distinguish them`
    );
  }

  return { cells, udfToCellUuids };
}

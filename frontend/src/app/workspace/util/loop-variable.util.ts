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

import { WorkflowGraphReadonly } from "../service/workflow-graph/model/workflow-graph";

/**
 * A plain assignment target: an optional star, an identifier and an optional annotation (`x: int`).
 * Identifiers are ASCII because a `$name` reference can only spell ASCII names.
 */
const ASSIGNMENT_TARGET = /^\*?\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?::[^=]*)?$/;

/** The characters that, right before `=`, make it part of another operator (`==`, `+=`, `<=`, `:=`, ...). */
const NOT_AN_ASSIGNMENT_BEFORE = "=!<>+-*/%&|^:@";

/** Python's keywords, which are never a name (`else: y = 1` must not yield `else`). */
const PYTHON_KEYWORDS: ReadonlySet<string> = new Set([
  "False",
  "None",
  "True",
  "and",
  "as",
  "assert",
  "async",
  "await",
  "break",
  "class",
  "continue",
  "def",
  "del",
  "elif",
  "else",
  "except",
  "finally",
  "for",
  "from",
  "global",
  "if",
  "import",
  "in",
  "is",
  "lambda",
  "nonlocal",
  "not",
  "or",
  "pass",
  "raise",
  "return",
  "try",
  "while",
  "with",
  "yield",
]);

/** The head of a statement that opens a def or class body, whose names are local to it. */
const LOCAL_SCOPE_HEADER = /^(?:async\s+)?(?:def|class)\b/;

/** The head of a compound statement whose body runs at module level; group 1 is the keyword. */
const COMPOUND_HEADER = /^(?:async\s+)?(if|elif|else|while|for|with|try|except|finally)\b/;

const OPENING = "([{";
const CLOSING = ")]}";

interface Statement {
  text: string;
  /** Whether the statement begins a line (as opposed to following a `;`), so that indentation is telling. */
  startsLine: boolean;
}

/**
 * The names a Loop Start's "initialization" Python code binds at module level, in first-binding order and
 * without duplicates: the loop's variables, which the operators of its block may refer to as `$name`.
 * The backend runs the code with exec and keeps the resulting globals as the loop state, and Python has
 * no block scope, so these are the assignments at any depth outside a def or class body, whether at the
 * top level or under an if/elif/else, try/except/finally, while, for or with, plus the targets of a for
 * loop.
 *
 * Assignments: `i = 0`, `K = 2; prev = float('-inf')`, tuple targets (`a, b = 1, 2`), chained
 * (`a = b = 0`), annotated (`x: int = 0`) and starred (`a, *rest = xs`) ones. Augmented assignments,
 * comparisons, keyword arguments, attribute or subscript targets and comments are not assignments of a
 * loop variable and are ignored, as are the names an import, a def, a class, `with ... as` or
 * `except ... as` binds: none of those is a value the loop state can carry. Separators inside strings or
 * brackets do not split.
 */
export function extractLoopVariables(initialization: string): string[] {
  const names: string[] = [];
  // The indentation of the def/class header whose body the current line is in; undefined at module level.
  let localHeaderIndent: number | undefined;
  let lineIsLocal = false;
  for (const statement of splitStatements(initialization)) {
    const text = statement.text.trim();
    if (statement.startsLine) {
      const indent = statement.text.length - statement.text.trimStart().length;
      if (localHeaderIndent !== undefined && indent <= localHeaderIndent) {
        localHeaderIndent = undefined; // dedented out of the def/class body
      }
      lineIsLocal = localHeaderIndent !== undefined;
      if (!lineIsLocal && LOCAL_SCOPE_HEADER.test(text)) {
        // its body, on the lines below or after the colon, is local; so is what follows a `;` on the line
        localHeaderIndent = indent;
        lineIsLocal = true;
      }
    }
    if (lineIsLocal) {
      continue;
    }
    for (const name of boundNames(text)) {
      if (!names.includes(name)) {
        names.push(name);
      }
    }
  }
  return names;
}

/**
 * The loop variables in scope of an operator: those of every enclosing Loop Start, outermost first,
 * without duplicates. Undefined outside every block, so that one call tells both whether the operator
 * sits in a block and what it may refer to; empty for a block whose Loop Starts declare nothing. A
 * Loop Start whose initialization is missing or not a string contributes nothing.
 */
export function loopVariablesInScope(graph: WorkflowGraphReadonly, operatorID: string): string[] | undefined {
  const loopStartIDs = graph.getEnclosingLoopStarts(operatorID);
  if (loopStartIDs.length === 0) {
    return undefined;
  }
  const names: string[] = [];
  for (const loopStartID of loopStartIDs) {
    const initialization: unknown = graph.getOperator(loopStartID).operatorProperties?.["initialization"];
    if (typeof initialization !== "string") {
      continue;
    }
    for (const name of extractLoopVariables(initialization)) {
      if (!names.includes(name)) {
        names.push(name);
      }
    }
  }
  return names;
}

/**
 * Splits Python code into statements at top-level newlines and semicolons, dropping comments. Quotes
 * and brackets protect their contents, and a backslash continues a line.
 */
function splitStatements(code: string): Statement[] {
  const statements: Statement[] = [];
  let current = "";
  let startsLine = true;
  let depth = 0;
  let quote: string | undefined;
  const flush = (nextStartsLine: boolean) => {
    if (current.trim() !== "") {
      statements.push({ text: current, startsLine });
    }
    current = "";
    startsLine = nextStartsLine;
  };
  for (let i = 0; i < code.length; i++) {
    const char = code[i];
    if (quote !== undefined) {
      current += char;
      if (char === "\\" && i + 1 < code.length) {
        current += code[++i];
      } else if (char === quote) {
        quote = undefined;
      }
      continue;
    }
    if (char === "'" || char === '"') {
      quote = char;
    } else if (char === "#") {
      while (i + 1 < code.length && code[i + 1] !== "\n") {
        i++;
      }
      continue;
    } else if (char === "\\" && code[i + 1] === "\n") {
      i++;
      current += " ";
      continue;
    } else if (OPENING.includes(char)) {
      depth++;
    } else if (CLOSING.includes(char)) {
      depth = Math.max(0, depth - 1);
    } else if (depth === 0 && char === "\n") {
      flush(true);
      continue;
    } else if (depth === 0 && char === ";") {
      flush(false);
      continue;
    }
    current += char;
  }
  flush(true);
  return statements;
}

/**
 * The names one module-level statement binds: its assignment targets or, for a compound statement, a
 * for loop's targets plus what its one-line body (`try: k = 1`) binds. A compound statement whose body
 * is on the lines below binds nothing itself; those lines are statements of their own.
 */
function boundNames(statement: string): string[] {
  const compound = COMPOUND_HEADER.exec(statement);
  if (compound === null) {
    return assignedNames(statement);
  }
  // the header ends at the first top-level colon that is not the walrus `:=`
  const colon = topLevelPositions(statement, (text, i) => text[i] === ":" && text[i + 1] !== "=")[0];
  if (colon === undefined) {
    return [];
  }
  const header = statement.slice(compound[0].length, colon);
  const loopTargets = compound[1] === "for" ? forTargetNames(header) : [];
  const body = statement.slice(colon + 1).trim();
  return body === "" ? loopTargets : [...loopTargets, ...boundNames(body)];
}

/** The names a plain statement assigns, e.g. `a = b = 0` -> a, b; `x += 1` or `f(k=1)` -> nothing. */
function assignedNames(statement: string): string[] {
  const names: string[] = [];
  let from = 0;
  for (const position of assignmentPositions(statement)) {
    names.push(...targetNames(statement.slice(from, position)));
    from = position + 1;
  }
  return names;
}

/** The targets of a for loop's header (what follows `for`), e.g. ` i, (a, b) in pairs` -> i, a, b. */
function forTargetNames(header: string): string[] {
  const inKeyword = topLevelPositions(
    header,
    (text, i) => /^in\b/.test(text.slice(i)) && /[^A-Za-z0-9_]/.test(text[i - 1] ?? " ")
  )[0];
  return inKeyword === undefined ? [] : targetNames(header.slice(0, inKeyword));
}

/** The indexes of the top-level `=` signs that are assignments (not part of `==`, `+=`, `<=`, `:=`, ...). */
function assignmentPositions(statement: string): number[] {
  return topLevelPositions(
    statement,
    (text, i) => text[i] === "=" && text[i + 1] !== "=" && !NOT_AN_ASSIGNMENT_BEFORE.includes(text[i - 1] ?? "")
  );
}

/** The indexes, outside strings and brackets, at which `matches` holds, in order. */
function topLevelPositions(text: string, matches: (text: string, index: number) => boolean): number[] {
  const positions: number[] = [];
  let depth = 0;
  let quote: string | undefined;
  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    if (quote !== undefined) {
      if (char === "\\") {
        i++;
      } else if (char === quote) {
        quote = undefined;
      }
      continue;
    }
    if (char === "'" || char === '"') {
      quote = char;
    } else if (OPENING.includes(char)) {
      depth++;
    } else if (CLOSING.includes(char)) {
      depth = Math.max(0, depth - 1);
    } else if (depth === 0 && matches(text, i)) {
      positions.push(i);
    }
  }
  return positions;
}

/** The plain names in one assignment target list, e.g. `a, (b, c)` -> a, b, c; `self.x, d[0]` -> nothing. */
function targetNames(target: string): string[] {
  const names: string[] = [];
  for (const item of splitTopLevelCommas(unwrap(target.trim()))) {
    const trimmed = item.trim();
    const match = ASSIGNMENT_TARGET.exec(trimmed);
    if (match !== null) {
      if (!PYTHON_KEYWORDS.has(match[1])) {
        names.push(match[1]);
      }
    } else if (/^[([]/.test(trimmed)) {
      names.push(...targetNames(trimmed)); // a nested tuple target
    }
  }
  return names;
}

/** Strips one pair of brackets that wraps the whole text, e.g. `(a, b)` -> `a, b`. */
function unwrap(text: string): string {
  const openIndex = OPENING.indexOf(text[0] ?? "");
  if (openIndex === -1 || text[text.length - 1] !== CLOSING[openIndex]) {
    return text;
  }
  let depth = 0;
  for (let i = 0; i < text.length - 1; i++) {
    if (OPENING.includes(text[i])) {
      depth++;
    } else if (CLOSING.includes(text[i])) {
      depth--;
      if (depth === 0) {
        return text; // the opening bracket closes before the end: it does not wrap the whole text
      }
    }
  }
  return text.slice(1, -1);
}

function splitTopLevelCommas(text: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let from = 0;
  for (let i = 0; i < text.length; i++) {
    if (OPENING.includes(text[i])) {
      depth++;
    } else if (CLOSING.includes(text[i])) {
      depth = Math.max(0, depth - 1);
    } else if (depth === 0 && text[i] === ",") {
      parts.push(text.slice(from, i));
      from = i + 1;
    }
  }
  parts.push(text.slice(from));
  return parts;
}

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

// Measures recall@3 and recall@5 of the docs keyword index over a fixed question set.
// Usage: bun run scripts/docs-search-eval.ts [docsDir]   (default: ../docs)

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { loadDocsIndex } from "../src/agent/tools/docs-index";

interface EvalQuestion {
  group: string;
  question: string;
  expected: string;
}

const docsDir = process.argv[2] ?? join(import.meta.dir, "..", "..", "docs");
const index = loadDocsIndex(docsDir);
if (!index) {
  console.error(`docs directory not found: ${docsDir}`);
  process.exit(1);
}

const questions: EvalQuestion[] = JSON.parse(readFileSync(join(import.meta.dir, "docs-search-eval.json"), "utf8"));

let hitAt3 = 0;
let hitAt5 = 0;
const misses: string[] = [];
const noHit: string[] = [];

for (const q of questions) {
  const hits = index.search(q.question, 5);
  const paths = [...new Set(hits.map(h => h.path))];
  const rank = paths.indexOf(q.expected);
  if (rank >= 0 && rank < 3) hitAt3++;
  if (rank >= 0) hitAt5++;
  if (hits.length === 0) noHit.push(q.question);
  const mark = rank < 0 ? "MISS" : `#${rank + 1}`;
  console.log(`${mark.padEnd(5)} [${q.group}] ${q.question}`);
  if (rank < 0) {
    misses.push(`${q.question} -> expected ${q.expected}, got ${paths.slice(0, 3).join(", ") || "(nothing)"}`);
  }
}

console.log(`\npages indexed: ${index.pages().length}`);
console.log(`recall@3: ${hitAt3}/${questions.length} = ${(hitAt3 / questions.length).toFixed(2)}`);
console.log(`recall@5: ${hitAt5}/${questions.length} = ${(hitAt5 / questions.length).toFixed(2)}`);
if (misses.length > 0) {
  console.log("\nmissed (expected page not in top 5):");
  for (const m of misses) console.log(`- ${m}`);
}
console.log(`\nno hit at all (candidate docs gaps): ${noHit.length === 0 ? "none" : ""}`);
for (const q of noHit) console.log(`- ${q}`);

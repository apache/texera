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

// Generates the operator embedding index consumed by the semantic search.
// Run offline; the JSON it writes is committed so the app never needs an API key.
//
//   node frontend/scripts/generate-operator-embeddings.mjs <metadata.json> \
//     frontend/src/assets/operator-embeddings.json \
//     frontend/scripts/operator-hints.json
//
// <metadata.json> is the response of GET /api/resources/operator-metadata from
// a running instance. The index only needs regenerating when an operator is
// added or its name, group or description changes.
import { pipeline } from "@xenova/transformers";
import { readFileSync, writeFileSync, existsSync } from "fs";

// An English-only model. Measured against 19 intent queries with known answers
// it ranks the right operator first 14 times, where the multilingual model of
// the same family manages 6: short operator text gives a multilingual model too
// little to disambiguate on. Larger retrieval-tuned models (mpnet, bge, gte) did
// no better here either — these documents are a dozen words, not paragraphs.
const MODEL = "Xenova/all-MiniLM-L6-v2";

const SRC = process.argv[2];
const OUT = process.argv[3];
const HINTS = process.argv[4];

const meta = JSON.parse(readFileSync(SRC, "utf-8"));

// Hand-written phrasings per operator, keyed by userFriendlyName. Optional:
// without them the index still builds, just from Texera's own metadata.
let hints = {};
if (HINTS && existsSync(HINTS)) {
  const raw = JSON.parse(readFileSync(HINTS, "utf-8"));
  hints = Object.fromEntries(Object.entries(raw).filter(([key]) => !key.startsWith("_")));
}

const known = new Set(meta.operators.map(op => op.additionalMetadata.userFriendlyName));
const unknown = Object.keys(hints).filter(name => !known.has(name));
if (unknown.length > 0) {
  // A typo here silently costs an operator its phrasings, so it is worth saying.
  console.warn(`warning: ${unknown.length} hint name(s) match no operator: ${unknown.join(", ")}`);
}

const extractor = await pipeline("feature-extraction", MODEL);

const round = value => Math.round(value * 1e4) / 1e4;
const entries = [];
let done = 0;

for (const op of meta.operators) {
  const m = op.additionalMetadata;
  // Name, group and description all carry signal; the stock keyword search only
  // ever looked at the name. The phrasings, where present, carry the most: they
  // are the words a user reaches for, which the metadata rarely contains.
  const phrasings = hints[m.userFriendlyName] ?? [];
  const text = [m.userFriendlyName, m.operatorGroupName, m.operatorDescription ?? "", ...phrasings]
    .filter(Boolean)
    .join(". ")
    .trim();

  const out = await extractor(text, { pooling: "mean", normalize: true });
  entries.push({
    operatorType: op.operatorType,
    name: m.userFriendlyName,
    group: m.operatorGroupName,
    description: m.operatorDescription ?? "",
    hinted: phrasings.length > 0,
    vector: Array.from(out.data).map(round),
  });

  if (++done % 40 === 0) {
    console.log(`  ${done}/${meta.operators.length}`);
  }
}

writeFileSync(OUT, JSON.stringify({ model: MODEL, dims: entries[0].vector.length, operators: entries }));

const hinted = entries.filter(entry => entry.hinted).length;
console.log(`${entries.length} operators (${hinted} with phrasings), ${entries[0].vector.length} dims -> ${OUT}`);

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

/**
 * Ranks operators by meaning rather than by name.
 *
 * The palette's keyword search only indexes `userFriendlyName`, so a query
 * phrased as an intent ("quitar duplicados") matches nothing even when an
 * operator's description says exactly that. This service compares the query
 * against a committed index of every operator's name, group and description.
 *
 * Everything runs in the browser: the operator vectors are generated offline
 * by `frontend/scripts/generate-operator-embeddings.mjs` and shipped as an
 * asset, and only the user's query is embedded at runtime. No backend call and
 * no API key are involved.
 */
import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { firstValueFrom } from "rxjs";
import { OperatorSchema } from "../../types/operator-schema.interface";

interface IndexedOperator {
  operatorType: string;
  name: string;
  group: string;
  description: string;
  vector: number[];
}

interface EmbeddingIndex {
  model: string;
  dims: number;
  operators: IndexedOperator[];
}

export interface SemanticHit {
  schema: OperatorSchema;
  score: number;
}

const INDEX_URL = "assets/operator-embeddings.json";

@Injectable({ providedIn: "root" })
export class SemanticOperatorSearchService {
  private index?: EmbeddingIndex;
  // The library types the result as a Tensor whose `data` may be any of several
  // array kinds; with `normalize` on it is always numeric, which is all we read.
  private extractor?: (text: string, opts: object) => Promise<{ data: ArrayLike<number> }>;
  // One in-flight warm-up shared by every caller, so a burst of keystrokes
  // during the first load does not start several model downloads.
  private warmup?: Promise<void>;

  constructor(private http: HttpClient) {}

  /**
   * Downloads the index and the embedding model. Safe to call repeatedly —
   * the work happens once. The model is a few tens of megabytes on first use
   * and is then served from the browser cache.
   */
  public ready(): Promise<void> {
    if (!this.warmup) {
      this.warmup = this.load();
    }
    return this.warmup;
  }

  public isReady(): boolean {
    return this.index !== undefined && this.extractor !== undefined;
  }

  private async load(): Promise<void> {
    this.index = await firstValueFrom(this.http.get<EmbeddingIndex>(INDEX_URL));

    // Imported lazily so the library and its wasm runtime stay out of the
    // initial bundle; the palette only needs them once semantic search is used.
    const transformers = await import("@xenova/transformers");
    // The model is fetched from the Hugging Face CDN, not from our own assets.
    transformers.env.allowLocalModels = false;
    const pipe = await transformers.pipeline("feature-extraction", this.index.model);
    this.extractor = pipe as unknown as typeof this.extractor;
  }

  /**
   * Returns the `limit` operators closest in meaning to `query`, restricted to
   * the schemas the palette is currently showing.
   *
   * Vectors on both sides are L2-normalised, so the dot product is already the
   * cosine similarity and lands in [-1, 1] — higher is more relevant.
   */
  public async search(
    query: string,
    available: ReadonlyArray<OperatorSchema>,
    limit: number = 3
  ): Promise<SemanticHit[]> {
    await this.ready();
    const index = this.index!;
    const extractor = this.extractor!;

    const embedded = await extractor(query, { pooling: "mean", normalize: true });
    const q = embedded.data;

    const byType = new Map(available.map(schema => [schema.operatorType, schema]));

    return index.operators
      .filter(entry => byType.has(entry.operatorType))
      .map(entry => ({
        schema: byType.get(entry.operatorType)!,
        score: dot(q, entry.vector),
      }))
      .sort((a, b) => b.score - a.score)
      .slice(0, limit);
  }
}

function dot(a: ArrayLike<number>, b: number[]): number {
  let sum = 0;
  for (let i = 0; i < b.length; i++) {
    sum += a[i] * b[i];
  }
  return sum;
}

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
import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { firstValueFrom, Subject } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { WorkflowContent } from "../../../common/type/workflow";
import { MacroExpansion } from "../../../common/util/workflow-macro";

/** Runtime projections are cached by graph semantics; compilation is the authority. */
@Injectable({ providedIn: "root" })
export class MacroExpansionService {
  public readonly changed$ = new Subject<void>();
  private readonly cache = new Map<string, MacroExpansion>();
  private readonly pending = new Map<string, Promise<MacroExpansion>>();
  constructor(private http: HttpClient) {}

  private key(content: WorkflowContent): string {
    return JSON.stringify([
      content.operators,
      content.links,
      content.macroDependencies,
      content.macroInterface,
      content.formBinding,
    ]);
  }
  public get(content: WorkflowContent): MacroExpansion | undefined {
    const key = this.key(content),
      cached = this.cache.get(key);
    if (cached) {
      this.cache.delete(key);
      this.cache.set(key, cached);
    }
    return cached;
  }
  public remember(content: WorkflowContent, expanded: MacroExpansion): void {
    const key = this.key(content);
    this.cache.delete(key);
    this.cache.set(key, expanded);
    if (this.cache.size > 20) this.cache.delete(this.cache.keys().next().value!);
    this.changed$.next();
  }
  public expand(content: WorkflowContent): Promise<MacroExpansion> {
    const cached = this.get(content);
    if (cached) return Promise.resolve(cached);
    const key = this.key(content);
    if (!this.pending.has(key)) {
      const snapshot = structuredClone(content);
      const request = firstValueFrom(
        this.http.post<MacroExpansion>(`${AppSettings.getApiEndpoint()}/compile/expand`, snapshot)
      )
        .then(expanded => {
          this.remember(snapshot, expanded);
          return expanded;
        })
        .catch(error => {
          throw new Error(error.error?.message ?? error.message ?? "Could not validate macro graph");
        })
        .finally(() => this.pending.delete(key));
      this.pending.set(key, request);
    }
    return this.pending.get(key)!;
  }
}

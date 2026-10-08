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

import { describe, expect, it } from "vitest";
import { buildFixPrompt, withoutSecrets } from "./ai-fix-prompt";

describe("withoutSecrets", () => {
  const passwordWidget = { widget: { formlyConfig: { templateOptions: { type: "password" } } } };

  it("drops a field the operator's schema renders as a password widget", () => {
    const kept = withoutSecrets({ host: "db.example.com", pw: "hunter2" }, { host: {}, pw: passwordWidget });

    expect(kept).toEqual({ host: "db.example.com" });
  });

  it("drops credential-looking names even without a schema", () => {
    const kept = withoutSecrets({
      hfApiToken: "hf_live_abc",
      password: "hunter2",
      apiKey: "sk-live",
      api_key: "sk-live",
      clientSecret: "shh",
      modelId: "Qwen/Qwen2.5",
    });

    expect(kept).toEqual({ modelId: "Qwen/Qwen2.5" });
  });

  it("keeps the ordinary settings a fix actually needs", () => {
    const kept = withoutSecrets({ modelId: "gpt-4-turb", temperature: 0, workers: 1 }, { modelId: {} });

    expect(kept).toEqual({ modelId: "gpt-4-turb", temperature: 0, workers: 1 });
  });

  it("never lets a secret reach the built prompt", () => {
    const properties = { hfApiToken: "hf_live_abc", password: "hunter2", modelId: "gpt-4-turb" };

    const prompt = buildFixPrompt("404 model not found", undefined, [], withoutSecrets(properties));

    expect(prompt).not.toContain("hf_live_abc");
    expect(prompt).not.toContain("hunter2");
    expect(prompt).toContain("gpt-4-turb");
  });
});

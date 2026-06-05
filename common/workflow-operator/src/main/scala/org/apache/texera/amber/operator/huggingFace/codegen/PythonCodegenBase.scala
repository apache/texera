/*
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

package org.apache.texera.amber.operator.huggingFace.codegen

import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext

/**
  * Builds the Python script emitted by HuggingFaceInferenceOpDesc.
  *
  * The script defines a `ProcessTableOperator` class with:
  *   - Per-instance configuration set in `open(self)` from base64-encoded
  *     values that the `pyb"..."` macro decodes at runtime (so user-input
  *     strings never appear as raw Python literals in the source).
  *   - A provider-fallback system that walks the HF Hub's inference-provider
  *     list cheapest-first and tries each provider's native chat-completions
  *     route, with HF Inference Router as the default.
  *   - A `process_table` loop that validates the prompt column, builds the
  *     per-row payload via the per-task codegen, posts to the resolved
  *     provider, and parses the response.
  *   - A `_parse_response` task switch whose branches are provided by the
  *     per-task codegen.
  *
  * Per-task variation lives in `TaskCodegen` implementations. This class
  * holds only what is shared across all HF tasks; per-task helpers (image
  * loading, audio MIME inference, media-URL fetching, etc.) will be added
  * in subsequent PRs as the corresponding task families land.
  */
object PythonCodegenBase {

  def render(ctx: CodegenContext, codegen: TaskCodegen): String = {
    val payload = codegen.payloadPython(ctx)
    val parse = codegen.parsePython(ctx)
    val hfApiToken = ctx.hfApiToken
    val modelId = ctx.modelId
    val promptColumn = ctx.promptColumn
    val resultColumn = ctx.resultColumn
    val task = ctx.task
    val systemPrompt = ctx.systemPrompt
    val maxNewTokens = ctx.safeMaxTokens
    val temperature = ctx.safeTemp
    pyb"""import os
       |import re
       |import json
       |import requests
       |import pandas as pd
       |from pytexera import *
       |
       |# Defensive format check for MODEL_ID before it is interpolated into
       |# HF URL paths. The base host is hardcoded so the worst case isn't
       |# SSRF, but rejecting `..` segments / query strings / fragments /
       |# control chars keeps the operator's request shape predictable.
       |_HF_MODEL_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*(/[A-Za-z0-9._-]+)+$$")
       |
       |class ProcessTableOperator(UDFTableOperator):
       |
       |    # Providers ranked cheapest-first (lower index = cheaper).
       |    # Unknown providers are appended at the end.
       |    PROVIDER_COST_PRIORITY = [
       |        "hf-inference",
       |        "cerebras",
       |        "sambanova",
       |        "groq",
       |        "novita",
       |        "nebius",
       |        "fireworks-ai",
       |        "together",
       |        "hyperbolic",
       |        "scaleway",
       |        "nscale",
       |        "ovhcloud",
       |        "deepinfra",
       |        "featherless-ai",
       |        "baseten",
       |        "publicai",
       |        "nvidia",
       |        "openai",
       |        "cohere",
       |        "clarifai",
       |    ]
       |
       |    # Per-provider chat-completions route overrides. Providers not listed
       |    # here use the default `v1/chat/completions` path. Single source of
       |    # truth for both _post_with_fallback (text-gen) and _call_provider
       |    # (OpenAI-compatible fallback) so the two stay in sync as providers
       |    # are added.
       |    CHAT_ROUTES = {
       |        "groq": "openai/v1/chat/completions",
       |        "fireworks-ai": "inference/v1/chat/completions",
       |        "cohere": "compatibility/v1/chat/completions",
       |        "clarifai": "v2/ext/openai/v1/chat/completions",
       |        "deepinfra": "v1/openai/chat/completions",
       |    }
       |
       |    # Third-party providers that speak the OpenAI chat-completions
       |    # protocol. Used by _call_provider's OpenAI-compatible branch.
       |    OPENAI_COMPATIBLE_PROVIDERS = (
       |        "cerebras", "sambanova", "groq", "novita", "nebius",
       |        "fireworks-ai", "together", "hyperbolic", "cohere", "clarifai",
       |        "deepinfra", "featherless-ai", "nscale", "nvidia", "openai",
       |        "ovhcloud", "publicai", "scaleway", "baseten",
       |    )
       |
       |    def open(self):
       |        # User-provided strings reach the operator via base64-encoded
       |        # decode expressions so they cannot break Python syntax or
       |        # leak raw text into the generated source.
       |        self.HF_API_TOKEN = $hfApiToken
       |        self.MODEL_ID = $modelId
       |        self.PROMPT_COLUMN = $promptColumn
       |        self.RESULT_COLUMN = $resultColumn
       |        self.TASK = $task
       |        self.SYSTEM_PROMPT = $systemPrompt
       |        self.MAX_NEW_TOKENS = $maxNewTokens
       |        self.TEMPERATURE = $temperature
       |
       |    def _resolve_providers(self, token):
       |        '''Query the HF Hub API for inference providers serving this model.
       |        Returns a list of dicts with 'name' and 'providerId' sorted
       |        cheapest-first. Falls back to hf-inference if anything goes wrong.
       |        '''
       |        try:
       |            resp = requests.get(
       |                f"https://huggingface.co/api/models/{self.MODEL_ID}",
       |                headers={"Authorization": f"Bearer {token}"},
       |                params={"expand[]": "inferenceProviderMapping"},
       |                timeout=30,
       |            )
       |            if resp.status_code == 200:
       |                data = resp.json()
       |                mapping = (
       |                    data.get("inferenceProviderMapping")
       |                    or data.get("inference_provider_mapping")
       |                    or {}
       |                )
       |                if mapping:
       |                    live = [
       |                        {
       |                            "name": p,
       |                            "providerId": v.get("providerId", self.MODEL_ID),
       |                            "task": v.get("task", ""),
       |                            "isModelAuthor": v.get("isModelAuthor", False),
       |                        }
       |                        for p, v in mapping.items()
       |                        if isinstance(v, dict) and v.get("status") == "live"
       |                    ]
       |                    if live:
       |                        priority = {name: idx for idx, name in enumerate(self.PROVIDER_COST_PRIORITY)}
       |                        live.sort(key=lambda prov: priority.get(prov["name"], len(self.PROVIDER_COST_PRIORITY)))
       |                        return live
       |        except Exception:
       |            pass
       |        return [{"name": "hf-inference", "providerId": self.MODEL_ID}]
       |
       |    def _post_with_fallback(self, providers, json_headers, pipeline_payload, prompt_value):
       |        '''Try providers in order, using the correct API route for each.
       |        Returns (response, provider_summary). provider_summary is None on
       |        success or a string describing what failed.
       |        '''
       |        RETRYABLE = (400, 404, 422, 429, 502, 503)
       |        last_resp = None
       |        errors = []
       |        for prov in providers:
       |            provider_name = prov["name"]
       |            provider_id = prov["providerId"]
       |            try:
       |                if self.TASK == "text-generation":
       |                    route = self.CHAT_ROUTES.get(provider_name, "v1/chat/completions")
       |                    url = f"https://router.huggingface.co/{provider_name}/{route}"
       |                    resp = requests.post(url, headers=json_headers, json=pipeline_payload, timeout=120)
       |                elif provider_name == "hf-inference":
       |                    url = f"https://router.huggingface.co/hf-inference/models/{self.MODEL_ID}"
       |                    resp = requests.post(url, headers=json_headers, json=pipeline_payload, timeout=120)
       |                else:
       |                    resp = self._call_provider(provider_name, provider_id, json_headers, pipeline_payload, prompt_value)
       |            except Exception as e:
       |                errors.append(f"{provider_name}: {type(e).__name__}")
       |                continue
       |            if resp.status_code in (200, 201):
       |                return resp, None
       |            if resp.status_code == 401:
       |                return resp, None
       |            try:
       |                detail = resp.json().get("error", resp.text[:200])
       |            except Exception:
       |                detail = resp.text[:200] if resp.text else "no details"
       |            errors.append(f"{provider_name}: HTTP {resp.status_code} - {detail}")
       |            last_resp = resp
       |            if resp.status_code not in RETRYABLE:
       |                return resp, "; ".join(errors)
       |        summary = "; ".join(errors) if errors else "no providers available"
       |        return last_resp, summary
       |
       |    def _call_provider(self, provider_name, provider_id, json_headers, pipeline_payload, prompt_value):
       |        '''Route to a third-party provider using its native API format.
       |        For the text-gen-only build this covers the OpenAI-compatible chat
       |        providers and an unknown-provider fallback that tries the pipeline
       |        format then chat completions. Image / audio / media routing will
       |        be added in subsequent PRs alongside the corresponding task
       |        codegens.
       |        '''
       |        base = f"https://router.huggingface.co/{provider_name}"
       |        if provider_name in self.OPENAI_COMPATIBLE_PROVIDERS:
       |            url = f"{base}/{self.CHAT_ROUTES.get(provider_name, 'v1/chat/completions')}"
       |            messages = [{"role": "user", "content": prompt_value}]
       |            return requests.post(
       |                url,
       |                headers=json_headers,
       |                json={"model": provider_id, "messages": messages},
       |                timeout=120,
       |            )
       |
       |        # Unknown provider: try pipeline format, then chat completions.
       |        url = f"{base}/{provider_id}"
       |        resp = requests.post(url, headers=json_headers, json=pipeline_payload, timeout=120)
       |        if resp.status_code in (400, 404, 422):
       |            url = f"{base}/v1/chat/completions"
       |            messages = [{"role": "user", "content": prompt_value}]
       |            resp2 = requests.post(
       |                url,
       |                headers=json_headers,
       |                json={"model": provider_id, "messages": messages},
       |                timeout=120,
       |            )
       |            if resp2.status_code == 200:
       |                return resp2
       |        return resp
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        prompt_col = self.PROMPT_COLUMN
       |        result_col = self.RESULT_COLUMN
       |        task = self.TASK
       |
       |        # --- validate MODEL_ID format before any HF URL is built ---
       |        if not _HF_MODEL_ID_PATTERN.match(self.MODEL_ID or ""):
       |            raise ValueError(
       |                f"Invalid Hugging Face model ID '{self.MODEL_ID}'. "
       |                f"Expected format like 'org/model-name' or 'org/model-name/revision'."
       |            )
       |
       |        # --- resolve API token ---
       |        token = self.HF_API_TOKEN if self.HF_API_TOKEN else os.environ.get("HF_TOKEN", "")
       |        if not token:
       |            raise ValueError(
       |                "Hugging Face API token is not set. "
       |                "Provide it in the operator config or via HF_TOKEN env var."
       |            )
       |
       |        # --- resolve all available inference providers for this model (tried in order) ---
       |        providers = self._resolve_providers(token)
       |
       |        # --- validate prompt column exists ---
       |        assert prompt_col in table.columns, (
       |            f"Prompt column '{prompt_col}' not found in input table. "
       |            f"Available columns: {list(table.columns)}"
       |        )
       |
       |        # --- handle empty table ---
       |        if table.empty:
       |            table[result_col] = pd.Series(dtype="object")
       |            yield table
       |            return
       |
       |        json_headers = {
       |            "Authorization": f"Bearer {token}",
       |            "Content-Type": "application/json",
       |        }
       |
       |        results = []
       |        for idx, row in table.iterrows():
       |            prompt_value = row[prompt_col]
       |            if pd.isna(prompt_value):
       |                prompt_value = ""
       |            else:
       |                prompt_value = str(prompt_value)
       |
       |            # --- build task-specific payload (provided by per-task codegen) ---
       |${payload}
       |
       |            try:
       |                resp, provider_summary = self._post_with_fallback(
       |                    providers, json_headers, payload, prompt_value
       |                )
       |
       |                if resp is None:
       |                    results.append(
       |                        self._format_error(
       |                            "All inference providers failed",
       |                            f"No provider could serve model '{self.MODEL_ID}'. "
       |                            f"Tried: {provider_summary}"
       |                        )
       |                    )
       |                    continue
       |
       |                if resp.status_code == 429:
       |                    results.append(
       |                        self._format_http_error(
       |                            "HF API rate limit hit, retry later", resp.status_code, resp.text
       |                        )
       |                    )
       |                    continue
       |                if resp.status_code == 401:
       |                    results.append(
       |                        self._format_http_error("Invalid HF API token", resp.status_code, resp.text)
       |                    )
       |                    continue
       |                if resp.status_code not in (200, 201):
       |                    results.append(
       |                        self._format_error(
       |                            "All inference providers failed",
       |                            f"No provider could serve model '{self.MODEL_ID}'. "
       |                            f"Tried: {provider_summary}"
       |                        )
       |                    )
       |                    continue
       |
       |                try:
       |                    body = resp.json()
       |                except ValueError:
       |                    body = resp.text
       |                content = self._parse_response(body)
       |                results.append(content)
       |
       |            except Exception as e:
       |                import warnings
       |                warnings.warn(
       |                    f"Row {idx}: request failed ({type(e).__name__}: {e}), "
       |                    f"setting result to readable error text."
       |                )
       |                results.append(self._format_error("Request failed", f"{type(e).__name__}: {e}"))
       |
       |        table[result_col] = results
       |        yield table
       |
       |    def _format_error(self, title, detail):
       |        return f"{title}: {detail}"
       |
       |    def _format_http_error(self, title, status_code, response_text):
       |        # Cap at 200 chars to match the truncation in _post_with_fallback's
       |        # error-detail extraction; a large body / HTML error page would
       |        # otherwise land verbatim in the result cell.
       |        detail = response_text.strip()[:200]
       |        if not detail:
       |            detail = "<empty response>"
       |        return f"{title} [status={status_code}] response={detail}"
       |
       |    def _parse_response(self, body):
       |        task = self.TASK
       |        try:
       |            if isinstance(body, str):
       |                return body
       |${parse}
       |            else:
       |                return json.dumps(body)
       |        except (KeyError, IndexError, TypeError):
       |            return json.dumps(body)
       |""".encode
  }
}

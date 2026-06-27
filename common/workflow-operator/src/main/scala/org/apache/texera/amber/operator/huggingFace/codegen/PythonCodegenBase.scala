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
    val imageInput = ctx.imageInput
    val inputImageColumn = ctx.inputImageColumn
    val audioInput = ctx.audioInput
    val inputAudioColumn = ctx.inputAudioColumn
    val contextColumn = ctx.contextColumn
    val candidateLabels = ctx.candidateLabels
    val sentencesColumn = ctx.sentencesColumn
    pyb"""import os
       |import re
       |import json
       |import base64
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
       |    # Hard cap on bytes pulled from an external (user/response-provided) URL.
       |    MAX_REMOTE_FETCH_BYTES = 25 * 1024 * 1024
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
       |        self.IMAGE_INPUT = $imageInput
       |        self.INPUT_IMAGE_COLUMN = $inputImageColumn
       |        self.AUDIO_INPUT = $audioInput
       |        self.INPUT_AUDIO_COLUMN = $inputAudioColumn
       |        self.CONTEXT_COLUMN = $contextColumn
       |        self.CANDIDATE_LABELS = $candidateLabels
       |        self.SENTENCES_COLUMN = $sentencesColumn
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
       |    def _post_with_fallback(self, providers, json_headers, raw_binary_headers, pipeline_payload, use_raw_binary_body, prompt_value):
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
       |            is_model_author = prov.get("isModelAuthor", False)
       |            prov_task = prov.get("task", "")
       |            try:
       |                if self.TASK in ("text-generation", "image-text-to-text"):
       |                    route = self.CHAT_ROUTES.get(provider_name, "v1/chat/completions")
       |                    url = f"https://router.huggingface.co/{provider_name}/{route}"
       |                    resp = requests.post(url, headers=json_headers, json=pipeline_payload, timeout=120)
       |                elif is_model_author and prov_task in ("image-to-text", "image-text-to-text") and provider_name not in ("zai-org",):
       |                    url = f"https://router.huggingface.co/{provider_name}/v1/chat/completions"
       |                    img_b64 = ""
       |                    if use_raw_binary_body and isinstance(pipeline_payload, bytes):
       |                        img_b64 = base64.b64encode(pipeline_payload).decode("utf-8")
       |                    chat_payload = {
       |                        "model": provider_id,
       |                        "messages": [{
       |                            "role": "user",
       |                            "content": [
       |                                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img_b64}"}} if img_b64 else None,
       |                                {"type": "text", "text": prompt_value if prompt_value else "What is in this image?"},
       |                            ],
       |                        }],
       |                    }
       |                    chat_payload["messages"][0]["content"] = [c for c in chat_payload["messages"][0]["content"] if c is not None]
       |                    resp = requests.post(url, headers=json_headers, json=chat_payload, timeout=120)
       |                elif provider_name == "hf-inference":
       |                    url = f"https://router.huggingface.co/hf-inference/models/{self.MODEL_ID}"
       |                    if use_raw_binary_body:
       |                        resp = requests.post(url, headers=raw_binary_headers, data=pipeline_payload, timeout=120)
       |                    else:
       |                        resp = requests.post(url, headers=json_headers, json=pipeline_payload, timeout=120)
       |                else:
       |                    resp = self._call_provider(provider_name, provider_id, json_headers, raw_binary_headers, pipeline_payload, use_raw_binary_body, prompt_value)
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
       |    def _call_provider(self, provider_name, provider_id, json_headers, raw_binary_headers, pipeline_payload, use_raw_binary_body, prompt_value):
       |        '''Route to a third-party provider using its native API format.
       |        Handles OpenAI-compatible chat providers for text-gen, zai-org's
       |        custom API, Replicate / Fal-ai / Wavespeed for media-generation
       |        and image-to-image, and an unknown-provider fallback that tries
       |        the pipeline format then chat completions.
       |        '''
       |        base = f"https://router.huggingface.co/{provider_name}"
       |        task = self.TASK
       |        img_b64 = ""
       |        if use_raw_binary_body and isinstance(pipeline_payload, bytes):
       |            img_b64 = base64.b64encode(pipeline_payload).decode("utf-8")
       |        elif isinstance(pipeline_payload, dict):
       |            # Image+prompt tasks (visual-question-answering, document-question-
       |            # answering, zero-shot-image-classification) build dict payloads
       |            # with use_raw_binary_body=False, so the raw-bytes extraction above
       |            # doesn't fire. Without this branch, when one of those tasks routes
       |            # to a third-party provider (replicate / fal-ai / wavespeed /
       |            # OpenAI-compatible / unknown-fallback) the image is silently
       |            # dropped and only prompt_value is sent — they happen to work only
       |            # on hf-inference, where the dict goes through as JSON. Surfacing
       |            # img_b64 here keeps the provider-specific branches below image-
       |            # aware without each branch needing to know the dict shape.
       |            inputs = pipeline_payload.get("inputs")
       |            if isinstance(inputs, dict) and isinstance(inputs.get("image"), str):
       |                img_b64 = inputs["image"]
       |            elif task == "zero-shot-image-classification" and isinstance(inputs, str):
       |                img_b64 = inputs
       |
       |        # zai-org: custom /api/paas/v4/ surface.
       |        if provider_name == "zai-org":
       |            zai_headers = {**json_headers, "x-source-channel": "hugging_face", "accept-language": "en-US,en"}
       |            if task in ("image-to-text", "image-text-to-text"):
       |                url = f"{base}/api/paas/v4/layout_parsing"
       |                file_data = f"data:image/png;base64,{img_b64}" if img_b64 else ""
       |                return requests.post(url, headers=zai_headers, json={"model": provider_id, "file": file_data}, timeout=120)
       |            url = f"{base}/api/paas/v4/chat/completions"
       |            messages = [{"role": "user", "content": prompt_value}]
       |            if img_b64:
       |                messages = [{"role": "user", "content": [
       |                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img_b64}"}},
       |                    {"type": "text", "text": prompt_value if prompt_value else "What is in this image?"},
       |                ]}]
       |            return requests.post(url, headers=zai_headers, json={"model": provider_id, "messages": messages}, timeout=120)
       |
       |        # Replicate: synchronous predictions endpoint with polling fallback.
       |        if provider_name == "replicate":
       |            url = f"{base}/v1/models/{provider_id}/predictions"
       |            hdrs = {**json_headers, "Prefer": "wait"}
       |            if task == "text-to-speech":
       |                inp = {"text": prompt_value}
       |            elif task in ("text-to-image", "text-to-video"):
       |                inp = {"prompt": prompt_value}
       |            elif task in ("automatic-speech-recognition", "audio-classification") and img_b64:
       |                audio_content_type = raw_binary_headers.get("Content-Type", "audio/mpeg")
       |                inp = {"audio": f"data:{audio_content_type};base64,{img_b64}"}
       |            elif task == "image-to-image" and img_b64:
       |                data_url = f"data:image/png;base64,{img_b64}"
       |                inp = {"image": data_url, "images": [data_url], "input_image": data_url, "prompt": prompt_value}
       |            elif img_b64:
       |                inp = {"image": f"data:image/png;base64,{img_b64}", "prompt": prompt_value}
       |            else:
       |                inp = {"prompt": prompt_value}
       |            resp = requests.post(url, headers=hdrs, json={"input": inp}, timeout=120)
       |            if resp.status_code == 202:
       |                import time as _time
       |                pred = resp.json()
       |                poll_url = pred.get("urls", {}).get("get", "")
       |                if not poll_url:
       |                    return resp
       |                from urllib.parse import urlparse as _urlparse
       |                poll_path = _urlparse(poll_url).path
       |                poll_url = f"{base}{poll_path}"
       |                # Worst case: 300 polls × 2s = ~10 minutes per row before we give
       |                # up. Sized for text-to-video which legitimately takes minutes on
       |                # Replicate. process_table is synchronous, so emit a progress
       |                # line every 30 polls (~1 min) to distinguish slow work from a
       |                # hang in the worker log.
       |                for poll_idx in range(300):
       |                    _time.sleep(2)
       |                    poll_resp = requests.get(poll_url, headers=json_headers, timeout=30)
       |                    if poll_resp.status_code != 200:
       |                        continue
       |                    status = poll_resp.json().get("status", "")
       |                    if status == "succeeded":
       |                        return poll_resp
       |                    if status in ("failed", "canceled"):
       |                        # The polling HTTP request itself returned 200, but the
       |                        # Replicate prediction terminally failed. Without this
       |                        # branch, process_table would treat the 200 as success
       |                        # and emit json.dumps(body) (raw error JSON) into the
       |                        # output cell. Convert to a synthetic 502 so
       |                        # _post_with_fallback's non-200 handler surfaces the
       |                        # actual failure detail via _format_error.
       |                        body_json = poll_resp.json() if poll_resp.text else {}
       |                        detail = (body_json.get("error") or body_json.get("logs") or status) \
       |                            if isinstance(body_json, dict) else status
       |                        poll_resp.status_code = 502
       |                        poll_resp._content = json.dumps({
       |                            "error": f"Replicate prediction {status}: {detail}"
       |                        }).encode("utf-8")
       |                        return poll_resp
       |                    if (poll_idx + 1) % 30 == 0:
       |                        print(f"[hf] Replicate still running for model '{self.MODEL_ID}' after {(poll_idx + 1) * 2}s; will wait up to 600s.")
       |                return poll_resp
       |            return resp
       |
       |        # Fal-ai: per-model endpoint.
       |        if provider_name == "fal-ai":
       |            url = f"{base}/{provider_id}"
       |            if task == "text-to-speech":
       |                return requests.post(url, headers=json_headers, json={"text": prompt_value}, timeout=120)
       |            if task in ("text-to-image", "text-to-video"):
       |                return requests.post(url, headers=json_headers, json={"prompt": prompt_value}, timeout=120)
       |            if task == "image-to-image" and img_b64:
       |                data_url = f"data:image/png;base64,{img_b64}"
       |                return requests.post(url, headers=json_headers, json={"image_url": data_url, "image_urls": [data_url], "prompt": prompt_value}, timeout=120)
       |            if img_b64:
       |                return requests.post(url, headers=json_headers, json={"image_url": f"data:image/png;base64,{img_b64}", "prompt": prompt_value}, timeout=120)
       |            return requests.post(url, headers=json_headers, json={"prompt": prompt_value}, timeout=120)
       |
       |        # Wavespeed: async submit + poll.
       |        if provider_name == "wavespeed":
       |            url = f"{base}/api/v3/{provider_id}"
       |            payload = {"prompt": prompt_value}
       |            if img_b64:
       |                payload["image"] = img_b64
       |                payload["images"] = [img_b64]
       |            submit_resp = requests.post(url, headers=json_headers, json=payload, timeout=120)
       |            if submit_resp.status_code not in (200, 201):
       |                return submit_resp
       |            get_path = submit_resp.json().get("data", {}).get("urls", {}).get("get", "")
       |            if not get_path:
       |                return submit_resp
       |            from urllib.parse import urlparse as _urlparse
       |            result_url = f"{base}{_urlparse(get_path).path}"
       |            import time as _time
       |            poll_resp = submit_resp
       |            # Worst case: 120 polls × 1s = ~2 minutes per row. Emit a progress
       |            # line every 30 polls (~30 s) so the worker log distinguishes slow
       |            # work from a hang.
       |            for poll_idx in range(120):
       |                _time.sleep(1)
       |                poll_resp = requests.get(result_url, headers=json_headers, timeout=30)
       |                if poll_resp.status_code != 200:
       |                    continue
       |                body_json = poll_resp.json() if poll_resp.text else {}
       |                data_obj = body_json.get("data", {}) if isinstance(body_json, dict) else {}
       |                status = data_obj.get("status", "") if isinstance(data_obj, dict) else ""
       |                if status == "completed":
       |                    return poll_resp
       |                if status == "failed":
       |                    # Same shape as Replicate: HTTP 200 + body says "failed".
       |                    # Synthesize a 502 so _post_with_fallback's non-200 handler
       |                    # reports the actual reason instead of process_table
       |                    # parsing the success-shaped body and writing raw error
       |                    # JSON into the result cell.
       |                    detail = (
       |                        (data_obj.get("error") if isinstance(data_obj, dict) else None)
       |                        or (body_json.get("error") if isinstance(body_json, dict) else None)
       |                        or "failed"
       |                    )
       |                    poll_resp.status_code = 502
       |                    poll_resp._content = json.dumps({
       |                        "error": f"Wavespeed job failed: {detail}"
       |                    }).encode("utf-8")
       |                    return poll_resp
       |                if (poll_idx + 1) % 30 == 0:
       |                    print(f"[hf] Wavespeed still running for model '{self.MODEL_ID}' after {poll_idx + 1}s; will wait up to 120s.")
       |            return poll_resp
       |
       |        if provider_name in self.OPENAI_COMPATIBLE_PROVIDERS:
       |            if task == "text-to-image":
       |                url = f"{base}/v1/images/generations"
       |                return requests.post(url, headers=json_headers, json={"model": provider_id, "prompt": prompt_value}, timeout=120)
       |            if task == "text-to-speech":
       |                url = f"{base}/v1/audio/speech"
       |                return requests.post(url, headers=json_headers, json={"model": provider_id, "input": prompt_value}, timeout=120)
       |            url = f"{base}/{self.CHAT_ROUTES.get(provider_name, 'v1/chat/completions')}"
       |            messages = [{"role": "user", "content": prompt_value}]
       |            if img_b64:
       |                messages = [{"role": "user", "content": [
       |                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img_b64}"}},
       |                    {"type": "text", "text": prompt_value if prompt_value else "What is in this image?"},
       |                ]}]
       |            return requests.post(
       |                url,
       |                headers=json_headers,
       |                json={"model": provider_id, "messages": messages},
       |                timeout=120,
       |            )
       |
       |        # Unknown provider: try pipeline format, then chat completions.
       |        url = f"{base}/{provider_id}"
       |        if use_raw_binary_body:
       |            resp = requests.post(url, headers=raw_binary_headers, data=pipeline_payload, timeout=120)
       |        else:
       |            resp = requests.post(url, headers=json_headers, json=pipeline_payload, timeout=120)
       |        if resp.status_code in (400, 404, 422):
       |            url = f"{base}/v1/chat/completions"
       |            messages = [{"role": "user", "content": prompt_value}]
       |            if img_b64:
       |                messages = [{"role": "user", "content": [
       |                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img_b64}"}},
       |                    {"type": "text", "text": prompt_value if prompt_value else "Describe this image."},
       |                ]}]
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
       |        image_only_tasks = ("image-classification", "object-detection", "image-segmentation", "image-to-text")
       |        image_prompt_tasks = ("visual-question-answering", "document-question-answering", "zero-shot-image-classification", "image-text-to-text", "image-to-image")
       |        image_tasks = image_only_tasks + image_prompt_tasks
       |        audio_only_tasks = ("automatic-speech-recognition", "audio-classification")
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
       |        # --- validate prompt column exists (skipped for image tasks and binary-only audio tasks) ---
       |        if task not in image_tasks and task not in audio_only_tasks:
       |            assert prompt_col in table.columns, (
       |                f"Prompt column '{prompt_col}' not found in input table. "
       |                f"Available columns: {list(table.columns)}"
       |            )
       |        if task == "zero-shot-classification":
       |            labels = [l.strip() for l in str(self.CANDIDATE_LABELS).split(",") if l.strip()]
       |            assert labels, (
       |                "Candidate Labels are required for zero-shot-classification. "
       |                "Provide a comma-separated list of labels."
       |            )
       |        if task == "question-answering":
       |            ctx_col = self.CONTEXT_COLUMN
       |            assert ctx_col and ctx_col in table.columns, (
       |                f"Context column '{ctx_col}' not found in input table. "
       |                f"Available columns: {list(table.columns)}"
       |            )
       |        if task in ("sentence-similarity", "text-ranking"):
       |            sent_col = self.SENTENCES_COLUMN
       |            assert sent_col and sent_col in table.columns, (
       |                f"Sentences column '{sent_col}' not found in input table. "
       |                f"Available columns: {list(table.columns)}"
       |            )
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
       |        image_headers = {
       |            "Authorization": f"Bearer {token}",
       |            "Content-Type": "application/octet-stream",
       |        }
       |        # --- pre-compute table dict for table-question-answering ---
       |        table_dict = None
       |        if task == "table-question-answering":
       |            table_dict = {}
       |            for col in table.columns:
       |                if col != prompt_col and col != result_col:
       |                    table_dict[col] = [
       |                        str(v) if not pd.isna(v) else "" for v in table[col].tolist()
       |                    ]
       |
       |        # --- resolve image source (upload or column) for image tasks ---
       |        has_image_upload = bool(self.IMAGE_INPUT) and bool(str(self.IMAGE_INPUT).strip())
       |        use_image_column = not has_image_upload and bool(self.INPUT_IMAGE_COLUMN) and self.INPUT_IMAGE_COLUMN in table.columns
       |        image_bytes = None
       |        image_error = None
       |        has_audio_upload = bool(self.AUDIO_INPUT) and bool(str(self.AUDIO_INPUT).strip())
       |        use_audio_column = not has_audio_upload and bool(self.INPUT_AUDIO_COLUMN) and self.INPUT_AUDIO_COLUMN in table.columns
       |        audio_headers = {
       |            "Authorization": f"Bearer {token}",
       |            "Content-Type": "application/octet-stream" if use_audio_column else self._get_audio_content_type(),
       |        }
       |        audio_bytes = None
       |        audio_error = None
       |        if task in image_tasks and not use_image_column:
       |            if not has_image_upload:
       |                image_error = "No image source. Set an Input Image Column or upload an image."
       |            else:
       |                try:
       |                    image_bytes = self._read_image_input()
       |                except Exception as e:
       |                    image_error = f"Could not read image input ({type(e).__name__}: {e})"
       |        if task in audio_only_tasks and not use_audio_column:
       |            if not has_audio_upload:
       |                audio_error = "No audio source. Set an Input Audio Column or upload audio."
       |            else:
       |                try:
       |                    audio_bytes = self._read_audio_input()
       |                except Exception as e:
       |                    audio_error = f"Could not read audio input ({type(e).__name__}: {e})"
       |
       |        results = []
       |        for idx, row in table.iterrows():
       |            if image_error is not None:
       |                results.append(self._format_error("Image task configuration error", image_error))
       |                continue
       |            if audio_error is not None:
       |                results.append(self._format_error("Audio task configuration error", audio_error))
       |                continue
       |
       |            if task in image_only_tasks:
       |                prompt_value = ""
       |            elif task in audio_only_tasks:
       |                prompt_value = ""
       |            elif task in image_prompt_tasks and prompt_col not in table.columns:
       |                prompt_value = "What is shown in this image?"
       |            else:
       |                prompt_value = row[prompt_col]
       |                if pd.isna(prompt_value):
       |                    prompt_value = ""
       |                else:
       |                    prompt_value = str(prompt_value)
       |
       |            # --- resolve per-row image bytes from column ---
       |            current_image_bytes = image_bytes
       |            if task in image_tasks and use_image_column:
       |                try:
       |                    raw = self._read_binary_value(row[self.INPUT_IMAGE_COLUMN])
       |                    if raw is None:
       |                        results.append(self._format_error("Image data error", f"Row {idx}: image column is empty"))
       |                        continue
       |                    current_image_bytes = self._compress_image_bytes(raw)
       |                except Exception as e:
       |                    results.append(self._format_error("Image data error", f"Row {idx}: {type(e).__name__}: {e}"))
       |                    continue
       |
       |            # --- resolve per-row audio bytes from column ---
       |            current_audio_bytes = audio_bytes
       |            if task in audio_only_tasks and use_audio_column:
       |                try:
       |                    current_audio_bytes = self._read_binary_value(row[self.INPUT_AUDIO_COLUMN])
       |                    if current_audio_bytes is None:
       |                        results.append(self._format_error("Audio data error", f"Row {idx}: audio column is empty"))
       |                        continue
       |                except Exception as e:
       |                    results.append(self._format_error("Audio data error", f"Row {idx}: {type(e).__name__}: {e}"))
       |                    continue
       |
       |            # --- build task-specific payload (provided by per-task codegen) ---
       |            use_raw_binary_body = False
       |            raw_binary_headers = image_headers
       |${payload}
       |
       |            try:
       |                resp, provider_summary = self._post_with_fallback(
       |                    providers, json_headers, raw_binary_headers, payload, use_raw_binary_body, prompt_value
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
       |                content_type = resp.headers.get("Content-Type", "")
       |                if content_type.startswith("image/"):
       |                    b64 = base64.b64encode(resp.content).decode("utf-8")
       |                    results.append(f"data:{content_type};base64,{b64}")
       |                    continue
       |                if content_type.startswith("audio/") or content_type.startswith("video/"):
       |                    b64 = base64.b64encode(resp.content).decode("utf-8")
       |                    results.append(f"data:{content_type};base64,{b64}")
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
       |    # ──────────────────────────────────────────────────────────────────
       |    # Image-task helpers (used by ImageTaskCodegen and image-related
       |    # branches of _call_provider).
       |    # ──────────────────────────────────────────────────────────────────
       |
       |    def _fetch_remote_url(self, url):
       |        '''Fetch an external URL with SSRF hardening. Returns (content_type, data).
       |        Enforces https-only, rejects private/loopback/link-local/reserved
       |        addresses (covers the 169.254.169.254 cloud-metadata endpoint), and
       |        caps the response at MAX_REMOTE_FETCH_BYTES. The address check runs
       |        before the request, so it mitigates but does not fully prevent DNS
       |        rebinding (requests re-resolves on connect).
       |        '''
       |        import ipaddress
       |        import socket
       |        from urllib.parse import urlparse as _urlparse
       |        parsed = _urlparse(url)
       |        if parsed.scheme != "https":
       |            raise ValueError(f"Only https URLs are allowed (got scheme '{parsed.scheme}').")
       |        host = parsed.hostname
       |        if not host:
       |            raise ValueError("Remote URL has no host.")
       |        try:
       |            addrinfos = socket.getaddrinfo(host, parsed.port or 443, proto=socket.IPPROTO_TCP)
       |        except socket.gaierror as e:
       |            raise ValueError(f"Could not resolve host '{host}': {e}")
       |        for info in addrinfos:
       |            ip = ipaddress.ip_address(info[4][0])
       |            if (ip.is_private or ip.is_loopback or ip.is_link_local
       |                    or ip.is_reserved or ip.is_multicast or ip.is_unspecified):
       |                raise ValueError(f"Refusing to fetch from non-public address {ip}.")
       |        resp = requests.get(url, timeout=120, stream=True)
       |        resp.raise_for_status()
       |        content_type = resp.headers.get("Content-Type", "")
       |        total = 0
       |        chunks = []
       |        for chunk in resp.iter_content(65536):
       |            total += len(chunk)
       |            if total > self.MAX_REMOTE_FETCH_BYTES:
       |                resp.close()
       |                raise ValueError(
       |                    f"Remote file exceeds the {self.MAX_REMOTE_FETCH_BYTES} byte limit."
       |                )
       |            chunks.append(chunk)
       |        return content_type, b"".join(chunks)
       |
       |    def _read_image_input(self):
       |        image_input = str(self.IMAGE_INPUT or "").strip()
       |        if image_input.startswith("data:"):
       |            _, encoded = image_input.split(",", 1)
       |            return base64.b64decode(encoded)
       |        if image_input.startswith("http://") or image_input.startswith("https://"):
       |            _, data = self._fetch_remote_url(image_input)
       |            return data
       |        # Reading arbitrary worker-filesystem paths is intentionally NOT
       |        # supported: a workflow could otherwise point this at any file on the
       |        # worker (e.g. /etc/passwd) and exfiltrate it via the inference call.
       |        # Uploaded images arrive as data URLs; remote images as https URLs.
       |        raise ValueError(
       |            "Unsupported image input. Upload an image (sent as a data URL) "
       |            "or provide a public https image URL."
       |        )
       |
       |    def _compress_image_bytes(self, image_bytes, max_bytes=33000):
       |        from io import BytesIO
       |        from PIL import Image as PILImage
       |        if len(image_bytes) <= max_bytes:
       |            return image_bytes
       |        try:
       |            img = PILImage.open(BytesIO(image_bytes))
       |            img = img.convert("RGB")
       |            max_dim = 512
       |            quality = 75
       |            while max_dim >= 160:
       |                scale = min(1, max_dim / max(img.width, img.height))
       |                w = max(1, round(img.width * scale))
       |                h = max(1, round(img.height * scale))
       |                resized = img.resize((w, h), PILImage.LANCZOS)
       |                q = quality
       |                while q >= 35:
       |                    buf = BytesIO()
       |                    resized.save(buf, format="JPEG", quality=q)
       |                    if buf.tell() <= max_bytes:
       |                        return buf.getvalue()
       |                    q -= 10
       |                max_dim = int(max_dim * 0.75)
       |            buf = BytesIO()
       |            resized.save(buf, format="JPEG", quality=35)
       |            return buf.getvalue()
       |        except Exception:
       |            return image_bytes
       |
       |    def _image_input_as_base64(self, image_bytes):
       |        return base64.b64encode(image_bytes).decode("utf-8")
       |
       |    def _read_audio_input(self):
       |        audio_input = str(self.AUDIO_INPUT or "").strip()
       |        if audio_input.startswith("data:"):
       |            _, encoded = audio_input.split(",", 1)
       |            return base64.b64decode(encoded)
       |        if audio_input.startswith("http://") or audio_input.startswith("https://"):
       |            _, data = self._fetch_remote_url(audio_input)
       |            return data
       |        # Reading arbitrary worker-filesystem paths is intentionally NOT
       |        # supported: uploaded audio arrives as a data URL and remote audio
       |        # must be fetched through the hardened https-only helper above.
       |        raise ValueError(
       |            "Unsupported audio input. Upload an audio file (sent as a data URL) "
       |            "or provide a public https audio URL."
       |        )
       |
       |    def _read_binary_value(self, value):
       |        if value is None:
       |            return None
       |        if isinstance(value, bytes):
       |            return value
       |        # Treat scalar pandas/numpy missing sentinels (NaN, pd.NA, NaT) as empty.
       |        # isinstance(value, float) only catches float('nan'); pd.NA / NaT are not
       |        # floats and would otherwise be str()-ified into "<NA>"/"NaT" bytes. Guard
       |        # pd.isna against non-scalar inputs, where it returns an array and `if`
       |        # raises on an ambiguous truth value.
       |        try:
       |            if pd.isna(value):
       |                return None
       |        except (TypeError, ValueError):
       |            pass
       |        val = str(value).strip()
       |        if not val:
       |            return None
       |        if self._looks_like_html(val):
       |            return self._html_to_image_bytes(val)
       |        if val.startswith("data:"):
       |            _, encoded = val.split(",", 1)
       |            return base64.b64decode(encoded)
       |        if val.startswith("http://") or val.startswith("https://"):
       |            _, data = self._fetch_remote_url(val)
       |            return data
       |        # No worker-filesystem path reads here either (see _read_image_input):
       |        # a column value must be a data URL, http(s) URL, rendered HTML, or
       |        # base64-encoded bytes. Anything else is treated as raw bytes, never
       |        # as a path to open.
       |        try:
       |            return base64.b64decode(val)
       |        except Exception:
       |            return val.encode("utf-8")
       |
       |    def _looks_like_html(self, val):
       |        s = val.lstrip()[:200].lower()
       |        if s.startswith("<!doctype html") or s.startswith("<html"):
       |            return True
       |        if "plotly.newplot" in val[:5000].lower() or "plotly.react" in val[:5000].lower():
       |            return True
       |        if "<img" in s and "base64," in s:
       |            return True
       |        return False
       |
       |    def _html_to_image_bytes(self, html_string):
       |        match = re.search(r"data:image/[^;]+;base64,([A-Za-z0-9+/\n\r =]+)", html_string)
       |        if match:
       |            b64 = match.group(1).replace("\n", "").replace("\r", "").replace(" ", "")
       |            return base64.b64decode(b64)
       |        if "Plotly." in html_string:
       |            try:
       |                import plotly.graph_objects as go
       |                import plotly.io as pio
       |                plotly_match = re.search(r"Plotly\.(?:newPlot|react)\s*\(\s*", html_string)
       |                if plotly_match:
       |                    pos = plotly_match.end()
       |                    if pos < len(html_string) and html_string[pos] in ('"', "'"):
       |                        q = html_string[pos]
       |                        pos += 1
       |                        while pos < len(html_string) and html_string[pos] != q:
       |                            if html_string[pos] == "\\":
       |                                pos += 1
       |                            pos += 1
       |                        pos += 1
       |                    while pos < len(html_string) and html_string[pos] in " ,\n\r\t":
       |                        pos += 1
       |                    data_json, pos = self._extract_json_arg(html_string, pos)
       |                    while pos < len(html_string) and html_string[pos] in " ,\n\r\t":
       |                        pos += 1
       |                    layout_json, _ = self._extract_json_arg(html_string, pos)
       |                    if data_json:
       |                        data = json.loads(data_json)
       |                        layout = json.loads(layout_json) if layout_json else {}
       |                        fig = go.Figure(data=data, layout=layout)
       |                        return pio.to_image(fig, format="png", width=800, height=600)
       |            except ImportError as ie:
       |                raise ValueError(
       |                    f"Plotly chart detected but cannot render to image: {ie}. "
       |                    f"Install kaleido: pip install kaleido"
       |                )
       |            except json.JSONDecodeError:
       |                pass
       |        raise ValueError(
       |            "Cannot convert HTML to image. The HTML does not contain "
       |            "an extractable base64 image or a parseable Plotly chart."
       |        )
       |
       |    def _extract_json_arg(self, text, start_pos):
       |        if start_pos >= len(text):
       |            return None, start_pos
       |        ch = text[start_pos]
       |        openers = {"[": "]", "{": "}"}
       |        if ch not in openers:
       |            return None, start_pos
       |        closer = openers[ch]
       |        depth = 1
       |        pos = start_pos + 1
       |        in_string = False
       |        while pos < len(text) and depth > 0:
       |            c = text[pos]
       |            if in_string:
       |                if c == "\\":
       |                    pos += 2
       |                    continue
       |                if c == '"':
       |                    in_string = False
       |            else:
       |                if c == '"':
       |                    in_string = True
       |                elif c == ch:
       |                    depth += 1
       |                elif c == closer:
       |                    depth -= 1
       |            pos += 1
       |        if depth == 0:
       |            return text[start_pos:pos], pos
       |        return None, start_pos
       |
       |    def _get_audio_content_type(self):
       |        audio_input = str(self.AUDIO_INPUT or "").strip().lower()
       |        if audio_input.startswith("data:"):
       |            header = audio_input.split(",", 1)[0]
       |            if ";" in header:
       |                return header[5:header.index(";")]
       |            return header[5:]
       |        extension_map = {
       |            ".mp3": "audio/mpeg",
       |            ".mpeg": "audio/mpeg",
       |            ".wav": "audio/wav",
       |            ".flac": "audio/flac",
       |            ".ogg": "audio/ogg",
       |            ".oga": "audio/ogg",
       |            ".webm": "audio/webm",
       |            ".opus": "audio/webm;codecs=opus",
       |            ".amr": "audio/amr",
       |            ".m4a": "audio/m4a",
       |        }
       |        from urllib.parse import urlparse as _urlparse
       |        path = _urlparse(audio_input).path if audio_input.startswith("http") else audio_input
       |        _, ext = os.path.splitext(path)
       |        return extension_map.get(ext, "audio/mpeg")
       |
       |    def _url_to_data_url(self, url):
       |        '''Fetch a URL and return a data URL with the correct MIME type.
       |        Fetched via _fetch_remote_url so a malicious/compromised provider
       |        cannot redirect this to an internal address or oversized payload.
       |        '''
       |        raw_content_type, data = self._fetch_remote_url(url)
       |        content_type = raw_content_type.split(";")[0].strip()
       |        if not content_type or content_type == "application/octet-stream":
       |            from urllib.parse import urlparse as _urlparse
       |            ext = os.path.splitext(_urlparse(url).path.lower())[1]
       |            mime_map = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".gif": "image/gif", ".webp": "image/webp", ".svg": "image/svg+xml", ".mp3": "audio/mpeg", ".mpeg": "audio/mpeg", ".wav": "audio/wav", ".flac": "audio/flac", ".ogg": "audio/ogg", ".oga": "audio/ogg", ".m4a": "audio/m4a", ".mp4": "video/mp4", ".webm": "video/webm"}
       |            guessed = mime_map.get(ext, "")
       |            if guessed:
       |                content_type = guessed
       |            else:
       |                task_mime = {"image-to-image": "image/png", "text-to-image": "image/png", "text-to-video": "video/mp4", "text-to-speech": "audio/mpeg"}
       |                content_type = task_mime.get(self.TASK, "application/octet-stream")
       |        b64 = base64.b64encode(data).decode("utf-8")
       |        return f"data:{content_type};base64,{b64}"
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

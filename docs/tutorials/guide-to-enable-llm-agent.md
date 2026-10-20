<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

---
title: "Guide to enable the LLM‐based Texera agent"
weight: 40
---

This guide explains how to enable the AI agent feature in Texera. For detailed explanation about this feature, see https://github.com/apache/texera/pull/4020.

## Prerequisites
- Already know how to setup Texera
- Python 3.11+
- API key from a supported LLM provider (e.g., Anthropic, OpenAI)

## Step 1: Install LiteLLM

Run command:
```bash
pip install 'litellm[proxy]'
```

## Step 2: Configure API Keys

Set your LLM provider API key as an environment variable:

**For Anthropic (Claude):**
```bash
export ANTHROPIC_API_KEY=<your-anthropic-api-key>
```

**For OpenAI:**
```bash
export OPENAI_API_KEY=<your-openai-api-key>
```

> You can set multiple API keys if you want to use models from different providers.

## Step 3: Start LiteLLM Service

Start the LiteLLM proxy using the provided configuration:

```bash
litellm --config bin/litellm-config.yaml
```

By default, LiteLLM runs on `http://0.0.0.0:4000`.

> To customize available models, edit `bin/litellm-config.yaml`. See [LiteLLM documentation](https://docs.litellm.ai/docs/proxy/quick_start) for more options. Also see [LiteLLM Model Configuration](https://docs.litellm.ai/docs/providers) for supported providers and model formats.

## Step 4: Enable agent in Configuration

Modify `common/config/src/main/resources/gui.conf` to enable the agent feature:

```diff
 gui {
   workflow-workspace {
     # ... other settings ...

     # whether AI agent feature is enabled
-    copilot-enabled = false
+    copilot-enabled = true
   }
 }
```

## Step 5: Configure LiteLLM Connection (Optional)

The `AccessControlService` acts as a gateway between the frontend and LiteLLM. If LiteLLM is running on a different host or port, modify `common/config/src/main/resources/llm.conf`:

```diff
 llm {
   # Base URL for LiteLLM service
-  base-url = "http://0.0.0.0:4000"
+  base-url = "http://your-litellm-host:4000"

   # Master key for LiteLLM authentication
-  master-key = ""
+  master-key = "your-master-key"
 }
```

Alternatively, set environment variables:

```bash
export LITELLM_BASE_URL=http://your-litellm-host:4000
export LITELLM_MASTER_KEY=your-master-key
```

## Step 6: Start Texera Services

Start the **all** Texera micro services, including the `AccessControlService`.

## Done!

After opening any workflow, you should now see a robot icon at the bottom right. Click on it will expand a panel with all the available models:
![2025-11-25 18 34 39](/images/github-assets/c0fe6d8d-76ef-4761-9f4f-e23ebc2429fe.png)



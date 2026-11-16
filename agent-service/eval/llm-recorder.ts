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

/** One model call, as it went over the wire. */
export interface ModelCall {
  /** Bytes of the request: system prompt, tool definitions and messages, as sent. */
  requestBytes: number;
  status: number;
  inputTokens: number;
  outputTokens: number;
}

/**
 * Sits between the agent service and its LLM endpoint, passes every request through unchanged,
 * and records each chat completion: what it sent and the tokens the provider counted.
 */
export class LlmRecorder {
  readonly calls: ModelCall[] = [];
  private server?: ReturnType<typeof Bun.serve>;

  constructor(private readonly target: string) {}

  /** Starts listening on a free port and returns the URL to use as the LLM endpoint. */
  start(): string {
    this.server = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: request => this.forward(request) });
    return `http://127.0.0.1:${this.server.port}`;
  }

  stop(): void {
    this.server?.stop(true);
  }

  private async forward(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const body = await request.arrayBuffer();
    const headers = new Headers(request.headers);
    headers.delete("host");
    const response = await fetch(`${this.target.replace(/\/$/, "")}${url.pathname}${url.search}`, {
      method: request.method,
      headers,
      body: body.byteLength > 0 ? body : undefined,
    });
    const text = await response.text();
    if (url.pathname.endsWith("/chat/completions")) {
      const usage = parseUsage(text);
      this.calls.push({ requestBytes: body.byteLength, status: response.status, ...usage });
    }
    // The body is passed on already decoded, so the encoding headers no longer describe it.
    const out = new Headers(response.headers);
    out.delete("content-encoding");
    out.delete("content-length");
    return new Response(text, { status: response.status, headers: out });
  }
}

function parseUsage(text: string): { inputTokens: number; outputTokens: number } {
  try {
    const usage = JSON.parse(text)?.usage;
    return { inputTokens: usage?.prompt_tokens ?? 0, outputTokens: usage?.completion_tokens ?? 0 };
  } catch {
    return { inputTokens: 0, outputTokens: 0 };
  }
}

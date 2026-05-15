/**
 * UDF Copilot router — schema-aware AI inside the Python UDF editor.
 *
 * Four stateless HTTP endpoints; all share the same Texera context shape
 * (upstream schema, sample row, downstream consumer, operator type).
 */

import { Elysia, t } from "elysia";
import { createOpenAI } from "@ai-sdk/openai";
import { generateText } from "ai";
import { getBackendConfig } from "../api/backend-api";
import { env } from "../config/env";
import { PYTHON_UDF_INSTRUCTIONS } from "../agent/prompts";
import { createLogger } from "../logger";

const log = createLogger("UdfCopilot");

const DEFAULT_MODEL = "claude-haiku-4.5";

/**
 * In-memory sample-row store keyed by workerId. Filled by Texera's Python
 * worker on the first tuple it processes (see data_processor.py). Read by the
 * frontend via GET /sample-row?workerId=... when building UDF context.
 *
 * Entries are kept until process restart — small (one row per worker) and the
 * worker IDs are stable across re-runs, so successive runs just overwrite.
 */
const sampleRowStore = new Map<string, Record<string, unknown>>();

function createModel(modelType: string) {
  const config = getBackendConfig();
  const openai = createOpenAI({
    baseURL: `${config.modelsEndpoint}/api`,
    apiKey: env.LLM_API_KEY,
  });
  return openai.chat(modelType);
}

const UdfContextSchema = t.Object({
  upstreamSchema: t.Optional(
    t.Array(t.Object({ name: t.String(), type: t.String() }))
  ),
  sampleRow: t.Optional(t.Record(t.String(), t.Any())),
  downstreamSchema: t.Optional(
    t.Array(t.Object({ name: t.String(), type: t.String() }))
  ),
  operatorType: t.Optional(t.String()),
});

type UdfContext = {
  upstreamSchema?: { name: string; type: string }[];
  sampleRow?: Record<string, unknown>;
  downstreamSchema?: { name: string; type: string }[];
  operatorType?: string;
};

function renderContext(ctx?: UdfContext): string {
  if (!ctx) return "No upstream context available.";
  const parts: string[] = [];
  if (ctx.operatorType) parts.push(`Operator type: ${ctx.operatorType}`);
  if (ctx.upstreamSchema?.length) {
    const cols = ctx.upstreamSchema.map(c => `${c.name}: ${c.type}`).join(", ");
    parts.push(`Upstream output schema (this UDF's input):\n  [${cols}]`);
  }
  if (ctx.sampleRow) {
    parts.push(`Sample upstream row:\n  ${JSON.stringify(ctx.sampleRow)}`);
  }
  if (ctx.downstreamSchema?.length) {
    const cols = ctx.downstreamSchema.map(c => `${c.name}: ${c.type}`).join(", ");
    parts.push(`Downstream consumer expects:\n  [${cols}]`);
  }
  return parts.length > 0 ? parts.join("\n\n") : "No upstream context available.";
}

/**
 * Extract Python code from an LLM response. The prompts ask for raw code
 * (no fences, no commentary), but models routinely break both rules. So we:
 *  - prefer the FIRST fenced ```python ... ``` block if any (allowing
 *    arbitrary text before/after, AND empty bodies — strips trailing
 *    explanation paragraphs);
 *  - fall back to the trimmed text otherwise.
 */
function stripCodeFence(text: string): string {
  const trimmed = text.trim();
  // \n? around the body so degenerate ```python``` (no content) also strips
  // cleanly to "" instead of leaking the fence markers into the editor.
  const fence = trimmed.match(/```(?:python|py)?\s*\n?([\s\S]*?)\n?```/);
  if (fence) return fence[1].trim();
  return trimmed;
}

/** "delete this line", "remove this block", etc. — but NOT "don't delete". */
function isDeletionIntent(instruction: string): boolean {
  const negated = /\b(don'?t|do not|never)\s+(delete|remove|drop|clear|erase)\b/i.test(instruction);
  if (negated) return false;
  return /\b(delete|remove|drop|clear|erase)\b/i.test(instruction);
}

/**
 * Heuristic: the AI gave us a placeholder/marker instead of real code.
 * Triggers when the output is short AND either contains deletion-marker words
 * or has the shape of a parenthesized/commented note.
 */
function looksLikePlaceholder(code: string): boolean {
  const t = code.trim();
  if (t.length === 0) return true; // already empty, harmless
  if (t.length > 120) return false; // too long to be a marker
  if (/\b(deleted|removed|dropped|empty|cleared|no[ -]code|placeholder)\b/i.test(t)) return true;
  // shape: (foo), [foo], # foo — a single parenthesized note or comment line
  if (/^[\s#]*[\(\[][^\(\[\)\]]*[\)\]][\s#]*$/.test(t)) return true;
  if (/^#[^\n]*$/.test(t)) return true;
  return false;
}

/**
 * Lint the AI's "fixed" code for known anti-patterns. Returns `ok: false`
 * with a specific reason that gets fed back into a one-shot retry.
 *
 * Catches the two failure modes that show up repeatedly in practice:
 *   1. `yield tuple_["col"]` from `process_tuple` — yields a scalar, not a
 *      TupleLike. Triggers `pampy.MatchError` at runtime.
 *   2. `tuple_.items()` / `.keys()` / `.values()` / `.get()` — pytexera's
 *      Tuple isn't a dict; these methods don't exist.
 */
function validateUdfOutput(code: string): { ok: true } | { ok: false; reason: string } {
  // 1. Tuple-as-dict misuse anywhere in the code.
  const dictMethod = code.match(/\btuple_\.(items|keys|values|get)\s*\(/);
  if (dictMethod) {
    return {
      ok: false,
      reason:
        `\`tuple_.${dictMethod[1]}()\` does not exist on pytexera's Tuple. ` +
        `Tuple is NOT a Python dict — use \`for name, value in tuple_.as_key_value_pairs():\` ` +
        `to iterate fields, or \`tuple_.as_dict()\` to convert to a real dict.`,
    };
  }

  // 2. process_tuple yielding a scalar via subscript. Extract the method body
  // (rough — slice from "def process_tuple" to the next top-level def/class).
  const ptIdx = code.indexOf("def process_tuple");
  if (ptIdx !== -1) {
    const after = code.slice(ptIdx);
    const nextDef = after.slice(1).search(/\n(class |def )/);
    const body = nextDef === -1 ? after : after.slice(0, nextDef + 1);
    if (/^\s*yield\s+tuple_\[["'][^"']+["']\]\s*$/m.test(body)) {
      return {
        ok: false,
        reason:
          `\`yield tuple_["..."]\` yields a scalar (the column value), but ` +
          `\`process_tuple\` must yield a TupleLike (Tuple, dict, or pd.Series). ` +
          `Use \`yield tuple_\` to forward the whole row, or wrap as ` +
          `\`yield {"col": tuple_["..."]}\` to yield a single-column dict.`,
      };
    }
  }

  return { ok: true };
}

// Shared system-prompt header injected into every endpoint.
function baseSystem(ctx?: UdfContext): string {
  return `You are an expert Python coding assistant embedded in the Texera workflow editor's UDF code editor.

You have privileged context about the surrounding dataflow that GitHub Copilot does not have. Use it.

## Texera context
${renderContext(ctx)}

${PYTHON_UDF_INSTRUCTIONS}`;
}

export const udfCopilotRouter = new Elysia({ prefix: "/udf-copilot" })
  .onError(({ error, set }) => {
    log.error({ err: error }, "udf-copilot request error");
    const message = error instanceof Error ? error.message : String(error);
    set.status = 500;
    return { error: message };
  })

  // ── 0a. Texera Python worker pushes the first tuple it sees ───────────────
  .post(
    "/sample-capture",
    ({ body }) => {
      const { workerId, row } = body as { workerId: string; row: Record<string, unknown> };
      if (!workerId || !row) {
        return { ok: false };
      }
      sampleRowStore.set(workerId, row);
      log.info({ workerId, keys: Object.keys(row).slice(0, 8) }, "captured sample row from python worker");
      return { ok: true };
    },
    {
      body: t.Object({
        workerId: t.String(),
        row: t.Record(t.String(), t.Any()),
      }),
    }
  )

  // ── 0b. Frontend pulls sample row by workerId ─────────────────────────────
  .get(
    "/sample-row",
    ({ query }) => {
      const workerId = (query as { workerId?: string }).workerId;
      if (!workerId) return { row: null };
      return { row: sampleRowStore.get(workerId) ?? null };
    },
    {
      query: t.Object({ workerId: t.Optional(t.String()) }),
    }
  )

  // ── 1. Schema-aware ghost text ──────────────────────────────────────────────
  .post(
    "/complete",
    async ({ body }) => {
      const { prefix, suffix, context, modelType } = body as any;
      const ctx = context as UdfContext | undefined;

      const system =
        baseSystem(ctx) +
        `

## Task: Fill-in-the-middle completion

Given a prefix and a suffix of Python code, output the text that should be inserted at the cursor.

Rules:
- Output ONLY the insertion text. No markdown fences, no commentary, no \`\`\`.
- Keep completions short (1 line is best, max 3 lines).
- Prefer real column names from the upstream schema above over generic guesses.
- If the user is typing tuple_["..."] or a similar accessor, complete with an actual column name.
- If you're unsure or no good completion exists, output an empty string.`;

      const user = `### Prefix (code before cursor)
\`\`\`python
${prefix}
\`\`\`

### Suffix (code after cursor)
\`\`\`python
${suffix ?? ""}
\`\`\`

Output the text to insert at the cursor:`;

      const result = await generateText({
        model: createModel(modelType || DEFAULT_MODEL),
        system,
        messages: [{ role: "user", content: user }],
        temperature: 0,
      });

      let text = result.text ?? "";
      text = stripCodeFence(text);
      // Hard cap — ghost text shouldn't be a paragraph.
      if (text.length > 400) text = text.slice(0, 400);

      return { text };
    },
    {
      body: t.Object({
        prefix: t.String(),
        suffix: t.Optional(t.String()),
        context: t.Optional(UdfContextSchema),
        modelType: t.Optional(t.String()),
      }),
    }
  )

  // ── 2. Side chat ────────────────────────────────────────────────────────────
  .post(
    "/chat",
    async ({ body }) => {
      const { messages, code, context, modelType } = body as any;
      const ctx = context as UdfContext | undefined;

      const system =
        baseSystem(ctx) +
        `

## Task: Chat with the user about their UDF code

The user's current UDF code is shown below. Answer questions, suggest improvements, generate tests, etc.

If you propose code changes, wrap the FULL new code in a single fenced \`\`\`python ... \`\`\` block at the END of your reply. The frontend will offer an "Apply to editor" button for it. If you're just answering a question, omit the fenced block.

### Current UDF code
\`\`\`python
${code ?? ""}
\`\`\``;

      const result = await generateText({
        model: createModel(modelType || DEFAULT_MODEL),
        system,
        messages: messages.map((m: any) => ({ role: m.role, content: m.content })),
        temperature: 0.2,
      });

      const reply = result.text ?? "";
      // Extract last python code block if present.
      const blocks = [...reply.matchAll(/```python\s*\n([\s\S]*?)\n```/g)];
      const suggestedCode = blocks.length > 0 ? blocks[blocks.length - 1][1] : undefined;

      return { reply, suggestedCode };
    },
    {
      body: t.Object({
        messages: t.Array(
          t.Object({
            role: t.Union([t.Literal("user"), t.Literal("assistant")]),
            content: t.String(),
          })
        ),
        code: t.Optional(t.String()),
        context: t.Optional(UdfContextSchema),
        modelType: t.Optional(t.String()),
      }),
    }
  )

  // ── 3. Cmd+K inline rewrite ─────────────────────────────────────────────────
  .post(
    "/rewrite",
    async ({ body }) => {
      const { selectedCode, allCode, instruction, context, modelType } = body as any;
      const ctx = context as UdfContext | undefined;

      const system =
        baseSystem(ctx) +
        `

## Task: Rewrite a selection of Python code

The user has selected a region of their UDF and given a natural-language instruction. Rewrite ONLY the selection.

Rules:
- Output ONLY the replacement code. No markdown fences, no commentary, no placeholder/marker text.
- Preserve indentation that matches the original selection.
- Do not change the class name (ProcessTupleOperator / ProcessTableOperator).
- If the instruction is unclear or unsafe, output the original selection unchanged.
- **Deletion requests**: if the instruction asks to delete / remove / drop the selection (e.g. "delete this line", "remove this block"), output LITERALLY ZERO CHARACTERS — an empty response. Do NOT output any text like \`(deleted)\`, \`(empty)\`, \`# removed\`, or "the line is deleted" — anything you output becomes literal Python and will break the code. If you want to comment that something was deleted, don't — just emit nothing.

### Full UDF code (for context)
\`\`\`python
${allCode ?? ""}
\`\`\``;

      const user = `### Selected code to rewrite
\`\`\`python
${selectedCode}
\`\`\`

### Instruction
${instruction}

Output the new code that replaces the selection:`;

      const result = await generateText({
        model: createModel(modelType || DEFAULT_MODEL),
        system,
        messages: [{ role: "user", content: user }],
        temperature: 0.2,
      });

      let newCode = stripCodeFence(result.text ?? "");

      // Safety net for deletion intent: Haiku sometimes outputs a placeholder
      // like "(deleted)" or "# removed" instead of literally nothing. If the
      // user clearly asked to delete AND the output looks placeholder-ish,
      // force it to empty.
      if (isDeletionIntent(instruction) && looksLikePlaceholder(newCode)) {
        newCode = "";
      }

      return { newCode };
    },
    {
      body: t.Object({
        selectedCode: t.String(),
        allCode: t.Optional(t.String()),
        instruction: t.String(),
        context: t.Optional(UdfContextSchema),
        modelType: t.Optional(t.String()),
      }),
    }
  )

  // ── 5. Schema sync — AI infers outputColumns + retainInputColumns ──────────
  .post(
    "/sync-schema",
    async ({ body }) => {
      const { code, context, currentOutputColumns, currentRetainInputColumns, modelType } =
        body as any;
      const ctx = context as UdfContext | undefined;

      const system =
        baseSystem(ctx) +
        `

## Task: Infer the correct property-panel schema for this Python UDF

You are analyzing a Python UDF to determine the correct values for two
properties in the operator panel that affect how rows flow out of the UDF:

- \`retainInputColumns\`: boolean — if true, upstream columns pass through
  automatically and \`outputColumns\` lists only ADDITIONS/replacements.
  If false, \`outputColumns\` must enumerate EVERY column the UDF produces.
- \`outputColumns\`: array of \`{ attributeName, attributeType }\`. Types
  must be one of: "string" | "integer" | "double" | "boolean" | "long" |
  "timestamp" | "binary".

### Decision rules (in priority order)

1. If the code's only mutation of \`tuple_\` is field ASSIGNMENT
   (\`tuple_["x"] = ...\`), and it eventually \`yield tuple_\`:
   → \`retainInputColumns=true\`, \`outputColumns\` = the assigned columns
   that aren't already in upstream.

2. If the code drops/renames an upstream column (e.g. \`del tuple_["x"]\`
   or builds a new dict via \`yield {"a": ..., "b": ...}\` without including
   all upstream columns):
   → \`retainInputColumns=false\`, \`outputColumns\` = full list of every
   column the yielded shape actually has, in order.

3. If the code yields a pandas DataFrame (Table API) constructed from
   scratch (e.g. \`yield pd.DataFrame({...})\`):
   → \`retainInputColumns=false\`, \`outputColumns\` = the DataFrame's
   columns in order.

4. If the code yields \`tuple_\` unmodified, or you can't tell:
   → \`retainInputColumns=true\`, \`outputColumns=[]\` (no changes).

### Output

Output ONLY a JSON object, no commentary, no fences:

\`\`\`
{
  "retainInputColumns": true,
  "outputColumns": [
    { "attributeName": "...", "attributeType": "..." }
  ],
  "explanation": "one short sentence on WHY"
}
\`\`\``;

      const user = `### Current UDF code
\`\`\`python
${code ?? ""}
\`\`\`

### Currently declared outputColumns
${JSON.stringify(currentOutputColumns ?? [], null, 2)}

### Current retainInputColumns
${currentRetainInputColumns ?? true}

Output the recommended schema as JSON.`;

      const result = await generateText({
        model: createModel(modelType || DEFAULT_MODEL),
        system,
        messages: [{ role: "user", content: user }],
        temperature: 0.1,
      });

      // Extract the JSON block out of whatever the model returned.
      const raw = (result.text ?? "").trim();
      let parsed: { retainInputColumns?: boolean; outputColumns?: any[]; explanation?: string } = {};
      try {
        const fenced = raw.match(/```(?:json)?\s*\n?([\s\S]*?)\n?```/);
        const jsonText = fenced ? fenced[1] : raw;
        parsed = JSON.parse(jsonText);
      } catch (e) {
        log.warn({ raw, err: e }, "sync-schema: failed to parse model JSON");
      }

      return {
        retainInputColumns:
          typeof parsed.retainInputColumns === "boolean" ? parsed.retainInputColumns : true,
        outputColumns: Array.isArray(parsed.outputColumns) ? parsed.outputColumns : [],
        explanation: parsed.explanation ?? "",
      };
    },
    {
      body: t.Object({
        code: t.String(),
        currentOutputColumns: t.Optional(t.Array(t.Any())),
        currentRetainInputColumns: t.Optional(t.Boolean()),
        context: t.Optional(UdfContextSchema),
        modelType: t.Optional(t.String()),
      }),
    }
  )

  // ── 4. Quick Fix on Pyright errors ──────────────────────────────────────────
  .post(
    "/fix",
    async ({ body }) => {
      const { errorMessage, code, range, context, modelType } = body as any;
      const ctx = context as UdfContext | undefined;

      const system =
        baseSystem(ctx) +
        `

## Task: Fix a Pyright/runtime error in the user's UDF

You are given the full UDF code and a specific error. Produce a corrected version of the FULL code.

### Diagnose the error source FIRST

Before fixing, classify the error:

1. **UDF code error** — the traceback's deepest user frame points at the user's file
   (look for \`udf-v*.py\`, \`tmp*.py\`, or \`<unknown>:N, in process_tuple/process_table/process_batch\`).
   Examples: \`AttributeError: 'Tuple' object has no attribute 'items'\`,
   \`KeyError: 'foo'\`, \`TypeError\` from user code.
   → Apply a normal fix.

2. **API-contract violation** — the user's UDF compiles and runs, but yields the wrong shape.
   Common signs:
     - \`process_tuple\` yields a scalar (e.g. \`yield tuple_["a"]\` yields a number, not a Tuple).
     - \`process_table\` yields a Series instead of a DataFrame.
     - Returns the wrong column count for the declared output schema.
   Hint: framework errors like \`pampy.MatchError: This case is not handled: <scalar>\`
   or \`TypeError: cannot convert <type> to TupleLike\` almost always trace back to this.
   → Fix by wrapping the yield in a proper TupleLike (e.g. \`yield tuple_\` or
     \`yield {"a": tuple_["a"]}\` — a Tuple, dict, or pd.Series).

3. **Pure framework error** — the traceback contains NO user frame at all
   (only Texera/amber/pampy/pandas internals). The UDF code is fine.
   → Do NOT rewrite the UDF arbitrarily. Output the original code UNCHANGED and
     add a single Python comment at the top explaining why no fix was applied,
     e.g. \`# AI: this looks like a framework error, not a UDF bug. <reason>\`.

### General rules
- Output the corrected FULL code, NOT just the diff.
- **Output raw Python only.** Do NOT wrap in \`\`\`python fences. Do NOT add any explanation paragraph before or after the code. The frontend pastes your output directly into the editor — anything that isn't Python becomes a syntax error.
- Preserve everything that doesn't need to change.
- If the error references a column name (KeyError, "unknown attribute", etc.), check the upstream schema above — the right name is often a near-miss (e.g. \`age\` vs \`user_age\`). Pick the closest matching column.
- Do not change the class name.
- After fixing, double-check that every \`yield\` in \`process_tuple\` yields a TupleLike (Tuple / dict / pd.Series), NEVER a scalar.`;

      const user = `### Error
${errorMessage}

### Error location
Line ${range?.startLine ?? "?"}, column ${range?.startColumn ?? "?"} to line ${range?.endLine ?? "?"}, column ${range?.endColumn ?? "?"}

### Full UDF code
\`\`\`python
${code}
\`\`\`

Output the corrected full code:`;

      const model = createModel(modelType || DEFAULT_MODEL);

      const firstAttempt = await generateText({
        model,
        system,
        messages: [{ role: "user", content: user }],
        temperature: 0.1,
      });

      let newCode = stripCodeFence(firstAttempt.text ?? "");
      const check = validateUdfOutput(newCode);

      if (!check.ok) {
        // One-shot retry: hand the LLM its own failed output plus the
        // specific reason so it can correct course.
        log.info({ reason: check.reason }, "udf-copilot /fix: first attempt failed validation, retrying");
        const retry = await generateText({
          model,
          system,
          messages: [
            { role: "user", content: user },
            { role: "assistant", content: newCode },
            {
              role: "user",
              content:
                `That fix is incorrect. Reason: ${check.reason}\n\n` +
                `Produce a new corrected full code that does NOT violate this constraint. ` +
                `Output the corrected full code only, no commentary, no fences.`,
            },
          ],
          temperature: 0.1,
        });
        newCode = stripCodeFence(retry.text ?? newCode);
      }

      return { newCode, explanation: "" };
    },
    {
      body: t.Object({
        errorMessage: t.String(),
        code: t.String(),
        range: t.Optional(
          t.Object({
            startLine: t.Number(),
            startColumn: t.Number(),
            endLine: t.Number(),
            endColumn: t.Number(),
          })
        ),
        context: t.Optional(UdfContextSchema),
        modelType: t.Optional(t.String()),
      }),
    }
  );

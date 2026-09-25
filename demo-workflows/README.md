# Demo workflows for the AI Workflow Fixer

Seven workflows that fail on purpose, covering every error pattern the fixer
handles. Each file is a bare `WorkflowContent` (operators / links / positions /
settings), the same shape as `bin/single-node/examples/workflows/`.

## Prerequisites

The two `TextInput`-sourced files (`missing_column_textinput.json` and
`pipeline_missing_column.json`) carry their own rows and need no dataset, so
they run on a bare stack. The other five read the example movies dataset, so
bring the stack up with the examples loaded first:

```sh
bin/single-node.sh up --with-examples
```

That publishes `/dataset/texera/popular-movies-of-imdb/v1/TMDb_updated.csv`
(columns: `id`, `title`, `overview`, `original_language`, `vote_count`,
`vote_average`), which every workflow here scans.

## Loading them

Import a file from the workflow list in the GUI, or POST it the way
`bin/single-node/examples/load-examples.sh` does — the content travels as a
**string**, not as a nested object:

```sh
curl -s -X POST "$TEXERA_DASHBOARD_SERVICE_URL/workflow/create" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d "{\"name\":\"missing_column\", \"content\": $(jq -Rs < demo-workflows/missing_column.json)}"
```

## What each one does

| File | Operator | Expected failure | Fix the LLM should propose |
| --- | --- | --- | --- |
| `missing_column.json` | Python UDF | `KeyError: 'vote_counts'` | use `vote_count`, the closest column in the schema |
| `type_error.json` | Python UDF | `TypeError: unsupported operand type(s) for -: 'str' and 'int'` | cast `title`/`vote_count` explicitly |
| `null_error.json` | Python UDF | `ValueError: Input contains NaN.` | `.dropna()` (or `fillna`) before scaling |
| `model_not_found.json` | HuggingFace | provider 404 for the model | correct `modelId` to `Qwen/Qwen2.5-72B-Instruct` |
| `combined.json` | Python UDF | `KeyError: 'vote_avg'` first, `ValueError: Input contains NaN.` after | one fix per round: the missing column first |
| `missing_column_textinput.json` | Python UDF | `KeyError: 'lines'` | use `line`, the column `TextInput` actually emits |
| `pipeline_missing_column.json` | Python UDF | `KeyError: 'lines'` | same fix, but with `Filter` and `Sort` downstream so the re-run shows the whole pipeline producing rows |

Notes:

- The fixer classifies the **raw** error text, so the Python traceback that
  arrives as an ERROR console message is what it reads — not the trimmed text
  shown in the error tab.
- `model_not_found.json` needs a Hugging Face API token in the operator's
  `hfApiToken` field to reach the provider and get the 404; the field is left
  empty here on purpose so no credential is committed.
- `pipeline_missing_column.json` is the one used for the PR recording: the two
  operators after the UDF make it visible that applying a fix re-runs the
  workflow end to end rather than just clearing the traceback.
- The NaN cases inject the missing values themselves
  (`table.loc[table.index % 7 == 0, ...] = float("nan")`) so the failure is
  deterministic rather than dependent on gaps in the source data.

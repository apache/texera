window.BENCHMARK_DATA = {
  "lastUpdate": 1781250286086,
  "repoUrl": "https://github.com/apache/texera",
  "entries": {
    "Arrow Flight E2E Throughput": [
      {
        "commit": {
          "author": {
            "name": "Benjamin Le",
            "username": "benjaminle22",
            "email": "125538144+benjaminle22@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "39a12345a50292c3b047b7a44f8848a7c7102d8a",
          "message": "test(frontend): add unit tests for CodeEditorService (#5623)\n\n### What changes were proposed in this PR?\nAdds unit tests for CodeEditorService, which previously had no spec\nfile. Covers service creation, `setEditorState`/`getEditorState` for\ntrue and false states, and independent state tracking across multiple\noperator IDs.\n\n### Any related issues, documentation, discussions?\nCloses #5502\n\n### How was this PR tested?\nNew spec run via `yarn test -- code-editor.service` and `yarn lint`. 4\ntests passing.\n\n### Was this PR authored or co-authored using generative AI tooling?\nGenerated-by: Claude (Claude Sonnet 4.6)\n\nCo-authored-by: Benjamin Le <benjaminl@uci.edu>",
          "timestamp": "2026-06-11T23:09:20Z",
          "url": "https://github.com/apache/texera/commit/39a12345a50292c3b047b7a44f8848a7c7102d8a"
        },
        "date": 1781220330000,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "unit": "tuples/sec",
            "value": 383.47906093693575
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "unit": "tuples/sec",
            "value": 813.6903492392828
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "unit": "tuples/sec",
            "value": 927.9436461605338
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "143021053+kunwp1@users.noreply.github.com",
            "name": "Kunwoo (Chris)",
            "username": "kunwp1"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "80542aaaab476b675b10dbd54787c75982913b91",
          "message": "test(amber): fix ConcurrentModificationException flake in RegionExecutionCoordinatorSpec (#5562)\n\n### What changes were proposed in this PR?\n\n`RegionExecutionCoordinatorSpec`'s *\"retry EndWorker failures…\"* test\npolled the `ControllerRpcProbe.calls` buffer from the test thread\n(`waitUntil(endWorkerCalls.size >= 2)`) while the coordinator's 200 ms\n`EndWorker` retry appended to it from the kill-retry timer thread. That\nread racing an append tripped Scala 2.13's `MutationTracker` and\nsurfaced as a non-deterministic\n`java.util.ConcurrentModificationException`.\n\nThe `calls` buffer is test-only — production has no such buffer and\nnever reads it — so the race is a property of the test, not the source.\nRather than make the test helper thread-safe, this fixes the test: it\nwaits on a `CountDownLatch` (counted down from the probe callback once\nthe retry's `EndWorker` is recorded) instead of polling, so the test\nthread never iterates the buffer while the timer thread appends. The\nreal timer-thread retry still runs, so the production path is exercised\nfaithfully — the accesses are just ordered (append → latch → read)\ninstead of overlapping. No production code is changed;\n`ControllerRpcProbe` keeps its plain `ArrayBuffer`.\n\n### Any related issues, documentation, discussions?\n\nResolves #5546\n\n### How was this PR tested?\n\n`RegionExecutionCoordinatorSpec` + `WorkflowExecutionCoordinatorSpec` →\n10/10 pass. The retry test is race-free by construction: its only reads\nof the call buffer happen after the latch `await` returns — i.e. after\nthe timer thread has finished appending — so no read can overlap an\nappend.\n\n```\nsbt 'WorkflowExecutionService/testOnly org.apache.texera.amber.engine.architecture.scheduling.RegionExecutionCoordinatorSpec'\n```\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Anthropic Claude Opus 4.8)",
          "timestamp": "2026-06-12T05:17:54Z",
          "tree_id": "62319eb1f2ef7a97f45742feaf9d9f3dfaff4235",
          "url": "https://github.com/apache/texera/commit/80542aaaab476b675b10dbd54787c75982913b91"
        },
        "date": 1781242447431,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 412.7418451296882,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 816.6507392891085,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 942.235873426011,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "17627829+Yicong-Huang@users.noreply.github.com",
            "name": "Yicong Huang",
            "username": "Yicong-Huang"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1572edf43f708a89573710a4aab9e06726a33924",
          "message": "chore: enable dev static pages (#5637)\n\n### What changes were proposed in this PR?\nEnable GitHub Pages publishing through `.asf.yaml` by setting\n`github.ghp_branch` to `gh-pages` and `github.ghp_path` to `/`.\n\nThis is intended to make dev-facing static pages under the `gh-pages`\nbranch viewable in a browser. The first page this unlocks is the\nbenchmark dashboard generated under `dev/bench`, so benchmark results\ncan be inspected at a stable web URL instead of only through short-lived\nGitHub Actions artifacts.\n\nThe root Pages path is set explicitly because ASF `.asf.yaml` defaults\n`ghp_path` to `/docs` when it is omitted, while the existing dashboard\nfiles are generated at `gh-pages:/dev/bench`.\n\n### Any related issues, documentation, discussions?\nCloses #5636\n\n### How was this PR tested?\nConfiguration-only change; no unit tests were added.\n\n```bash\nruby -e \"require %q(yaml); YAML.load_file(%q(.asf.yaml)); puts %q(YAML OK)\"\ngit diff --check\n```\n\n### Was this PR authored or co-authored using generative AI tooling?\nGenerated-by: Codex (GPT-5)",
          "timestamp": "2026-06-12T05:29:52Z",
          "tree_id": "68e8731bdbf816310f405365441111c00785c1e6",
          "url": "https://github.com/apache/texera/commit/1572edf43f708a89573710a4aab9e06726a33924"
        },
        "date": 1781243098427,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 416.66760416877605,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 938.3660426528729,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1094.9450241456232,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "17627829+Yicong-Huang@users.noreply.github.com",
            "name": "Yicong Huang",
            "username": "Yicong-Huang"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "0731313a73fa36c47cef9d7cfa4c87abc8dfe69e",
          "message": "ci: compare benchmark PRs with main (#5639)\n\n### What changes were proposed in this PR?\nUpdate the benchmark PR comment workflow to show PR benchmark results\nnext to the latest main baseline and the 7-day average baseline\npublished on `gh-pages`.\n\nThe comment now reads the PR run artifact JSON/CSV files and\n`gh-pages:/dev/bench/data.js`, then renders a compact report:\n\n| Section | What reviewers see |\n| --- | --- |\n| Verdict | Material regression/no-regression summary |\n| Noise threshold | Changes within ±5% are treated as CI noise |\n| Summary | `🟢 better · 🔴 worse · ⚪ within ±5% noise` metric counts |\n| Links | Benchmark dashboard and full workflow run |\n| Main table | One row per PR benchmark config, with compact\nicon/value/delta cells |\n| Details | Collapsed latest-main and 7-day-average baseline table |\n| Metrics | Throughput, MB/s, and latency percentiles |\n\nThroughput and MB/s deltas mark higher values as better; latency deltas\nmark lower values as better. If the baseline cannot be loaded, the\nworkflow falls back to the existing PR-only CSV table. The comment\nincludes a disclaimer that CI benchmark machines are noisy and small\ndeltas should be treated cautiously.\n\n### Any related issues, documentation, discussions?\nCloses #5638\n\n### How was this PR tested?\n```bash\nruby -e \"require %q(yaml); YAML.load_file(%q(.github/workflows/benchmarks-pr-comment.yml)); puts %q(YAML OK)\"\nruby -e \"require %q(yaml); y=YAML.load_file(%q(.github/workflows/benchmarks-pr-comment.yml)); puts y[%q(jobs)][%q(comment)][%q(steps)][3][%q(with)][%q(script)]\" | node --input-type=module --check\ngit diff --check\ngh run download 27397378517 --repo apache/texera --name bench-results-27397378517 --dir /tmp/texera-bench-compare-pr5639\n# Locally simulated the compact rich PR-vs-main comment against:\n# https://raw.githubusercontent.com/apache/texera/gh-pages/dev/bench/data.js\n```\n\n### Was this PR authored or co-authored using generative AI tooling?\nGenerated-by: Codex (GPT-5)",
          "timestamp": "2026-06-12T07:29:46Z",
          "tree_id": "21413c1c67cdf9843b5a5102699eb7c6a157df02",
          "url": "https://github.com/apache/texera/commit/0731313a73fa36c47cef9d7cfa4c87abc8dfe69e"
        },
        "date": 1781250283617,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 386.2180304983788,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 931.7827204125922,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1097.5904071552002,
            "unit": "tuples/sec"
          }
        ]
      }
    ],
    "Arrow Flight E2E Latency": [
      {
        "commit": {
          "author": {
            "name": "Benjamin Le",
            "username": "benjaminle22",
            "email": "125538144+benjaminle22@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "39a12345a50292c3b047b7a44f8848a7c7102d8a",
          "message": "test(frontend): add unit tests for CodeEditorService (#5623)\n\n### What changes were proposed in this PR?\nAdds unit tests for CodeEditorService, which previously had no spec\nfile. Covers service creation, `setEditorState`/`getEditorState` for\ntrue and false states, and independent state tracking across multiple\noperator IDs.\n\n### Any related issues, documentation, discussions?\nCloses #5502\n\n### How was this PR tested?\nNew spec run via `yarn test -- code-editor.service` and `yarn lint`. 4\ntests passing.\n\n### Was this PR authored or co-authored using generative AI tooling?\nGenerated-by: Claude (Claude Sonnet 4.6)\n\nCo-authored-by: Benjamin Le <benjaminl@uci.edu>",
          "timestamp": "2026-06-11T23:09:20Z",
          "url": "https://github.com/apache/texera/commit/39a12345a50292c3b047b7a44f8848a7c7102d8a"
        },
        "date": 1781220330000,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "unit": "us",
            "value": 27097.077
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "unit": "us",
            "value": 32465.673
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "unit": "us",
            "value": 32465.673
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "unit": "us",
            "value": 123489.257
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "unit": "us",
            "value": 144333.299
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "unit": "us",
            "value": 144333.299
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "unit": "us",
            "value": 1076894.052
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "unit": "us",
            "value": 1152683.472
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "unit": "us",
            "value": 1152683.472
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "143021053+kunwp1@users.noreply.github.com",
            "name": "Kunwoo (Chris)",
            "username": "kunwp1"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "80542aaaab476b675b10dbd54787c75982913b91",
          "message": "test(amber): fix ConcurrentModificationException flake in RegionExecutionCoordinatorSpec (#5562)\n\n### What changes were proposed in this PR?\n\n`RegionExecutionCoordinatorSpec`'s *\"retry EndWorker failures…\"* test\npolled the `ControllerRpcProbe.calls` buffer from the test thread\n(`waitUntil(endWorkerCalls.size >= 2)`) while the coordinator's 200 ms\n`EndWorker` retry appended to it from the kill-retry timer thread. That\nread racing an append tripped Scala 2.13's `MutationTracker` and\nsurfaced as a non-deterministic\n`java.util.ConcurrentModificationException`.\n\nThe `calls` buffer is test-only — production has no such buffer and\nnever reads it — so the race is a property of the test, not the source.\nRather than make the test helper thread-safe, this fixes the test: it\nwaits on a `CountDownLatch` (counted down from the probe callback once\nthe retry's `EndWorker` is recorded) instead of polling, so the test\nthread never iterates the buffer while the timer thread appends. The\nreal timer-thread retry still runs, so the production path is exercised\nfaithfully — the accesses are just ordered (append → latch → read)\ninstead of overlapping. No production code is changed;\n`ControllerRpcProbe` keeps its plain `ArrayBuffer`.\n\n### Any related issues, documentation, discussions?\n\nResolves #5546\n\n### How was this PR tested?\n\n`RegionExecutionCoordinatorSpec` + `WorkflowExecutionCoordinatorSpec` →\n10/10 pass. The retry test is race-free by construction: its only reads\nof the call buffer happen after the latch `await` returns — i.e. after\nthe timer thread has finished appending — so no read can overlap an\nappend.\n\n```\nsbt 'WorkflowExecutionService/testOnly org.apache.texera.amber.engine.architecture.scheduling.RegionExecutionCoordinatorSpec'\n```\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Anthropic Claude Opus 4.8)",
          "timestamp": "2026-06-12T05:17:54Z",
          "tree_id": "62319eb1f2ef7a97f45742feaf9d9f3dfaff4235",
          "url": "https://github.com/apache/texera/commit/80542aaaab476b675b10dbd54787c75982913b91"
        },
        "date": 1781242449393,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 23896.043,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 31409.153,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 31409.153,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 122074.892,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 141697.41,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 141697.41,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1060606.797,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1122910.271,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1122910.271,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "17627829+Yicong-Huang@users.noreply.github.com",
            "name": "Yicong Huang",
            "username": "Yicong-Huang"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1572edf43f708a89573710a4aab9e06726a33924",
          "message": "chore: enable dev static pages (#5637)\n\n### What changes were proposed in this PR?\nEnable GitHub Pages publishing through `.asf.yaml` by setting\n`github.ghp_branch` to `gh-pages` and `github.ghp_path` to `/`.\n\nThis is intended to make dev-facing static pages under the `gh-pages`\nbranch viewable in a browser. The first page this unlocks is the\nbenchmark dashboard generated under `dev/bench`, so benchmark results\ncan be inspected at a stable web URL instead of only through short-lived\nGitHub Actions artifacts.\n\nThe root Pages path is set explicitly because ASF `.asf.yaml` defaults\n`ghp_path` to `/docs` when it is omitted, while the existing dashboard\nfiles are generated at `gh-pages:/dev/bench`.\n\n### Any related issues, documentation, discussions?\nCloses #5636\n\n### How was this PR tested?\nConfiguration-only change; no unit tests were added.\n\n```bash\nruby -e \"require %q(yaml); YAML.load_file(%q(.asf.yaml)); puts %q(YAML OK)\"\ngit diff --check\n```\n\n### Was this PR authored or co-authored using generative AI tooling?\nGenerated-by: Codex (GPT-5)",
          "timestamp": "2026-06-12T05:29:52Z",
          "tree_id": "68e8731bdbf816310f405365441111c00785c1e6",
          "url": "https://github.com/apache/texera/commit/1572edf43f708a89573710a4aab9e06726a33924"
        },
        "date": 1781243100344,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 25005.014,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 32181.706,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 32181.706,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 108068,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 116217.96,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 116217.96,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 903008.778,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 990043.858,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 990043.858,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "17627829+Yicong-Huang@users.noreply.github.com",
            "name": "Yicong Huang",
            "username": "Yicong-Huang"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "0731313a73fa36c47cef9d7cfa4c87abc8dfe69e",
          "message": "ci: compare benchmark PRs with main (#5639)\n\n### What changes were proposed in this PR?\nUpdate the benchmark PR comment workflow to show PR benchmark results\nnext to the latest main baseline and the 7-day average baseline\npublished on `gh-pages`.\n\nThe comment now reads the PR run artifact JSON/CSV files and\n`gh-pages:/dev/bench/data.js`, then renders a compact report:\n\n| Section | What reviewers see |\n| --- | --- |\n| Verdict | Material regression/no-regression summary |\n| Noise threshold | Changes within ±5% are treated as CI noise |\n| Summary | `🟢 better · 🔴 worse · ⚪ within ±5% noise` metric counts |\n| Links | Benchmark dashboard and full workflow run |\n| Main table | One row per PR benchmark config, with compact\nicon/value/delta cells |\n| Details | Collapsed latest-main and 7-day-average baseline table |\n| Metrics | Throughput, MB/s, and latency percentiles |\n\nThroughput and MB/s deltas mark higher values as better; latency deltas\nmark lower values as better. If the baseline cannot be loaded, the\nworkflow falls back to the existing PR-only CSV table. The comment\nincludes a disclaimer that CI benchmark machines are noisy and small\ndeltas should be treated cautiously.\n\n### Any related issues, documentation, discussions?\nCloses #5638\n\n### How was this PR tested?\n```bash\nruby -e \"require %q(yaml); YAML.load_file(%q(.github/workflows/benchmarks-pr-comment.yml)); puts %q(YAML OK)\"\nruby -e \"require %q(yaml); y=YAML.load_file(%q(.github/workflows/benchmarks-pr-comment.yml)); puts y[%q(jobs)][%q(comment)][%q(steps)][3][%q(with)][%q(script)]\" | node --input-type=module --check\ngit diff --check\ngh run download 27397378517 --repo apache/texera --name bench-results-27397378517 --dir /tmp/texera-bench-compare-pr5639\n# Locally simulated the compact rich PR-vs-main comment against:\n# https://raw.githubusercontent.com/apache/texera/gh-pages/dev/bench/data.js\n```\n\n### Was this PR authored or co-authored using generative AI tooling?\nGenerated-by: Codex (GPT-5)",
          "timestamp": "2026-06-12T07:29:46Z",
          "tree_id": "21413c1c67cdf9843b5a5102699eb7c6a157df02",
          "url": "https://github.com/apache/texera/commit/0731313a73fa36c47cef9d7cfa4c87abc8dfe69e"
        },
        "date": 1781250285563,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 25273.971,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 36064.079,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 36064.079,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 107284.548,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 121671.057,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 121671.057,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 912064.018,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 938140.861,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 938140.861,
            "unit": "us"
          }
        ]
      }
    ]
  }
}
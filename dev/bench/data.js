window.BENCHMARK_DATA = {
  "lastUpdate": 1784903056047,
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
      },
      {
        "commit": {
          "author": {
            "email": "mgball@uci.edu",
            "name": "Matthew B.",
            "username": "Ma77Ball"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "6723f074bc50f8e43f29e1e46bb7c665a0e032be",
          "message": "ci: warn when a PR or issue does not follow the template (#5622)\n\n### What changes were proposed in this PR?\n- Adds a non-blocking GitHub Actions workflow\n(`.github/workflows/template-compliance-warning.yml`) that comments when\na PR or issue is opened/edited without following the template, and\ndeletes the comment automatically once the description is fixed.\n- For PRs it strips the template's `<!-- -->` guidance and flags any\nrequired section that is missing or blank; for issues (GitHub form\ntemplates that already enforce required fields) it only flags a fully\nblank body.\n- Keeps the warning wording in `.github/template-compliance-warning.txt`\nso editing the message does not touch workflow logic.\n- Kept cheap on CI: a single `github-script` job with no build and only\na sparse-checkout of the message file, triggered on `opened`/`edited`\n(never `synchronize`), skipping drafts and bots, and posting one\nself-resolving sticky comment instead of duplicates.\n### Any related issues, documentation, discussions?\nCloses: #5621\n### How was this PR tested?\n- Validated the workflow YAML parses: `python3 -c \"import yaml;\nyaml.safe_load(open('.github/workflows/template-compliance-warning.yml'))\"`.\n- Exercised the detection logic in Node against the real\n`.github/PULL_REQUEST_TEMPLATE`: an unfilled template flags all three\nrequired sections empty, a properly filled body returns no problems, an\nempty body and a template with headings deleted are both flagged, and an\nissue with content passes.\n- The workflow itself runs only on real `pull_request_target`/`issues`\nevents, so end-to-end behavior (comment posted then auto-removed) is\nverifiable once merged; it cannot run from the PR branch beforehand.\n\ntested here: https://github.com/Ma77Ball/texera/issues/60\n<img width=\"1404\" height=\"980\" alt=\"image\"\nsrc=\"https://github.com/user-attachments/assets/1301fc83-8b28-481c-ae96-e137359d28af\"\n/>\n\n\n### Was this PR authored or co-authored using generative AI tooling?\nCo-authored with Claude Opus 4.8 in compliance with ASF",
          "timestamp": "2026-06-12T08:40:15Z",
          "tree_id": "976136e6a35d92bd7fe780b216d1b68a626105ab",
          "url": "https://github.com/apache/texera/commit/6723f074bc50f8e43f29e1e46bb7c665a0e032be"
        },
        "date": 1781254620660,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 366.1832198619419,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 792.3996706663354,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 933.8614922973254,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "lie18@uci.edu",
            "name": "lie18uci",
            "username": "lie18uci"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "ebaea080b5d64c5b19a2a91c18cbcd1ed33c8e50",
          "message": "fix(storage): close Files.walk stream in deleteRepo (#5633)\n\n### What changes were proposed in this PR?\n\nThis PR updates GitVersionControlLocalFileStorage.deleteRepo to close\nthe stream returned by Files.walk(directoryPath) using\ntry-with-resources.\n\nFiles.walk(...) returns a closeable stream backed by directory\nresources. Wrapping it in try-with-resources ensures the stream is\nclosed properly even if traversal or deletion throws.\n\nThis keeps the existing deletion behavior unchanged while fixing the\nstream lifecycle.\n\n### Any related issues, documentation, discussions?\n\nCloses #5548\n\n### How was this PR tested?\n\nAdded GitVersionControlLocalFileStorageSpec, which creates a temporary\nnested repository directory, calls deleteRepo, and verifies that the\nrepository directory is deleted recursively.\n\nRan formatting locally:\nsbt scalafmtAll\nsbt scalafmtCheckAll\nscalafmtCheckAll passed successfully.\n\nAttempted to run the targeted test locally:\n\nsbt \"WorkflowCore / testOnly\norg.apache.texera.amber.core.storage.util.dataset.GitVersionControlLocalFileStorageSpec\"\n\nbut my local backend setup could not generate jOOQ classes because\nPostgreSQL was not running on localhost:5432. The failure occurred\nbefore the test ran, due to missing generated\norg.apache.texera.dao.jooq.generated classes. I am relying on GitHub CI\nto run the backend test in the configured environment.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: ChatGPT",
          "timestamp": "2026-06-12T08:47:26Z",
          "tree_id": "6621a6bda9a9421f7af344395ad04700a3325c15",
          "url": "https://github.com/apache/texera/commit/ebaea080b5d64c5b19a2a91c18cbcd1ed33c8e50"
        },
        "date": 1781255020354,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 357.8780517762326,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 913.1546519216164,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1087.0502501065566,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "142070420+EmilySun621@users.noreply.github.com",
            "name": "EmilySun621",
            "username": "EmilySun621"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "b7b50798cbdab928d3928be36bd200984879d14c",
          "message": "test(frontend): add spec for VisualizationFrameContentComponent (#5585)\n\n### What changes were proposed in this PR?\n\nAdds a behavior-focused unit test spec for\n`VisualizationFrameContentComponent`. Tests cover:\n- `drawChart()` guard clauses (no-op when data is missing)\n- Render path through DomSanitizer to iframe `srcdoc`\n- `auditTime`-throttled subscription (tested with `fakeAsync`/`tick`)\n\n### Any related issues, documentation, discussions?\n\nRelated to #5474 \n\n### How was this PR tested?\n\nSpec verified with `npx ng test --watch=false\n--include='**/visualization-frame-content.component.spec.ts'`. 7 tests\npassing.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Anthropic)\n\nCo-authored-by: Claude Opus 4.7 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-06-12T08:49:49Z",
          "tree_id": "51eb74c19345b89f13dc1cd076c417ddd74a2f6f",
          "url": "https://github.com/apache/texera/commit/b7b50798cbdab928d3928be36bd200984879d14c"
        },
        "date": 1781255309711,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 387.32509759072207,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 931.8413423429488,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1089.9023774707525,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "yangz75@uci.edu",
            "name": "yangzhang75",
            "username": "yangzhang75"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5d74b610cf3c1990f7a70d3445dbdf2e6701f3a0",
          "message": "chore(pyright-language-service): remove unused hocon-parser and hoconjs dependencies (#5581)\n\n<!--\nThanks for sending a pull request (PR)! Here are some tips for you:\n1. If this is your first time, please read our contributor guidelines:\n[Contributing to\nTexera](https://github.com/apache/texera/blob/main/CONTRIBUTING.md)\n  2. Ensure you have added or run the appropriate tests for your PR\n  3. If the PR is work in progress, mark it a draft on GitHub.\n  4. Please write your PR title to summarize what this PR proposes, we \n    are following Conventional Commits style for PR titles as well.\n  5. Be sure to keep the PR description updated to reflect all changes.\n-->\n\n### What changes were proposed in this PR?\n<!--\nPlease clarify what changes you are proposing. The purpose of this\nsection\nis to outline the changes. Here are some tips for you:\n  1. If you propose a new API, clarify the use case for a new API.\n  2. If you fix a bug, you can clarify why it is a bug.\n  3. If it is a refactoring, clarify what has been changed.\n  3. It would be helpful to include a before-and-after comparison using \n     screenshots or GIFs.\n  4. Please consider writing useful notes for better and faster reviews.\n-->\n\nRemoves the dead hocon-parser integration from pyright-language-service.\nThe hoconParser call was removed in #3150 (when the language server\nbecame a standalone microservice) and the leftover import in #3415, but\nthe two dependencies and the type stub were never cleaned up.\n\n- Delete src/types/hocon-parser.d.ts (type stub for an unused module)\n- Remove hocon-parser and hoconjs from package.json\n- Regenerate yarn.lock via yarn install\n\n### Any related issues, documentation, discussions?\n<!--\nPlease use this section to link other resources if not mentioned\nalready.\n1. If this PR fixes an issue, please include `Fixes #1234`, `Resolves\n#1234`\nor `Closes #1234`. If it is only related, simply mention the issue\nnumber.\n  2. If there is design documentation, please add the link.\n  3. If there is a discussion in the mailing list, please add the link.\n-->\nCloses #5442\n\n### How was this PR tested?\n<!--\nIf tests were added, say they were added here. Or simply mention that if\nthe PR\nis tested with existing test cases. Make sure to include/update test\ncases that\ncheck the changes thoroughly including negative and positive cases if\npossible.\nIf it was tested in a way different from regular unit tests, please\nclarify how\nyou tested step by step, ideally copy and paste-able, so that other\nreviewers can\ntest and check, and descendants can verify in the future. If tests were\nnot added,\nplease describe why they were not added and/or why it was difficult to\nadd.\n-->\n\n- `grep -rn \"hocon\" pyright-language-service/src` returns nothing\n- The TypeScript build passes (`tsc --noEmit -p tsconfig.json`, exit 0)\n- No code in the service imports hocon-parser/hoconjs, so this is a pure\ndead-code removal\n\n### Was this PR authored or co-authored using generative AI tooling?\n<!--\nIf generative AI tooling has been used in the process of authoring this\nPR,\nplease include the phrase: 'Generated-by: ' followed by the name of the\ntool\nand its version. If no, write 'No'. \nPlease refer to the [ASF Generative Tooling\nGuidance](https://www.apache.org/legal/generative-tooling.html) for\ndetails.\n-->\nGenerated-by: Claude Code (Claude Opus 4.8)",
          "timestamp": "2026-06-12T08:56:51Z",
          "tree_id": "ced167a58d68b82ec2145a72bac159594ed50cb3",
          "url": "https://github.com/apache/texera/commit/5d74b610cf3c1990f7a70d3445dbdf2e6701f3a0"
        },
        "date": 1781255584217,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 372.7132944539945,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 815.2186085940908,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 917.8065130461815,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "149845903+suyashj1231@users.noreply.github.com",
            "name": "Suyash Jain",
            "username": "suyashj1231"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "7b1c8dc7abca17465039aa5c043a302d3580b419",
          "message": "fix(file-service): apply LakeFS error handling to all call sites (#5607)\n\n### What changes were proposed in this PR?\n\n#4177 introduced `LakeFSExceptionHandler.withLakeFSErrorHandling`, but\nonly the multipart-upload and dataset-version paths used it. The\nremaining LakeFS call sites in `DatasetResource` either leaked raw\n`io.lakefs.clients.sdk.ApiException` to Dropwizard (an opaque 500 for\nthe frontend) or caught `Exception` and rewrapped it as a generic 500,\ndiscarding the real LakeFS status code (401/403/404/409/...).\n\n```\nBefore:  LakeFS 404  ->  raw ApiException / catch(Exception)  ->  500 \"Failed to ...\"\nAfter:   LakeFS 404  ->  withLakeFSErrorHandling              ->  404 \"Error while deleting file 'a.csv' ...: LakeFS resource not found. ...\"\n```\n\nChanges:\n\n| Change | Where |\n| --- | --- |\n| New overload `withLakeFSErrorHandling(operation: String)(call)` that\nprefixes the user-visible message with the failed operation |\n`LakeFSExceptionHandler.scala` |\n| 8 bare LakeFS calls now wrapped (size lookup, version listing, zip\ndownload, presigned URLs, cover image) | `DatasetResource.scala` |\n| 5 `catch Exception -> generic 500` blocks now use the handler;\ncompensation logic (DB rollback on failed repo init, multipart abort) is\npreserved, and the abort-on-failure cleanup no longer masks the original\nerror | `DatasetResource.scala` |\n\nIntentionally unchanged: best-effort cleanup sites that deliberately\nswallow errors, the per-dataset skip in `listDatasets`, and the\n`FileService` startup health check (failing fast at boot is correct\nthere).\n\n### Any related issues, documentation, discussions?\n\nCloses #4176\n\n### How was this PR tested?\n\nNew `LakeFSExceptionHandlerSpec` (7 unit cases): status-code mapping\n(400/401/403/404/409/4xx/5xx/unknown), operation context included in the\nfrontend-visible message, success passthrough, and non-LakeFS exceptions\npropagating untouched.\n\nNew integration case in `DatasetResourceSpec`: deleting a dataset whose\nLakeFS repository does not exist now yields `NotFoundException` (404)\ninstead of a generic 500.\n\n```\nsbt \"FileService/testOnly org.apache.texera.service.util.LakeFSExceptionHandlerSpec\"\n# Tests: succeeded 7, failed 0\nsbt \"FileService/testOnly org.apache.texera.service.resource.DatasetResourceSpec\"\n# Tests: succeeded 94, failed 0  (Testcontainers: LakeFS 1.51 + MinIO + Postgres)\n```\n\n`sbt FileService/scalafixAll` and `sbt FileService/scalafmtAll` produce\nno further diff.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nYes, partially. I (Suyash Jain) worked on this PR together with Claude\nCode as a pair-programming assistant. I reviewed the final diff and ran\nthe unit and Testcontainers-based integration suites locally before\nopening the PR.\n\nGenerated-by: Claude Code (Claude Opus 4.7)",
          "timestamp": "2026-06-12T16:47:17Z",
          "tree_id": "28e0db6c1d142960f9b551f3c515e2e6d775cf4b",
          "url": "https://github.com/apache/texera/commit/7b1c8dc7abca17465039aa5c043a302d3580b419"
        },
        "date": 1781283806102,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 364.17202161443254,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 803.0611605702248,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 912.1648152089354,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "lie18@uci.edu",
            "name": "lie18uci",
            "username": "lie18uci"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "397d2757f3094818c96681261324cc9a9ff17763",
          "message": "test(frontend): add ConflictingFileModalContentComponent unit tests (#5631)\n\n### What changes were proposed in this PR?\n\nFrontend unit tests for ConflictingFileModalContentComponent are added\nin this PR.\n\nThe updated specification confirms that:\n\n1. The component has been successfully generated.\n2. The modal data inserted through NZ_MODAL_DATA is exposed by the\ncomponent.\n\nWithout altering current behavior, this increases test coverage for a\nminor presentational modal component.\n\n\n### Any related issues, documentation, discussions?\n\nCloses #5465\n\n\n### How was this PR tested?\nRan the following command locally from the frontend directory:\nyarn test\n--include='**/conflicting-file-modal-content.component.spec.ts'\nThe test passed successfully with 1 test file passed and 2 tests passed.\n\nAlso ran:\nyarn lint\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: ChatGPT\n\nChatGPT assisted me in finding the location of the component,\nstructuring the unit test, and executing the test command. I read\nthrough the guidance provided by ChatGPT, made the necessary changes\nlocally, and ran the test myself.",
          "timestamp": "2026-06-12T16:52:41Z",
          "tree_id": "a86659b9a1c4878619b19e3eb1f9d3e455059ddd",
          "url": "https://github.com/apache/texera/commit/397d2757f3094818c96681261324cc9a9ff17763"
        },
        "date": 1781284154622,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 409.11868601813325,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 931.5489455525241,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1073.070637656874,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "xinyual3@uci.edu",
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "d76a51e347f54c6c3ff43a7f8cd11f14ae5739ea",
          "message": "test(amber): add unit test coverage for FutureBijection and ElidableStatement (#5555)\n\n### What changes were proposed in this PR?\n\nPin behavior of two utility modules in `engine/common`. No\nproduction-code changes.\n\n| Spec | Source class | Tests |\n| --- | --- | --- |\n| `FutureBijectionSpec` | `FutureBijection` | 11 |\n| `ElidableStatementSpec` | `ElidableStatement` | 9 |\n\nBoth spec files follow the `<srcClassName>Spec.scala` one-to-one\nconvention.\n\n**Behavior pinned — `FutureBijection`**\n\n| Surface | Contract |\n| --- | --- |\n| `TwitterFuture.value.asScala` | resolves to the same value (type\npreserved, `null` preserved) |\n| `TwitterFuture.exception.asScala` | resolves with the same `Throwable`\ninstance (type, message, `eq` identity) |\n| `ScalaFuture.successful.asTwitter` | resolves to the same value (type\npreserved, `null` preserved) |\n| `ScalaFuture.failed.asTwitter` | resolves with the same `Throwable`\ninstance |\n| Twitter → Scala on an already-resolved future | the resulting Scala\nfuture is already completed when the implicit returns |\n| Twitter → Scala → Twitter round-trip | preserves both values and\nexceptions |\n| Scala → Twitter → Scala round-trip | preserves values |\n\n**Behavior pinned — `ElidableStatement`**\n\nThe texera build sets `-Xelide-below WARNING` (`amber/build.sbt`). Every\n`ElidableStatement` helper is annotated with an elide level **strictly\nbelow WARNING** (FINEST / FINER / FINE / INFO), so the Scala compiler\nreplaces every CALL to these helpers with a `()` Unit value at *compile*\ntime. The spec pins this silent-in-production contract:\n\n| Surface | Contract |\n| --- | --- |\n| `info` / `fine` / `finer` / `finest` (with side-effect block) | the\nside effect MUST NOT fire (counter stays at 0) |\n| same methods (with throwing block) | the exception MUST NOT propagate\n|\n| 1000 successive elided calls | no side-effect accumulation |\n| Return type | `Unit` (compile-time enforced) |\n| Parameter shape | accepts a `=> Unit` by-name block (compile-time\nenforced) |\n\nA regression that bumped a method's elide level above WARNING, removed\nthe `@elidable` annotation, or relaxed `-Xelide-below` in the build\nwould re-enable side effects in production — and this spec would catch\nit.\n\n### Any related issues, documentation, discussions?\n\nCloses #5551.\n\n### How was this PR tested?\n\nPure unit-test additions; verified locally with:\n\n- `sbt \"WorkflowExecutionService/testOnly\norg.apache.texera.amber.engine.common.FutureBijectionSpec\norg.apache.texera.amber.engine.common.ElidableStatementSpec\"` — 20\ntests, all green\n- `sbt scalafmtCheckAll` — clean\n- CI to confirm\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Sonnet 4.5)",
          "timestamp": "2026-06-12T17:06:53Z",
          "tree_id": "338e5b84790f546125613e0fa7259f4cfccdc911",
          "url": "https://github.com/apache/texera/commit/d76a51e347f54c6c3ff43a7f8cd11f14ae5739ea"
        },
        "date": 1781284962733,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 427.8146345670815,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 963.0354997598404,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1071.0653442706912,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "xinyual3@uci.edu",
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "7e9cabf2bf4edc0540ab7397dd644bd96cc2a042",
          "message": "test(amber): add unit test coverage for WorkerBatchInternalQueue (#5553)\n\n### What changes were proposed in this PR?\n\nPin behavior of `WorkerBatchInternalQueue` — the per-DP-thread mailbox\ntrait used by the Python worker. Previously uncovered; the only\nuncovered module in the `pythonworker` package whose contract is\nunit-testable without standing up a real Python subprocess. No\nproduction-code changes.\n\n| Spec | Source class | Tests |\n| --- | --- | --- |\n| `WorkerBatchInternalQueueSpec` | `WorkerBatchInternalQueue` (trait +\ncompanion) | 17 |\n\nSpec file name follows the `<srcClassName>Spec.scala` one-to-one\nconvention.\n\n**Behavior pinned**\n\n| Surface | Contract |\n| --- | --- |\n| `enqueueData` + `getElement` | round-trip a `DataElement` (with both\n`DataFrame` and `StateFrame` payloads) |\n| `enqueueCommand` + `getElement` | round-trip a `ControlElement` |\n| `enqueueActorCommand` + `getElement` | round-trip an\n`ActorCommandElement` |\n| Multi-priority dispatch | control elements are returned **before**\ndata elements when both are queued (sub-queue 0 < 1) |\n| FIFO within the control queue | `ControlElement` enqueued first comes\nout before `ActorCommandElement` enqueued second |\n| `getDataQueueLength` | reports only data-queue items (control is\nexcluded) |\n| `getControlQueueLength` / `isControlQueueEmpty` | report all\ncontrol-queue items (`ControlElement` + `ActorCommandElement`) |\n| `disableDataQueue` | hides queued data from `getElement` until\n`enableDataQueue` is called; control flow still moves |\n| `getQueuedCredit(sender)` | `0` initially; tracks bytes-in minus\nbytes-out for `DataFrame` payloads per sender; stays `0` for control /\n`StateFrame` payloads; per-sender accounting is independent; accumulates\nacross multiple enqueues for the same sender |\n| Companion constants | `CONTROL_QUEUE == 0`, `DATA_QUEUE == 1`, and\n`CONTROL_QUEUE < DATA_QUEUE` (relied on by the multi-priority semantics)\n|\n\nThe trait is exercised through a small test-only subclass (`class\nTestQueue extends WorkerBatchInternalQueue`), with\n`DirectControlMessagePayload` represented by a local marker case object\nsince the production trait carries no behavior.\n\n### Any related issues, documentation, discussions?\n\nCloses #5552.\n\n### How was this PR tested?\n\nPure unit-test addition; verified locally with:\n\n- `sbt \"WorkflowExecutionService/testOnly\norg.apache.texera.amber.engine.architecture.pythonworker.WorkerBatchInternalQueueSpec\"`\n— 17 tests, all green\n- `sbt scalafmtCheckAll` — clean\n- CI to confirm\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Sonnet 4.5)\n\n---------\n\nSigned-off-by: Yicong Huang <17627829+Yicong-Huang@users.noreply.github.com>\nCo-authored-by: Yicong Huang <17627829+Yicong-Huang@users.noreply.github.com>\nCo-authored-by: Copilot Autofix powered by AI <175728472+Copilot@users.noreply.github.com>",
          "timestamp": "2026-06-12T17:20:18Z",
          "tree_id": "45f935bf4d928bf2f5aac288a4f68467dece18c6",
          "url": "https://github.com/apache/texera/commit/7e9cabf2bf4edc0540ab7397dd644bd96cc2a042"
        },
        "date": 1781285782528,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 439.81546873489714,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 922.8431031717276,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1081.978843493359,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mgball@uci.edu",
            "name": "Matthew B.",
            "username": "Ma77Ball"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "cb07f2d8b36ce89172c31461e9f3ef3f5b54de2e",
          "message": "feat: Add Card View to Workflows (#4216)\n\n<!--\nThanks for sending a pull request (PR)! Here are some tips for you:\n1. If this is your first time, please read our contributor guidelines:\n[Contributing to\nTexera](https://github.com/apache/texera/blob/main/CONTRIBUTING.md)\n  2. Ensure you have added or run the appropriate tests for your PR\n  3. If the PR is work in progress, mark it a draft on GitHub.\n  4. Please write your PR title to summarize what this PR proposes, we \n    are following Conventional Commits style for PR titles as well.\n  5. Be sure to keep the PR description updated to reflect all changes.\n-->\n\n### What changes were proposed in this PR?\n<!--\nPlease clarify what changes you are proposing. The purpose of this\nsection\nis to outline the changes. Here are some tips for you:\n  1. If you propose a new API, clarify the use case for a new API.\n  2. If you fix a bug, you can clarify why it is a bug.\n  3. If it is a refactoring, clarify what has been changed.\n  3. It would be helpful to include a before-and-after comparison using \n     screenshots or GIFs.\n  4. Please consider writing useful notes for better and faster reviews.\n-->\nThis PR adds a Grid View (Tile View) for workflows in the dashboard.\n\n- New Card Component: Displays workflows as tiles with a preview image.\n- Grid Layout: Responsive grid that adapts to screen size.\n- Enhanced Metadata: Shows size, dates, and view counts; pinned to the\nbottom.\n- Quick Actions: Edit description, rename, duplicate, and share directly\nfrom the card.\n- Toggle: Added a button to switch between List and Grid views.\n### Old View\n<img width=\"2269\" height=\"1009\" alt=\"image\"\nsrc=\"https://github.com/user-attachments/assets/0174952f-e760-4590-aed7-72c2dfdccd99\"\n/>\n\n### New View\n<img width=\"2560\" height=\"1410\" alt=\"image\"\nsrc=\"https://github.com/user-attachments/assets/d36ba290-a28f-44be-b406-c70ad43cace4\"\n/>\n\n### Any related issues, documentation, discussions?\n<!--\nPlease use this section to link other resources if not mentioned\nalready.\n1. If this PR fixes an issue, please include `Fixes #1234`, `Resolves\n#1234`\nor `Closes #1234`. If it is only related, simply mention the issue\nnumber.\n  2. If there is design documentation, please add the link.\n  3. If there is a discussion in the mailing list, please add the link.\n-->\nN/A\n\n### How was this PR tested?\n<!--\nIf tests were added, say they were added here. Or simply mention that if\nthe PR\nis tested with existing test cases. Make sure to include/update test\ncases that\ncheck the changes thoroughly, including negative and positive cases if\npossible.\nIf it was tested in a way different from regular unit tests, please\nclarify how\nyou tested step by step, ideally copy and paste-able, so that other\nreviewers can\ntest and check, and descendants can verify in the future. If tests were\nnot added,\nplease describe why they were not added and/or why it was difficult to\nadd.\n-->\n- Manually verified switching between views.\n- Checked card layout responsiveness.\n- Tested all card actions (edit, like, share, delete).\n\n### Was this PR authored or co-authored using generative AI tooling?\n<!--\nIf generative AI tooling has been used in the process of authoring this\nPR,\nplease include the phrase: 'Generated-by: ' followed by the name of the\ntool\nand its version. If no, write 'No'. \nPlease refer to the [ASF Generative Tooling\nGuidance](https://www.apache.org/legal/generative-tooling.html) for\ndetails.\n-->\nReviewed by Gemini 3\n\n---------\n\nCo-authored-by: Chen Li <chenli@gmail.com>\nCo-authored-by: Claude Opus 4.7 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-06-12T18:30:37Z",
          "tree_id": "8a1462d267d0135b957ddd89a32b5ddad38febb9",
          "url": "https://github.com/apache/texera/commit/cb07f2d8b36ce89172c31461e9f3ef3f5b54de2e"
        },
        "date": 1781289976590,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 436.66832192344117,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 939.4184129089283,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1096.8545037561676,
            "unit": "tuples/sec"
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
          "id": "227cbd73960afbcaa734b30f3ac108dc669324f3",
          "message": "fix(workflow-core): paginate S3 deleteDirectory deletions (#5569)\n\n### What changes were proposed in this PR?\n\n`S3StorageClient.deleteDirectory` listed objects with a single\n`listObjectsV2` call and issued one `deleteObjects` batch. Both S3 APIs\ncap at 1000 keys per call, so for any prefix holding more than 1000\nobjects only the first 1000 were deleted and the rest causes a storage\nleak. This affects dataset deletion (`DatasetResource`) and\nper-execution cleanup (`LargeBinaryManager`), either of which can exceed\n1000 objects under one prefix.\n\nThis PR:\n- Lists via `listObjectsV2Paginator`, which follows the continuation\ntoken across all pages, and deletes in batches of at most 1000 keys.\nKeys are streamed so memory stays bounded to a single batch.\n- Inspects each `DeleteObjects` response and throws if any key failed.\n\n### Any related issues, documentation, discussions?\n\nCloses #5281\n\n### How was this PR tested?\n\n1. Create more than 1000 files `for i in {1..1100}; do printf 'x' >\n\"file_$i.txt\"; done`\n2. Upload them in a dataset. (There is a frontend memory issue when you\nupload all 1100 files at the same time. Try to upload batch-by-batch)\n3. Delete the dataset.\n4. Check if all the files are removed in the minio console. (Before this\nfix, some files remain)\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Claude Opus 4.8)",
          "timestamp": "2026-06-12T18:53:01Z",
          "tree_id": "15289e189e9647c0225659d1cb1ad61c963e39ff",
          "url": "https://github.com/apache/texera/commit/227cbd73960afbcaa734b30f3ac108dc669324f3"
        },
        "date": 1781291261166,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 422.1417599858694,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 894.4897875803534,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1076.9491519579547,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mgball@uci.edu",
            "name": "Matthew B.",
            "username": "Ma77Ball"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "3ab70f4d325a52e13b3c86806002ea2b5836d1ec",
          "message": "perf(pyamber): avoid per-read deepcopy in Tuple.as_dict() (#5599)\n\n### What changes were proposed in this PR?\n- Replace the per-read `deepcopy` in `Tuple.as_dict()`\n(`amber/src/main/python/core/models/tuple.py`) with a shallow copy, so\nreading a tuple no longer recursively clones every field value; cost now\nscales with field count instead of total field byte size.\n- This path is hot: `as_dict()` backs `as_series()` (per-tuple in the\nbatch operator path) and `as_key_value_pairs()`; a tuple carrying a\nlarge binary field previously duplicated that whole payload on every\nread.\n- The deepcopy's isolation was unnecessary: `as_dict()` has no callers\noutside `Tuple`, its two users immediately build a new container, and\nthe Tuple's mutators only reassign dict slots (never mutate a value in\nplace), so a shallow copy preserves the independent-dict contract.\n- Remove the now-unused `from copy import deepcopy` import and document\nwhy the shallow copy is safe.\n### Any related issues, documentation, discussions?\nCloses: #5598\n### How was this PR tested?\n- Existing tests only, no behavior change. Run `cd amber/src/main/python\n&& python -m pytest ../../test/python/core/models/test_tuple.py -q`,\nexpect 23 passed (covers `as_dict`/`as_series`/`as_key_value_pairs`).\n- Run `cd amber/src/main/python && python -m pytest\n../../test/python/core/runnables/test_main_loop.py\n../../test/python/core/architecture/managers/test_tuple_processing_manager.py\n-q`, expect 22 passed (exercises the batch read path that calls\n`as_series`).\n### Was this PR authored or co-authored using generative AI tooling?\nGenerated-by: Claude Opus 4.8\n\n---------\n\nCo-authored-by: Yicong Huang <17627829+Yicong-Huang@users.noreply.github.com>",
          "timestamp": "2026-06-12T20:15:59Z",
          "tree_id": "c6e3abfbba3a7794a4eae487f7cab98f78a712cf",
          "url": "https://github.com/apache/texera/commit/3ab70f4d325a52e13b3c86806002ea2b5836d1ec"
        },
        "date": 1781296481255,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 402.99470117782715,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 779.715364828098,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 935.9727208968402,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "149845903+suyashj1231@users.noreply.github.com",
            "name": "Suyash Jain",
            "username": "suyashj1231"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "d5f5e12fb6879f15dbcf0c9cf6aaae3b532784e6",
          "message": "fix(workflow-operator): no null padding in reservoir sampling (#5606)\n\n### What changes were proposed in this PR?\n\n`ReservoirSamplingOpExec` allocates a fixed-size reservoir of length\n`count` (the per-worker share of `k`). When a worker receives fewer\ntuples than `count`, only the first `n` slots are filled, but `onFinish`\nreturned the whole array, yielding `count - n` trailing `null` entries.\nThe nulls are currently swallowed by a distant null-guard in\n`DataProcessor`, so the bug is latent — but the operator violates the\n\"do not emit null tuples\" contract and breaks if that guard is ever\nnarrowed or bypassed.\n\n```\nBefore:  input < k  ->  onFinish emits [t0 .. tn-1, null, ..., null]  (engine guard hides them)\nAfter:   input < k  ->  onFinish emits [t0 .. tn-1]                   (no nulls emitted at all)\n```\n\nThe fix emits only the filled prefix:\n\n```scala\noverride def onFinish(port: Int): Iterator[TupleLike] = reservoir.iterator.take(n)\n```\n\n`take(n)` is a no-op when `n >= count` (input ≥ k), so the sampled\noutput is unchanged in the normal case.\n\n### Any related issues, documentation, discussions?\n\nCloses #5592\n\n### How was this PR tested?\n\nAdded three regression cases to `ReservoirSamplingOpExecSpec`:\n\n| Case | Asserts |\n| --- | --- |\n| `input size < k` | only the received tuples are emitted, in order, no\nnulls |\n| empty input | `onFinish` emits nothing |\n| skewed partitioning (`k=10`, 3 workers, worker 0 gets 2 tuples) | no\nnull padding for an under-filled worker share |\n\nAll three fail against the old `reservoir.iterator` and pass with\n`reservoir.iterator.take(n)`; the 9 pre-existing cases stay green (TDD\nred → green verified by stashing the source fix).\n\n```\nsbt \"WorkflowOperator/testOnly org.apache.texera.amber.operator.reservoirsampling.ReservoirSamplingOpExecSpec\"\n# Tests: succeeded 12, failed 0, canceled 0, ignored 0, pending 0\n```\n\n`sbt WorkflowOperator/scalafixAll` and `sbt\nWorkflowOperator/scalafmtAll` produce no further diff.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nYes, partially. I (Suyash Jain) worked on this PR together with Claude\nCode as a pair-programming assistant. I reviewed the final diff, ran the\nspec locally, and verified the red → green behavior of the new\nregression tests myself before opening the PR.\n\nGenerated-by: Claude Code (Claude Opus 4.7)\n\nCo-authored-by: Xuan Gu <162244362+xuang7@users.noreply.github.com>",
          "timestamp": "2026-06-12T20:26:57Z",
          "tree_id": "164ab7d040ed744e4bbdbed13ea4b521b4438ecd",
          "url": "https://github.com/apache/texera/commit/d5f5e12fb6879f15dbcf0c9cf6aaae3b532784e6"
        },
        "date": 1781297018685,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 392.7647925624236,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 813.5645013785633,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 941.207972885526,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "sarah_asad@live.com",
            "name": "Sarah Asad",
            "username": "SarahAsad23"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "fa5fcbb60b6f0a305a21635e2560ca0b04b823e2",
          "message": "feat: Make Python Virtual Environment Persistent: Add Environments to Left Panel  (#5577)\n\n<!--\nThanks for sending a pull request (PR)! Here are some tips for you:\n1. If this is your first time, please read our contributor guidelines:\n[Contributing to\nTexera](https://github.com/apache/texera/blob/main/CONTRIBUTING.md)\n  2. Ensure you have added or run the appropriate tests for your PR\n  3. If the PR is work in progress, mark it a draft on GitHub.\n  4. Please write your PR title to summarize what this PR proposes, we \n    are following Conventional Commits style for PR titles as well.\n  5. Be sure to keep the PR description updated to reflect all changes.\n-->\n\n### What changes were proposed in this PR?\n<!--\nPlease clarify what changes you are proposing. The purpose of this\nsection\nis to outline the changes. Here are some tips for you:\n  1. If you propose a new API, clarify the use case for a new API.\n  2. If you fix a bug, you can clarify why it is a bug.\n  3. If it is a refactoring, clarify what has been changed.\n  3. It would be helpful to include a before-and-after comparison using \n     screenshots or GIFs.\n  4. Please consider writing useful notes for better and faster reviews.\n-->\n\nThis PR introduces persistent Python Virtual Environments (PVEs) by\nmoving them out of the Computing Unit (CU) lifecycle and storing them in\nthe database.\n\nPreviously, PVEs were managed through Computing Units and existed only\nwithin the CU they were created in. As a result, PVEs were lost when the\ncorresponding CU was terminated. This PR adds a new\n`virtual_environments` table to persist PVE configurations and\nintroduces a dedicated dashboard interface for managing them.\n\nUsers can now create, view, update, and delete their own Python virtual\nenvironments through a new \"Environments\" page in the dashboard sidebar.\nPVE definitions are stored as user-owned resources in the database and\ncan be managed independently of Computing Units.\n\n<img width=\"1689\" height=\"652\" alt=\"Screenshot 2026-06-08 at 6 39 55 PM\"\nsrc=\"https://github.com/user-attachments/assets/82711baf-b1ce-4cc6-9e84-a29a230ddc3a\"\n/>\n\n<img width=\"1461\" height=\"500\" alt=\"Screenshot 2026-06-08 at 6 40 19 PM\"\nsrc=\"https://github.com/user-attachments/assets/5bbbc360-0adf-401b-8ae8-6d9597d486c2\"\n/>\n\nNote: This PR only introduces persistence for PVE metadata and\nconfiguration. Creating, updating, and deleting a PVE in this PR only\naffects the corresponding database records. The execution-time behavior\nof materializing and using these virtual environments inside a Computing\nUnit is not part of this change and will be introduced in a future PR.\n\nK8s configurations for this feature will be added in a future PR. \n\n### Any related issues, documentation, discussions?\n<!--\nPlease use this section to link other resources if not mentioned\nalready.\n1. If this PR fixes an issue, please include `Fixes #1234`, `Resolves\n#1234`\nor `Closes #1234`. If it is only related, simply mention the issue\nnumber.\n  2. If there is design documentation, please add the link.\n  3. If there is a discussion in the mailing list, please add the link.\n-->\n\nRelated discussions and issues: #5360, #5361.\n\n### How was this PR tested?\n<!--\nIf tests were added, say they were added here. Or simply mention that if\nthe PR\nis tested with existing test cases. Make sure to include/update test\ncases that\ncheck the changes thoroughly including negative and positive cases if\npossible.\nIf it was tested in a way different from regular unit tests, please\nclarify how\nyou tested step by step, ideally copy and paste-able, so that other\nreviewers can\ntest and check, and descendants can verify in the future. If tests were\nnot added,\nplease describe why they were not added and/or why it was difficult to\nadd.\n-->\n\nTested manually and tests added to PveResourceSpec. \n\n### Was this PR authored or co-authored using generative AI tooling?\n<!--\nIf generative AI tooling has been used in the process of authoring this\nPR,\nplease include the phrase: 'Generated-by: ' followed by the name of the\ntool\nand its version. If no, write 'No'. \nPlease refer to the [ASF Generative Tooling\nGuidance](https://www.apache.org/legal/generative-tooling.html) for\ndetails.\n-->\n\nCo-authored using: Claude Code",
          "timestamp": "2026-06-12T20:38:25Z",
          "tree_id": "eb69a6fef9622a398e2fd5de0467a0bd3fa96d5c",
          "url": "https://github.com/apache/texera/commit/fa5fcbb60b6f0a305a21635e2560ca0b04b823e2"
        },
        "date": 1781297584920,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 366.75194810841356,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 833.6982034363127,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 958.8160854848804,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "162244362+xuang7@users.noreply.github.com",
            "name": "Xuan Gu",
            "username": "xuang7"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "73c76f51920b0900de67bbc0baa1ee5be5b87bf0",
          "message": "chore(licensing): refresh transitive versions in LICENSE-binary-python (#5650)\n\n### What changes were proposed in this PR?\n\nRefresh three transitive Python dependency versions in\n`amber/LICENSE-binary-python` to match the bundled wheels, as reported\nby the License Binary Checker ([release\nrun](https://github.com/apache/texera/actions/runs/27444261750) and this\nPR's CI):\n\n- filelock 3.29.1 → 3.29.3\n- huggingface-hub 1.18.0 → 1.19.0\n- matplotlib 3.10.9 → 3.11.0\n\nVersion-only refresh; license groupings unchanged. The file is identical\non `main` and `release/v1.2`, so this PR fixes both via the\n`release/v1.2` backport label.\n\n### Any related issues, documentation, discussions?\n\nResolves #5649\n\n### How was this PR tested?\n\nLicense Binary Checker CI on this PR.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Claude Fable 5)",
          "timestamp": "2026-06-12T22:19:18Z",
          "tree_id": "2891f97620ae3450ebfd91d239d9f383db28c3aa",
          "url": "https://github.com/apache/texera/commit/73c76f51920b0900de67bbc0baa1ee5be5b87bf0"
        },
        "date": 1781303755644,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 420.7679054668489,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 839.888855172904,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 948.0957509664834,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "xinyual3@uci.edu",
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "190823f7562ba8c0bb2a515b0ce4823cf640e049",
          "message": "test(workflow-operator): add unit test coverage for CaseSensitiveAnalyzer (#5658)\n\n### What changes were proposed in this PR?\n\nPin behavior of the Lucene `Analyzer` used by the keyword-search\noperator when the user opts into case-sensitive matching. The\nabstraction skips the lowercasing pipeline used by `StandardAnalyzer`,\nso a regression here would silently downgrade case-sensitive search. No\nproduction-code changes.\n\n| Spec | Source class | Tests |\n| --- | --- | --- |\n| `CaseSensitiveAnalyzerSpec` | `CaseSensitiveAnalyzer` | 13 |\n\nSpec file name follows the `<srcClassName>Spec.scala` one-to-one\nconvention.\n\n**Behavior pinned**\n\n| Surface | Contract |\n| --- | --- |\n| Mixed-case input | every emitted token preserves its original case |\n| All-uppercase / all-lowercase tokens | preserved (no normalization in\neither direction) |\n| Single-space splitting | tokens are separated cleanly |\n| Tabs and newlines | also split tokens |\n| Collapsed whitespace runs | no empty tokens emitted |\n| Embedded punctuation (`abc,def`) | stays one token\n(`WhitespaceTokenizer` only splits on whitespace) |\n| Sentence-final punctuation (`Hello, world!`) | stays attached\n(`Hello,`, `world!`) |\n| Empty input | no tokens |\n| Pure-whitespace input | no tokens |\n| `StopFilter` with `CharArraySet.EMPTY_SET` | English stop words (`the`\n/ `and` / `a`) are NOT removed (vs `StandardAnalyzer`'s default\nbehavior) |\n| Different field names | same tokenization (field-name independent) |\n| Successive `tokenStream` calls | each gets its own independent stream\n|\n\nThe harness uses the canonical Lucene `reset → incrementToken → end →\nclose` lifecycle and collects `CharTermAttribute` values into a buffer —\nsame pattern any future analyzer spec in this codebase should follow.\n\n### Any related issues, documentation, discussions?\n\nCloses #5654.\n\n### How was this PR tested?\n\nPure unit-test addition; verified locally with:\n\n- `sbt \"WorkflowOperator/testOnly\norg.apache.texera.amber.operator.keywordSearch.CaseSensitiveAnalyzerSpec\"`\n— 13 tests, all green\n- `sbt scalafmtCheckAll` — clean\n- CI to confirm\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.7 [1M context])",
          "timestamp": "2026-06-13T00:19:41Z",
          "tree_id": "f611d8cc0bcd54dca2e32c8d9c294bd948752aed",
          "url": "https://github.com/apache/texera/commit/190823f7562ba8c0bb2a515b0ce4823cf640e049"
        },
        "date": 1781310927966,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 396.9884897792455,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 853.6580675939518,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 967.5051233121175,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "xinyual3@uci.edu",
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "e0a96478817a5c7a2f585de1952e40f7c8ba534f",
          "message": "test(workflow-operator): add unit test coverage for AutoClosingIterator and UnionOpExec (#5657)\n\n### What changes were proposed in this PR?\n\nPin behavior of two previously-uncovered helpers in\n`common/workflow-operator`. No production-code changes.\n\n| Spec | Source class | Tests |\n| --- | --- | --- |\n| `AutoClosingIteratorSpec` | `AutoClosingIterator` | 10 |\n| `UnionOpExecSpec` | `UnionOpExec` | 7 |\n\nBoth spec files follow the `<srcClassName>Spec.scala` one-to-one\nconvention.\n\n**Behavior pinned — `AutoClosingIterator`**\n\n| Surface | Contract |\n| --- | --- |\n| `hasNext` (non-empty source) | `true`; `onClose` not invoked |\n| `hasNext` (exhausted source) | `false`; `onClose` invoked exactly once\n|\n| Repeated `hasNext` after exhaustion | does NOT re-fire `onClose`\n(`alreadyClosed` guard) |\n| `next()` | delegates straight to the wrapped iterator (in order) |\n| Full traversal via `toList` | yields every element; `onClose` fires\nonce at the end |\n| Already-empty source | first `hasNext` returns `false` and fires\n`onClose` |\n| Mid-iteration | `onClose` stays un-fired between elements |\n| `onClose` throws | exception propagates (no swallowing) |\n| `onClose` throws + retry | current impl re-fires `onClose` (assignment\nto `alreadyClosed` runs AFTER `onClose()`); characterization pins this\nbehavior |\n\n**Behavior pinned — `UnionOpExec`**\n\n| Surface | Contract |\n| --- | --- |\n| `processTuple(tuple, port = 0)` | yields a single-element iterator\ncontaining the tuple |\n| `processTuple` (any port) | port-agnostic — same tuple passes through\nfor ports 0, 1, 5, 99, MaxValue, -1 |\n| Tuple identity | pass-through preserves the exact `Tuple` reference\n(no copy) |\n| Successive calls | each returns an independent fresh iterator (no\nshared cursor) |\n| Per-call iterator | yields exactly one element |\n| `null` tuple | passes through as `null` (the impl does not null-check)\n|\n| Type contract | `UnionOpExec` is an `OperatorExecutor` |\n\n### Any related issues, documentation, discussions?\n\nCloses #5653.\n\n### How was this PR tested?\n\nPure unit-test additions; verified locally with:\n\n- `sbt \"WorkflowOperator/testOnly\norg.apache.texera.amber.operator.source.scan.AutoClosingIteratorSpec\norg.apache.texera.amber.operator.union.UnionOpExecSpec\"` — 17 tests, all\ngreen\n- `sbt scalafmtCheckAll` — clean\n- CI to confirm\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.7 [1M context])",
          "timestamp": "2026-06-13T00:22:16Z",
          "tree_id": "ade9f49ac2e186c74ad0214bce109814859ccfa2",
          "url": "https://github.com/apache/texera/commit/e0a96478817a5c7a2f585de1952e40f7c8ba534f"
        },
        "date": 1781311177762,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 449.83945702110344,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 953.8605991110668,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1101.959375960379,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "xinyual3@uci.edu",
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "7ae9b35f12748616daf7bcc925fdde2e5def5187",
          "message": "test(workflow-operator): add unit test coverage for filter-family operator executors (#5656)\n\n### What changes were proposed in this PR?\n\nPin behavior of four previously-uncovered modules in the `FilterOpExec`\ninheritance hierarchy in `common/workflow-operator`. No production-code\nchanges.\n\n| Spec | Source class | Tests |\n| --- | --- | --- |\n| `FilterOpExecSpec` | `FilterOpExec` (abstract base) | 9 |\n| `RegexOpExecSpec` | `RegexOpExec` | 8 |\n| `SubstringSearchOpExecSpec` | `SubstringSearchOpExec` | 10 |\n| `RandomKSamplingOpExecSpec` | `RandomKSamplingOpExec` | 7 |\n\nAll four spec files follow the `<srcClassName>Spec.scala` one-to-one\nconvention. `SpecializedFilterOpExec` already has its own spec; this PR\ncovers the rest of the family.\n\n**Behavior pinned — `FilterOpExec`**\n\n| Surface | Contract |\n| --- | --- |\n| `processTuple` (matching predicate) | yields the input tuple as a\nsingle-element iterator |\n| `processTuple` (non-matching predicate) | yields an empty iterator |\n| `processTuple` | passes the actual tuple instance to the predicate;\nignores the `port` argument |\n| `setFilterFunc` | swapping the predicate changes the next\n`processTuple` result; value-aware predicates branch per-tuple |\n| Type contract | `FilterOpExec` is a `Serializable OperatorExecutor` |\n\n**Behavior pinned — `RegexOpExec`**\n\n| Surface | Contract |\n| --- | --- |\n| matching regex | yields the tuple |\n| find-semantics | unanchored substring match (not full-string\n`matches`) |\n| `caseInsensitive = true` / `false` | matches case-(in)sensitively |\n| invalid regex string | construction succeeds (lazy `Pattern`);\n`PatternSyntaxException` surfaces on first `processTuple` |\n| repeated invocations | pattern stays cached; results are stable |\n| malformed descriptor JSON | construction throws\n`JsonProcessingException` |\n\n**Behavior pinned — `SubstringSearchOpExec`**\n\n| Surface | Contract |\n| --- | --- |\n| substring present / absent | yields tuple / nothing |\n| position in value (start / middle / end) | irrelevant —\n`String.contains` semantics |\n| `isCaseSensitive = true` / `false` | case-(in)sensitive (lowercased\nequality on both sides) |\n| empty substring | matches every value, including the empty string |\n| repeated invocations | results stable |\n| malformed descriptor JSON | construction throws\n`JsonProcessingException` |\n\n**Behavior pinned — `RandomKSamplingOpExec`**\n\n| Surface | Contract |\n| --- | --- |\n| `percentage = 100` | accepts every tuple (1000-sample run) |\n| `percentage = 0` | rejects every tuple (1000-sample run) |\n| Same `workerCount` + `percentage` | identical emission count across\ntwo fresh instances (deterministic seed) |\n| `percentage = 50` | approximately half pass (within ±150 of 1000 over\n2000 draws) |\n| Different `workerCount` | divergent emission sequences (the seed is\n`workerCount`) |\n| malformed descriptor JSON | construction throws\n`JsonProcessingException` |\n\n`FilterOpExec` is abstract, so the spec uses a minimal test-only\nconcrete subclass that exposes `setFilterFunc` for behavior-only\nassertions. The three subclass specs build descriptor JSON via\n`objectMapper.writeValueAsString` of a fresh `*OpDesc` (same fixture\npattern as the existing `SpecializedFilterOpExecSpec`).\n\n### Any related issues, documentation, discussions?\n\nCloses #5652.\n\n### How was this PR tested?\n\nPure unit-test additions; verified locally with:\n\n- `sbt \"WorkflowOperator/testOnly\norg.apache.texera.amber.operator.filter.FilterOpExecSpec\norg.apache.texera.amber.operator.regex.RegexOpExecSpec\norg.apache.texera.amber.operator.substringSearch.SubstringSearchOpExecSpec\norg.apache.texera.amber.operator.randomksampling.RandomKSamplingOpExecSpec\"`\n— 34 tests, all green\n- `sbt scalafmtCheckAll` — clean\n- CI to confirm\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.7 [1M context])",
          "timestamp": "2026-06-13T00:30:08Z",
          "tree_id": "57e3eee2ddc27b399f843bfc0bdff1f893477272",
          "url": "https://github.com/apache/texera/commit/7ae9b35f12748616daf7bcc925fdde2e5def5187"
        },
        "date": 1781311467014,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 394.18147463904586,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 729.171868130637,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 946.2959606810432,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "xinyual3@uci.edu",
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "6032413f6f1e65b6a32e53542c4668f00698ec26",
          "message": "test(amber): add unit test coverage for LogicalPlan (#5441)\n\n### What changes were proposed in this PR?\n\nAdds `LogicalPlanSpec` covering\n`amber/src/main/scala/org/apache/texera/workflow/LogicalPlan.scala` —\nthe user-facing logical workflow graph case class plus its companion\nfactory.\n\n| Surface | Pinned |\n| --- | --- |\n| Construction | `LogicalPlan(operators, links)` exposes both fields\nverbatim. |\n| `LogicalPlan.apply(LogicalPlanPojo)` | Lifts the POJO's operators +\nlinks into a `LogicalPlan`, ignoring the POJO-only `opsToViewResult` /\n`opsToReuseResult` fields. |\n| `getTopologicalOpIds` | Topological order on a linear chain; respects\nedge directionality across a fan-out (`a → b, a → c`) — source first,\ntwo sinks unordered in the tail. |\n| `getOperator` | Returns the operator with the requested id; throws\n`NoSuchElementException` for an unknown id. |\n| `getTerminalOperatorIds` | Single sink in a linear chain; every\nout-degree-0 operator in a fan-out plan; every operator when there are\nno links; empty list for an empty plan. |\n| `getUpstreamLinks` | Returns every link whose `toOpId` matches the\nargument; preserves construction order when multiple links flow into the\nsame target; returns an empty list when nothing flows in. |\n| `resolveScanSourceOpFileName` | Failures with `Some(errorList)` are\nappended per-operator instead of throwing; with `None` the first failure\nrethrows; non-`ScanSourceOpDesc` operators are left untouched (no\nerrors, no resolution). Failures are forced deterministically by\npointing a `ScanSourceOpDesc` fixture at a non-existent file path. |\n\nA happy-path `resolveScanSourceOpFileName` test is intentionally\nomitted: `FileResolver` reaches the LakeFS / dataset service in\nproduction and is environment-dependent, so a deterministic unit test\nwould have to mock that surface — out of scope for this spec.\n\nNo production code changed; this is test-only.\n\n### Any related issues, documentation, discussions?\n\nCloses #5438\n\n### How was this PR tested?\n\n```\nsbt \"WorkflowExecutionService/Test/testOnly org.apache.texera.workflow.LogicalPlanSpec\"\n# → 16 tests, all pass\n\nsbt \"WorkflowExecutionService/Test/scalafmtCheck\"\n# → clean\n```\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Claude Opus 4.7)",
          "timestamp": "2026-06-13T17:17:17Z",
          "tree_id": "9dc71d1c975c6d1272c2d271d85a342f4cc0310f",
          "url": "https://github.com/apache/texera/commit/6032413f6f1e65b6a32e53542c4668f00698ec26"
        },
        "date": 1781372023982,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 414.82193623199566,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 949.8288698549242,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1126.0742796626898,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mengw15@uci.edu",
            "name": "Meng Wang",
            "username": "mengw15"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "515d37221adb3d3e2ae282a8bd82151f547c3d51",
          "message": "test(frontend): extend GmailService spec to cover all methods (#5460)\n\n### What changes were proposed in this PR?\n\nExtends the existing `gmail.service.spec.ts` (added in #5164, which\ncovered `sendEmail`'s success/error toasts) to cover the remaining\nsurface of the 3-method service:\n\n- `sendEmail` request-body shape — explicit receiver, and the\nempty-string default when omitted\n- `sendEmail` error branch also logs `console.error(\"Send email error:\",\n…)`\n- `getSenderEmail()` — a `GET` to `/gmail/sender/email` (text) that\nemits the body with no `NotificationService` side-effect\n- `notifyUnauthorizedLogin` — POST body shape, success toast, and error\ntoast + `console.error` logging\n\nFollows `frontend/TESTING.md` (Vitest, `HttpClientTestingModule`).\n\n### Any related issues, documentation, discussions?\n\nCloses #5456. Builds on #5164.\n\n### How was this PR tested?\n\n`yarn test --include='**/gmail.service.spec.ts'` → 9 passed. `prettier\n--check` clean.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (claude-opus-4-7)",
          "timestamp": "2026-06-13T17:18:45Z",
          "tree_id": "47f15aad73a3f32861d6965c95a3df7035331d92",
          "url": "https://github.com/apache/texera/commit/515d37221adb3d3e2ae282a8bd82151f547c3d51"
        },
        "date": 1781372247631,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 536.3525231881152,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 1207.6054812065568,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1435.0459001483277,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "91584519+PG1204@users.noreply.github.com",
            "name": "Prateek Ganigi",
            "username": "PG1204"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "99b9ca281dd4524b0423bac9593ce1f9f136f14d",
          "message": "test(frontend): share workflow-editor TestBed setup, drop double-run (#5626)\n\n### What changes were proposed in this PR?\nAddresses the two review comments on\n[#5318](https://github.com/apache/texera/issues/5318)'s follow-up\nimplementation:\n\n1. Share a common TestBed setup so the jsdom and browser specs don't\ndrift.\n\nThe .browser.spec.ts split created two TestBed configurations for the\nsame component - one in workflow-editor.component.spec.ts (External\nModule Integration describe), one in workflow-editor.browser.spec.ts.\nBoth configured nearly identical imports/providers arrays, which would\ninevitably drift over time.\nExtracted them into a new sibling file workflow-editor.test-utils.ts\nexporting two arrays:\n\n\nexport const workflowEditorTestImports = [ ... ];\nexport const workflowEditorTestProviders: Provider[] = [ ... ];\nThis follows the existing project convention in\n[frontend/src/app/common/testing/test-utils.ts](vscode-webview://1epki5h79lmkghv36u4evg54fuvmk17ndjca203mcg7opnnd5sg9/frontend/src/app/common/testing/test-utils.ts)\n(which exports commonTestImports and commonTestProviders the same way).\nEach spec's TestBed now collapses to:\n\n\nawait TestBed.configureTestingModule({\n  imports: workflowEditorTestImports,\n  providers: workflowEditorTestProviders,\n}).compileComponents();\nAdding or removing a service from either spec's setup is now a single\nedit in one file.\n\n2. Drop the explicit workflow-editor.component.spec.ts entry from the\ntest-browser include in angular.json.\n\nWith the six mouse-event tests now living in\nworkflow-editor.browser.spec.ts (picked up by the **/*.browser.spec.ts\nglob), the explicit listing of workflow-editor.component.spec.ts in\ntest-browser's include array was causing the file's 25 jsdom-friendly\ntests to run twice, once in jsdom (the default test target) and once in\nreal Chrome (the test-browser target). Removed that explicit entry; the\ntest-browser target now picks up only **/*.browser.spec.ts files.\n\nScope note. The pre-existing JointJS Paper describe block at the top of\nworkflow-editor.component.spec.ts has its own deliberately-different\nTestBed setup (ContextMenuComponent instead of\nNzModalCommentBoxComponent, Overlay, MockComputingUnitStatusService) and\nwas left untouched, it wasn't part of the duplication introduced by the\n.browser.spec.ts split.\n\n### Any related issues, documentation, discussions?\nAddresses review feedback on the follow-up PR for\n[#5318](https://github.com/apache/texera/issues/5318).\nCloses #5318 \nRelated to #3614 / PR #5146 (this PR restores tests that were commented\nout during PR #5146 as collateral from the #3614 fix).\n\n\n### How was this PR tested?\nExisting test runs: Verified no test-count regressions in either target,\nand the double-run is gone:\n\nng test (jsdom, full suite): 947 pass / 0 fail / 2 skipped / 1 todo\nacross 103 files\nng run gui:test-browser: 13 pass / 0 fail across 2 files (6 from\nworkflow-editor.browser.spec.ts + 7 from the pre-existing\ncode-editor.component.browser.spec.ts)\nworkflow-editor.component.spec.ts under jsdom: 25/25 pass (unchanged\nfrom before this PR)\nBrowser test count dropped from 38 -> 13, confirming the 25\njsdom-friendly tests in workflow-editor.component.spec.ts no longer\ndouble-run in real Chrome.\n\nStatic checks: tsc --noEmit and eslint clean on all four changed files.\n\n### Was this PR authored or co-authored using generative AI tooling?\nCo-authored-by: Claude Code (Anthropic Claude Opus 4.7)\n\n---------\n\nCo-authored-by: Claude Opus 4.7 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-06-13T17:32:57Z",
          "tree_id": "1d7d21a43f56d77acba5d876421fcaacd0250392",
          "url": "https://github.com/apache/texera/commit/99b9ca281dd4524b0423bac9593ce1f9f136f14d"
        },
        "date": 1781372835399,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 408.43387108356256,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 942.105216087343,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1122.2841129750852,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "133594317+justinsiek@users.noreply.github.com",
            "name": "Justin Siek",
            "username": "justinsiek"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "1a65a3168c5ffbd171e8ed0d64dec495e45c1b24",
          "message": "fix(workflow-operator): set alreadyClosed before onClose (#5678)\n\n### What changes were proposed in this PR?\n\n`AutoClosingIterator.hasNext` only set `alreadyClosed = true` after\ncalling `onClose()`, and so if `onClose()` throws, `alreadyClosed` would\nstay false, and so a subsequent `hasNext` would reinvoke `onClose()`,\nrunning cleanup a second time on a resource whose close already failed.\n\nThe change makes `alreadyClosed = true` run before `onClose()`.\n\n### Any related issues, documentation, discussions?\n\nCloses #5660 \n\n### How was this PR tested?\n\nUpdated AutoClosingIteratorSpec — replaced the existing characterization\ntest (\"re-invoke onClose on a retry when the previous onClose threw\")\nwith a positive assertion that a second hasNext after a throwing close\ndoes NOT re-invoke onClose (closeCount stays at 1) and returns false.\n\n`sbt \"WorkflowOperator/testOnly\norg.apache.texera.amber.operator.source.scan.AutoClosingIteratorSpec\"`\n- 10/10 pass. `sbt scalafmtCheckAll` passes.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Claude Opus 4.8)\n\nCo-authored-by: Justin Siek <justinsiek@Justins-MacBook-Air.local>",
          "timestamp": "2026-06-13T17:51:17Z",
          "tree_id": "def41efc3df06efdc98d8d418cd0036e1f7a88f8",
          "url": "https://github.com/apache/texera/commit/1a65a3168c5ffbd171e8ed0d64dec495e45c1b24"
        },
        "date": 1781373959362,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 465.0311459260295,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 952.7004708795436,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1118.5934653905929,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "123780557+KYinXu@users.noreply.github.com",
            "name": "Kyle Yin Xu",
            "username": "KYinXu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "6becb8596df32058f69473c39f4a9028f149954e",
          "message": "test(workflow-operator): add ImageUtilitySpec for encodeImageToHTML (#5679)\n\n<!--\nThanks for sending a pull request (PR)! Here are some tips for you:\n1. If this is your first time, please read our contributor guidelines:\n[Contributing to\nTexera](https://github.com/apache/texera/blob/main/CONTRIBUTING.md)\n  2. Ensure you have added or run the appropriate tests for your PR\n  3. If the PR is work in progress, mark it a draft on GitHub.\n  4. Please write your PR title to summarize what this PR proposes, we \n    are following Conventional Commits style for PR titles as well.\n  5. Be sure to keep the PR description updated to reflect all changes.\n-->\n\n### What changes were proposed in this PR?\n\nPin the Python snippet emitted by ImageUtility.encodeImageToHTML() — the\nhelper that visualization operators splice into generated UDF code to\nbase64-encode binary image data into an <img> tag. Any drift in the\nf-string template or the error fallback would silently break\nimage-rendering operators (e.g. Word Cloud). No production-code changes.\nNegative cases are covered implicitly: removing any pinned substring\nfails the corresponding assertion.\n\n| Spec | Source Class | Tests |\n| --- | --- | --- |\n| ImageUtilitySpec | ImageUtility | 8 |\n\nSpec file name follows the `<srcClassName>Spec.scala` one-to-one\nconvention.\n\n**Behavior pinned**\n\n| Surface | Contract |\n| :--- | :--- |\n| `encodeImageToHTML` | returns a non-empty `String` |\n| Snippet imports base64 | substring `import base64` appears |\n| Snippet calls `base64.b64encode(binary_image_data)` | substring\nappears verbatim |\n| Snippet decodes to UTF-8 | substring `.decode(\"utf-8\")` appears |\n| Snippet emits an `<img>` tag with `data:image;base64,...` | substring\n`data:image;base64,{encoded_image_str}` appears (verifies the f-string\ntemplate) |\n| Snippet handles binary-decode failure via try/except | substrings\n`except Exception` and `Binary input is not valid` both appear |\n| Snippet contains a top-level `html =` assignment | substring `html =\nf` appears so downstream code can reference `html` |\n| Determinism | two calls return the exact same string |\n\nThe harness pins via `contains` on canonical substrings — resilient to\nincidental whitespace changes from `stripMargin` while still catching\nreal template drift.\n### Any related issues, documentation, discussions?\n<!--\nPlease use this section to link other resources if not mentioned\nalready.\n1. If this PR fixes an issue, please include `Fixes #1234`, `Resolves\n#1234`\nor `Closes #1234`. If it is only related, simply mention the issue\nnumber.\n  2. If there is design documentation, please add the link.\n  3. If there is a discussion in the mailing list, please add the link.\n-->\nCloses #5665 \n\n### How was this PR tested?\n<!--\nIf tests were added, say they were added here. Or simply mention that if\nthe PR\nis tested with existing test cases. Make sure to include/update test\ncases that\ncheck the changes thoroughly including negative and positive cases if\npossible.\nIf it was tested in a way different from regular unit tests, please\nclarify how\nyou tested step by step, ideally copy and paste-able, so that other\nreviewers can\ntest and check, and descendants can verify in the future. If tests were\nnot added,\nplease describe why they were not added and/or why it was difficult to\nadd.\n-->\nUnit tests were added here, tested through local verification:\n\n- ```sbt \"WorkflowOperator/testOnly\norg.apache.texera.amber.operator.visualization.ImageUtilitySpec\"``` -\nAll 8 tests passed\n- ```sbt scalafmtCheckAll``` -- clean\n- ```sbt scalafixAll --check``` No lint errors\n\n### Was this PR authored or co-authored using generative AI tooling?\n<!--\nIf generative AI tooling has been used in the process of authoring this\nPR,\nplease include the phrase: 'Generated-by: ' followed by the name of the\ntool\nand its version. If no, write 'No'. \nPlease refer to the [ASF Generative Tooling\nGuidance](https://www.apache.org/legal/generative-tooling.html) for\ndetails.\n-->\nGenerated-by: Composer 2.5 Fast",
          "timestamp": "2026-06-13T18:00:43Z",
          "tree_id": "13f359fb502e155ab86032e7378f5d3f1ef762de",
          "url": "https://github.com/apache/texera/commit/6becb8596df32058f69473c39f4a9028f149954e"
        },
        "date": 1781374631758,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 526.4027816891697,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 1179.7760368503139,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1429.717512187697,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "xinyual3@uci.edu",
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "7ae2374bead9361735f5538b6501337d3ea32c56",
          "message": "test(config): add unit test coverage for ConfigParserUtil (#5659)\n\n### What changes were proposed in this PR?\n\nPin behavior of `ConfigParserUtil.parseSizeStringToBytes` — the\nsize-string parser used by `StorageConfig` for S3 multipart sizing. The\n`common/config` module had no test infrastructure before this PR (no\n`src/test` directory existed); this PR adds the directory and configures\nthe standard ScalaTest dependency the way the other backend modules do.\n\n| Spec | Source class | Tests |\n| --- | --- | --- |\n| `ConfigParserUtilSpec` | `ConfigParserUtil` | 16 |\n\nSpec file name follows the `<srcClassName>Spec.scala` one-to-one\nconvention.\n\n**Behavior pinned**\n\n| Surface | Contract |\n| --- | --- |\n| `1KB` / `1MB` / `1GB` | parses to `1024L` / `1024 * 1024L` / `1024 *\n1024 * 1024L` |\n| Multi-digit values (`100MB`, `1024KB`, `128GB`) | scales correctly by\nthe unit multiplier |\n| `5GB` | preserves `Long` precision (result exceeds `Int.MaxValue`) |\n| `0010KB` | parses to `10 * 1024L` (decimal-only, no octal\ninterpretation) |\n| Missing unit (`100`) | throws `IllegalArgumentException` with\ndiagnostic |\n| Unsupported unit (`5TB`) | throws `IllegalArgumentException` |\n| Empty string | throws `IllegalArgumentException` |\n| Lowercase unit (`5mb`) | throws `IllegalArgumentException` (regex is\nanchored to `[KMG]B`) |\n| Embedded whitespace (`5 MB`) | throws `IllegalArgumentException` |\n| Non-numeric value (`abcMB`) | throws `IllegalArgumentException` |\n| Unit-only input (`MB`) | throws `IllegalArgumentException` |\n| Return type | `Long` (compile-time enforced) |\n\n**Build-config change**\n\nAdds `org.scalatest %% scalatest % 3.2.15 % Test` to\n`common/config/build.sbt`. The version matches the other backend modules\n(`common/workflow-operator`, `common/dao`, `amber`). Scope is `Test` so\nthe dependency does not leak into the production classpath.\n\n### Any related issues, documentation, discussions?\n\nCloses #5655.\n\n### How was this PR tested?\n\nPure unit-test addition (plus the build-config tweak above); verified\nlocally with:\n\n- `sbt \"Config/testOnly\norg.apache.texera.amber.util.ConfigParserUtilSpec\"` — 16 tests, all\ngreen\n- `sbt scalafmtCheckAll` — clean\n- CI to confirm\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.7 [1M context])",
          "timestamp": "2026-06-13T18:52:29Z",
          "tree_id": "2e5a358109f2310bebc03bf7975d3e3f51f1d54a",
          "url": "https://github.com/apache/texera/commit/7ae2374bead9361735f5538b6501337d3ea32c56"
        },
        "date": 1781377497927,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 379.9732636752667,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 812.9586889741707,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 919.1715558369533,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "125538144+benjaminle22@users.noreply.github.com",
            "name": "Benjamin Le",
            "username": "benjaminle22"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "aceca29cbda1f223ac17753efb58eba726e58b2f",
          "message": "test(frontend): expand OperatorReuseCacheStatusService spec coverage (#5624)\n\n### What changes were proposed in this PR?\nExpands the existing spec for `OperatorReuseCacheStatusService`, which\npreviously only checked `should be created`. Adds behavior-focused tests\ncovering both constructor subscriptions: the `CacheStatusUpdateEvent`\nwebsocket handler and the `getReuseCacheOperatorsChangedStream` handler.\nTests verify that `JointUIService.changeOperatorReuseCacheStatus` is\ncalled correctly for each operator, that the `mainJointPaper` null guard\nis respected in both handlers, and that empty operator lists do not\nthrow.\n\n### Any related issues, documentation, discussions?\nCloses #5543\n\n### How was this PR tested?\nNew tests run via `yarn test -- operator-reuse-cache-status` and `yarn\nlint`. 6 tests passing.\n\n### Was this PR authored or co-authored using generative AI tooling?\nGenerated-by: Claude (Claude Sonnet 4.6)\n\nCo-authored-by: Benjamin Le <benjaminl@uci.edu>",
          "timestamp": "2026-06-13T18:54:04Z",
          "tree_id": "a6bb83aa00c102b61bfa1c9af3c242f86a6c33da",
          "url": "https://github.com/apache/texera/commit/aceca29cbda1f223ac17753efb58eba726e58b2f"
        },
        "date": 1781377772571,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 361.59528252380477,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 797.545154101567,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 917.3749188210405,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "49699333+dependabot[bot]@users.noreply.github.com",
            "name": "dependabot[bot]",
            "username": "dependabot[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4fd395b8f9875d82d8aaf3cc4884ab2366381ae0",
          "message": "chore(deps): bump shell-quote from 1.8.3 to 1.8.4 in /frontend (#5684)\n\nBumps [shell-quote](https://github.com/ljharb/shell-quote) from 1.8.3 to\n1.8.4.\n<details>\n<summary>Changelog</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/ljharb/shell-quote/blob/main/CHANGELOG.md\">shell-quote's\nchangelog</a>.</em></p>\n<blockquote>\n<h2><a\nhref=\"https://github.com/ljharb/shell-quote/compare/v1.8.3...v1.8.4\">v1.8.4</a>\n- 2026-05-22</h2>\n<h3>Commits</h3>\n<ul>\n<li>[Fix] <code>quote</code>: validate object-token shapes <a\nhref=\"https://github.com/ljharb/shell-quote/commit/4378a6e613db5948168684864e49b42b83134d2d\"><code>4378a6e</code></a></li>\n<li>[Dev Deps] update <code>@ljharb/eslint-config</code>,\n<code>auto-changelog</code>, <code>eslint</code>, <code>npmignore</code>\n<a\nhref=\"https://github.com/ljharb/shell-quote/commit/22ebec04349065a45ad8afc8cc8d53c4624634a6\"><code>22ebec0</code></a></li>\n<li>[Tests] increase coverage <a\nhref=\"https://github.com/ljharb/shell-quote/commit/9f3caa31900cc6ee64858b31134144c648ce206d\"><code>9f3caa3</code></a></li>\n<li>[readme] replace runkit CI badge with shields.io check-runs badge <a\nhref=\"https://github.com/ljharb/shell-quote/commit/3344a047dd1e95f71c4ca27522cbfd05c56277e0\"><code>3344a04</code></a></li>\n<li>[Dev Deps] update <code>@ljharb/eslint-config</code> <a\nhref=\"https://github.com/ljharb/shell-quote/commit/699c5113d135f4d4591574bebf173334ffa453d4\"><code>699c511</code></a></li>\n</ul>\n</blockquote>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a\nhref=\"https://github.com/ljharb/shell-quote/commit/ff166e2b63eb5f932bd131a8886a99e9afdf45ae\"><code>ff166e2</code></a>\nv1.8.4</li>\n<li><a\nhref=\"https://github.com/ljharb/shell-quote/commit/4378a6e613db5948168684864e49b42b83134d2d\"><code>4378a6e</code></a>\n[Fix] <code>quote</code>: validate object-token shapes</li>\n<li><a\nhref=\"https://github.com/ljharb/shell-quote/commit/22ebec04349065a45ad8afc8cc8d53c4624634a6\"><code>22ebec0</code></a>\n[Dev Deps] update <code>@ljharb/eslint-config</code>,\n<code>auto-changelog</code>, <code>eslint</code>, `npmig...</li>\n<li><a\nhref=\"https://github.com/ljharb/shell-quote/commit/9f3caa31900cc6ee64858b31134144c648ce206d\"><code>9f3caa3</code></a>\n[Tests] increase coverage</li>\n<li><a\nhref=\"https://github.com/ljharb/shell-quote/commit/3344a047dd1e95f71c4ca27522cbfd05c56277e0\"><code>3344a04</code></a>\n[readme] replace runkit CI badge with shields.io check-runs badge</li>\n<li><a\nhref=\"https://github.com/ljharb/shell-quote/commit/699c5113d135f4d4591574bebf173334ffa453d4\"><code>699c511</code></a>\n[Dev Deps] update <code>@ljharb/eslint-config</code></li>\n<li>See full diff in <a\nhref=\"https://github.com/ljharb/shell-quote/compare/v1.8.3...v1.8.4\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility\nscore](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=shell-quote&package-manager=npm_and_yarn&previous-version=1.8.3&new-version=1.8.4)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't\nalter it yourself. You can also trigger a rebase manually by commenting\n`@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits\nthat have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all\nof the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop\nDependabot creating any more for this major version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop\nDependabot creating any more for this minor version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop\nDependabot creating any more for this dependency (unless you reopen the\nPR or upgrade to it yourself)\nYou can disable automated security fix PRs for this repo from the\n[Security Alerts page](https://github.com/apache/texera/network/alerts).\n\n</details>\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>",
          "timestamp": "2026-06-13T20:59:04Z",
          "tree_id": "58c4b12a2ccef3dd30abd0a0da746c79facf6ca1",
          "url": "https://github.com/apache/texera/commit/4fd395b8f9875d82d8aaf3cc4884ab2366381ae0"
        },
        "date": 1781385341632,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 425.8555569622274,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 957.9201030252267,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1117.584572849056,
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
          "id": "a0442876321ede19fe7f33e57c1f7b277c09e3ba",
          "message": "chore(frontend): refresh UDF operator icons (#5686)\n\n### What changes were proposed in this PR?\n\nReplaces the eight UDF operator icons under\n`frontend/src/assets/operator_images/` with a clean, consistent\ntypographic set. The previous icons were mismatched — the three Python\nUDF icons carried a leftover red \"New\" overlay, and the\nLambda/Reducer/Java/R icons varied in size and visual style (logo art,\nplain text, stock marks).\n\nEach new icon is a 1024×1024 transparent PNG with a large,\ntightly-cropped hero glyph and, where needed, a small descriptor stacked\nbeneath it: `Py` (Python UDF), `Py` + `src` (Python UDF source), `Py` +\n`2-in` (dual-input Python UDF), `λ` + `py` (Python lambda), `Σ` + `py`\n(Python table reducer), `Java` (Java UDF), `R` (R UDF), `R` + `src` (R\nUDF source). Every Python wordmark — both the hero `Py` and the `py`\ndescriptor — uses the same Python blue/gold two-color scheme; the\n`src`/`2-in` qualifiers are gray. Java uses its orange/blue, R its blue.\nFilenames are unchanged, so they continue to map to the\n`assets/operator_images/<operatorType>.png` lookup in\n`joint-ui.service.ts`.\n\nBefore / after comparison of all eight icons:\n\n![Before and after comparison of the eight UDF operator\nicons](https://raw.githubusercontent.com/Yicong-Huang/texera/media/udf-icons-preview/udf-icons-preview/_before_after.png)\n\n### Any related issues, documentation, discussions?\n\nCloses #5685\n\n### How was this PR tested?\n\nThis is a static asset swap with no code changes, so no automated tests\nwere added. Verified manually by running the Angular frontend (`ng\nserve`) against a local backend and confirming all eight icons render\ncorrectly in the operator panel and on the workflow canvas. Each PNG is\na 1024×1024 transparent square and the eight filenames exactly match the\nexisting files, so the operator-type icon lookup is unaffected.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Opus 4.8 (Cowork)\n\nCo-authored-by: Yicong Huang <yiconghuang@cs.umass.edu>",
          "timestamp": "2026-06-13T22:03:39Z",
          "tree_id": "d6b17b997a64c4c258a8e93652ed019f2f77f126",
          "url": "https://github.com/apache/texera/commit/a0442876321ede19fe7f33e57c1f7b277c09e3ba"
        },
        "date": 1781389200204,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 395.3602388469252,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 833.8320687920495,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 953.2404698392654,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "eugenegujing@outlook.com",
            "name": "Eugene Gu",
            "username": "eugenegujing"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "bdde6b946d79fbe668d211b77e663e2ed263e28f",
          "message": "feat(workflow-operator): add not-blank validation messages (#5640)\n\n<!-- PR title: feat(workflow-operator): add not-blank validation\nmessages -->\n\n### What changes were proposed in this PR?\n\nExtends the not-blank validation pattern introduced for basic chart\noperators in #4006 to **all visualization operators**, and gives every\nbackend `assert` a human-readable message.\n\n**Why this matters.** The frontend (AJV + `required` + the autofill\nattribute enum) already blocks most empty required fields in normal UI\nusage, but three gaps remained where users or API callers hit a bare\n`assertion failed` with no explanation:\n\n| Gap | Example | Fix |\n|---|---|---|\n| Bare asserts are the only backend check for execution paths that\nbypass frontend validation (direct API calls, agent-generated workflows)\n| `assert(value.nonEmpty)` in `PieChartOpDesc` | Every assert now\ncarries a message, e.g. `assert(value.nonEmpty, \"Value Column cannot be\nempty\")` |\n| Empty lists pass AJV's `required` check (it only verifies key\npresence) | `FigureFactoryTableOpDesc.columns = []` ran and crashed |\n`@NotEmpty(message = ...)` on required list attributes → schema gains\n`minItems: 1` |\n| Numeric constraints existed only in backend asserts, invisible to the\nfrontend | `rowHeight = 10` passed the form, then `assert(rowHeight >=\n30)` crashed | `@DecimalMin` on `FigureFactoryTable`\n`fontSize`/`rowHeight` → schema gains `minimum` |\n\n**Changes in detail:**\n\n- Added messages to all 45 previously bare asserts across 23\nvisualization operators, splitting compound asserts (e.g.\n`assert(x.nonEmpty && y.nonEmpty)`) per field so the error names the\nexact missing attribute.\n- Added `@NotNull(message = ...)` to required string attributes lacking\nit; with the generator's `useMinLengthForNotNull`, the schema gains\n`minLength: 1` so the frontend rejects empty strings as well. Annotation\nmessages are identical to their assert messages (same convention as\n#4006). An annotation-only sweep over 20 more operator/config files\nbrings every required visualization attribute under a constraint.\n- Fixed two null defaults (`ImageVisualizerOpDesc.binaryContent`,\n`ScatterMatrixChartOpDesc.selectedAttributes`) that threw a\n`NullPointerException` before the assert could produce its message.\n- Added two missing guards for fields that were interpolated unchecked:\n`IcicleChartOpDesc.manipulateTable()` (`value`) and\n`RadarChartOpDesc.createPlotlyFigure()` (`valueColumns`).\n- No optional field gained a new required-ness constraint, so existing\nsaved workflows are unaffected.\n\n**Notes for reviewers:**\n\n- Messages quote each field's `@JsonSchemaTitle` verbatim; a few titles\nare terse (`x`, `r`, `theta` in contourPlot/quiverPlot/polarChart),\nproducing messages like `x cannot be empty`. Improving those titles is\nleft out of scope — happy to adjust if preferred.\n- `LineChartOpDesc.lines` reuses its pre-existing assert message (`At\nleast one line must be configured`) instead of inventing a second\nphrasing.\n- Non-visualization operators also have required attributes without\nconstraint annotations (~55 files); those fields need per-field semantic\nreview, so they are left for a follow-up issue.\n\n### Any related issues, documentation, discussions?\n\nCloses #4053. Follows the approach of #4006 (and #3692).\n\n### How was this PR tested?\n\nAdded 14 new spec files and extended 9 existing ones, covering every\ntouched operator with positive (all fields set → template renders the\nconfigured columns), negative (empty field → `AssertionError` whose\nmessage names the field and contains \"cannot be empty\"), and boundary\ncases (`rowHeight = 10` → \"at least 30\", `fontSize = -1` →\n\"non-negative\", exact boundary values pass).\n\n```\nsbt \"WorkflowOperator/testOnly org.apache.texera.amber.operator.visualization.*\"\n```\n\n168 tests, 31 suites, all passed. Also ran `sbt scalafixAll` and `sbt\nscalafmtAll` (clean).\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nCo-authored-by: Claude Code (Claude Fable 5)",
          "timestamp": "2026-06-13T23:36:46Z",
          "tree_id": "a2d36008c4931c4b2cd36a093654aa09320810a5",
          "url": "https://github.com/apache/texera/commit/bdde6b946d79fbe668d211b77e663e2ed263e28f"
        },
        "date": 1781394717820,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 414.3305647702846,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 804.7969348285035,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 929.5848463771631,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "1c580e59eb69bc45205298606c3980c67a05803f",
          "message": "fix(execution-service): surface init-time fatal errors to the websocket (#5781)\n\n### What changes were proposed in this PR?\n\nWhen workflow execution initialization fails, the error was recorded\ninto the execution metadata store but never pushed to the websocket, so\nconnected frontend clients saw nothing — particularly for failures\nduring `WorkflowExecutionService` construction, which happens *before*\nthe execution is published to subscribers.\n\n`WorkflowService.initExecutionService`'s catch arm now, after\n`errorHandler(e)` records the fatal error, pushes a `WorkflowErrorEvent`\n(carrying the recorded fatal errors) to `errorSubject` — the\nworkflow-level channel that `connect()` subscribers listen on — so\ninit-time failures surface in the UI.\n\n| init failure | before | after |\n|---|---|---|\n| during `WorkflowExecutionService` construction (pre-publish) | logged\n+ stored, invisible to the UI | `WorkflowErrorEvent` delivered to the\nfrontend |\n| during `executeWorkflow()` | recorded; UI delivery depended on\nsubscription timing | `WorkflowErrorEvent` delivered to the frontend |\n\nThe push is extracted into a small `reportFatalErrorsToSubscribers`\nmethod so it can be unit-tested without a database (the init path itself\nis DB-bound).\n\n### Any related issues, documentation, discussions?\n\nResolves #5782. Discovered while splitting #5700 (loop operators) into\nsmaller PRs; this fix is independent of that feature and applies to\n`main` on its own.\n\n### How was this PR tested?\n\nNew `WorkflowServiceSpec` (TDD, red → green): pins that\n`reportFatalErrorsToSubscribers` delivers a `WorkflowErrorEvent` to a\n`connect()` subscriber carrying exactly the fatal errors recorded in the\nexecution state store (single error, and all errors when several are\npresent). `sbt \"WorkflowExecutionService/testOnly *WorkflowServiceSpec\"`\npasses (2/2); scalafmt + scalafix clean.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nCo-authored with Claude Opus 4.8 in compliance with ASF.",
          "timestamp": "2026-06-24T02:45:12Z",
          "url": "https://github.com/apache/texera/commit/1c580e59eb69bc45205298606c3980c67a05803f"
        },
        "date": 1782308015128,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 650.4837531886355,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1287.5991093248963,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1436.9147097894797,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 831.5202916863423,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1357.723035471166,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1433.19306222954,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 866.5715022106941,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1360.2078935161044,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1415.5968699209252,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 726.9270055469568,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 1066.155601084265,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 1128.9386124527498,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 799.6982169242415,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 1068.5158034731887,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1113.6476343352147,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 788.2748197519269,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 1041.7613219142518,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 1103.0001561273777,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 478.8542612687155,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 587.0580209295506,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 595.2581678307049,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 473.5436054480575,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 580.5323390138135,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 587.4811430192561,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 457.91222640313714,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 559.0628391186693,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 564.6111496289036,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "abbe7a30244d6e50bc5ce7a89befcbc7a9d31c28",
          "message": "test(workflow-operator): add unit test coverage for Sklearn tree-based classifier descriptors (#5939)\n\n### What changes were proposed in this PR?\n\nPin behavior of four previously-untested Sklearn tree-based classifier\ndescriptors in `common/workflow-operator`. No production-code changes.\n\n| Spec | Source class | Tests |\n| --- | --- | --- |\n| `SklearnDecisionTreeOpDescSpec` | `SklearnDecisionTreeOpDesc` | 5 |\n| `SklearnExtraTreeOpDescSpec` | `SklearnExtraTreeOpDesc` | 5 |\n| `SklearnExtraTreesOpDescSpec` | `SklearnExtraTreesOpDesc` | 5 |\n| `SklearnRandomForestOpDescSpec` | `SklearnRandomForestOpDesc` | 5 |\n\n**Behavior pinned**\n\n| Surface | Contract |\n| --- | --- |\n| `operatorInfo` | exact model name + `Sklearn <name> Operator`\ndescription; Sklearn group; training/testing input ports + one blocking\noutput |\n| field defaults | `countVectorizer`/`tfidfTransformer` `false`;\n`target`/`text` `null` |\n| `getOutputSchemas` | `model_name` (STRING) + `model` (BINARY) keyed by\nthe declared output port |\n| `generatePythonCode` | imports the matching sklearn estimator and\nbuilds the `make_pipeline` model |\n| Round-trip | config fields preserved through the polymorphic\n`LogicalOp` base, with the correct `operatorType` discriminator |\n\n### Any related issues, documentation, discussions?\n\nPart of the ongoing `workflow-operator` unit-test coverage effort\n(follow-up to the Sklearn Naive Bayes coverage in #5925).\n\n### How was this PR tested?\n\n- `sbt \"WorkflowOperator/testOnly *SklearnDecisionTreeOpDescSpec\n*SklearnExtraTreeOpDescSpec *SklearnExtraTreesOpDescSpec\n*SklearnRandomForestOpDescSpec\"` — 20 tests, all green\n- `sbt \"WorkflowOperator/Test/scalafmtCheck\"` and `sbt\n\"WorkflowOperator/scalafixAll --check\"` — clean\n- CI to confirm\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-06-25T07:19:56Z",
          "url": "https://github.com/apache/texera/commit/abbe7a30244d6e50bc5ce7a89befcbc7a9d31c28"
        },
        "date": 1782394907829,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 681.7316276213489,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1141.1874757652577,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1195.0540956458694,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 864.5664071944375,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1143.415464265076,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1196.7131332866613,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 872.4866627337208,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1140.6388965088288,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1208.215304273915,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 723.7632492824176,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 936.3680609938499,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 955.8726169132882,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 762.5662580667366,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 930.2180532277979,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 958.0017389428375,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 766.5009803513046,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 927.3228391262231,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 945.9315632802263,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 452.4345141748527,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 528.1829046718673,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 539.6278636643726,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 450.2220539179129,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 527.2967527731824,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 538.2611919823398,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 437.083053008114,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 501.4935026396234,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 514.4630465690494,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "7a38b6cf4c476e6f7ae0b0555fd711b50f4e85ec",
          "message": "test(workflow-operator): add unit test coverage for Sklearn Naive Bayes descriptors (#5925)\n\n### What changes were proposed in this PR?\n\nPin behavior of the four previously-untested Sklearn Naive Bayes\nclassifier descriptors in `common/workflow-operator`. No production-code\nchanges.\n\n| Spec | Source class | Tests |\n| --- | --- | --- |\n| `SklearnBernoulliNaiveBayesOpDescSpec` |\n`SklearnBernoulliNaiveBayesOpDesc` | 5 |\n| `SklearnComplementNaiveBayesOpDescSpec` |\n`SklearnComplementNaiveBayesOpDesc` | 5 |\n| `SklearnGaussianNaiveBayesOpDescSpec` |\n`SklearnGaussianNaiveBayesOpDesc` | 5 |\n| `SklearnMultinomialNaiveBayesOpDescSpec` |\n`SklearnMultinomialNaiveBayesOpDesc` | 5 |\n\n**Behavior pinned**\n\n| Surface | Contract |\n| --- | --- |\n| `operatorInfo` | exact model name + `Sklearn <name> Operator`\ndescription; Sklearn group; training/testing input ports + one blocking\noutput |\n| field defaults | `countVectorizer`/`tfidfTransformer` `false`;\n`target`/`text` `null` |\n| `getOutputSchemas` | `model_name` (STRING) + `model` (BINARY) keyed by\nthe declared output port |\n| `generatePythonCode` | imports and instantiates the matching sklearn\nestimator (e.g. `BernoulliNB`) via `make_pipeline` |\n| Round-trip | config fields preserved through the polymorphic\n`LogicalOp` base, with the correct `operatorType` discriminator |\n\n### Any related issues, documentation, discussions?\n\nPart of the ongoing `workflow-operator` unit-test coverage effort.\n\n### How was this PR tested?\n\n- `sbt \"WorkflowOperator/testOnly *SklearnBernoulliNaiveBayesOpDescSpec\n*SklearnComplementNaiveBayesOpDescSpec\n*SklearnGaussianNaiveBayesOpDescSpec\n*SklearnMultinomialNaiveBayesOpDescSpec\"` — 20 tests, all green\n- `sbt \"WorkflowOperator/Test/scalafmtCheck\"` and `sbt\n\"WorkflowOperator/scalafixAll --check\"` — clean\n- CI to confirm\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-06-26T08:35:47Z",
          "url": "https://github.com/apache/texera/commit/7a38b6cf4c476e6f7ae0b0555fd711b50f4e85ec"
        },
        "date": 1782481149782,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 576.2831696533727,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1057.980801264424,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1137.3852216752157,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 790.2801231705785,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1111.5203714298443,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1157.032780695532,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 774.8118030168804,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1105.0432581104042,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1149.5012724029139,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 701.6727865900524,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 902.2113741385191,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 910.11474852682,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 714.3834125958101,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 907.9558607992127,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 919.3675843529894,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 672.693484104106,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 875.8722583272764,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 912.2551519701937,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 445.8343681594853,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 521.8282310352096,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 525.0855178941647,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 432.1230240317772,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 511.94018652715624,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 524.072749773277,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 408.82635269861913,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 493.63956918145755,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 495.5842293231326,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "7944c0979b0d79a44ae3d4641deb80a87a426682",
          "message": "test(workflow-operator): add unit test coverage for Sklearn training SVM, neighbor & misc descriptors (#5956)\n\n### What changes were proposed in this PR?\n\nPin behavior of nine previously-untested Sklearn **training** operator\ndescriptors in `common/workflow-operator`. No production-code changes.\n\n| Spec | Source class |\n| --- | --- |\n| `SklearnTrainingSVMOpDescSpec` | `SklearnTrainingSVMOpDesc` |\n| `SklearnTrainingLinearSVMOpDescSpec` |\n`SklearnTrainingLinearSVMOpDesc` |\n| `SklearnTrainingKNNOpDescSpec` | `SklearnTrainingKNNOpDesc` |\n| `SklearnTrainingNearestCentroidOpDescSpec` |\n`SklearnTrainingNearestCentroidOpDesc` |\n| `SklearnTrainingRidgeOpDescSpec` | `SklearnTrainingRidgeOpDesc` |\n| `SklearnTrainingRidgeCVOpDescSpec` | `SklearnTrainingRidgeCVOpDesc` |\n| `SklearnTrainingDummyClassifierOpDescSpec` |\n`SklearnTrainingDummyClassifierOpDesc` |\n| `SklearnTrainingMultiLayerPerceptronOpDescSpec` |\n`SklearnTrainingMultiLayerPerceptronOpDesc` |\n| `SklearnTrainingProbabilityCalibrationOpDescSpec` |\n`SklearnTrainingProbabilityCalibrationOpDesc` |\n\n**Behavior pinned** (shared `SklearnTrainingOpDesc` contract)\n\n| Surface | Contract |\n| --- | --- |\n| `operatorInfo` | exact model name (`Training: <model>`) + `Sklearn\n<name> Operator` description; **Sklearn Training** group; single\n`training` input port; one blocking output |\n| field defaults | `countVectorizer`/`tfidfTransformer` `false`;\n`target`/`text` `null` |\n| `getOutputSchemas` | `model_name` (STRING) + `model` (BINARY) keyed by\nthe declared output port |\n| `generatePythonCode` | imports the matching sklearn estimator and\nbuilds the `make_pipeline(...).fit(X, Y)` training model |\n| Round-trip | config fields preserved through the polymorphic\n`LogicalOp` base, with the correct `operatorType` discriminator |\n\n### Any related issues, documentation, discussions?\n\nPart of the ongoing `workflow-operator` unit-test coverage effort (the\ntraining-side counterpart to the Sklearn classifier coverage in\n#5925/#5939/#5940/#5941/#5945/#5946/#5951).\n\n### How was this PR tested?\n\n- `sbt \"WorkflowOperator/testOnly\norg.apache.texera.amber.operator.sklearn.training.*\"` — 45 tests, all\ngreen\n- `sbt \"WorkflowOperator/Test/scalafmtCheck\"` and `sbt\n\"WorkflowOperator/scalafixAll --check\"` — clean\n- CI to confirm\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-06-27T00:24:53Z",
          "url": "https://github.com/apache/texera/commit/7944c0979b0d79a44ae3d4641deb80a87a426682"
        },
        "date": 1782566448527,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 663.3561062047322,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1117.0825872932649,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1208.9525342597474,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 843.1469561343058,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1159.1794548340051,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1212.9528006670607,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 875.6412881015916,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1167.6024750442616,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1194.489555064826,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 708.0746811903396,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 941.9503896172347,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 963.9845256023498,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 749.7604101247025,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 948.6335053645886,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 965.2678929844304,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 761.1418634708496,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 920.8446213634342,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 952.856591729709,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 452.3432618802734,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 533.8702085478014,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 540.7273037922697,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 454.5702533156899,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 532.8369823562394,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 543.2076879939052,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 444.2315046393665,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 514.2193506290274,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 520.9451381339056,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Jiadong Bai",
            "username": "bobbai00",
            "email": "43344272+bobbai00@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "a24d1d1701dfffc46279ecda1db34268e2dca44c",
          "message": "refactor(agent-service): extract WebSocket message types into types/ws (#5751)\n\n### What changes were proposed in this PR?\n\nThis PR refactors the agent-service ↔ frontend WebSocket messaging into\na dedicated **`types/ws`** folder. Each frame is a small class whose\n`type` discriminator equals its class name (matching the main Texera WS\nconvention, e.g. `RegionUpdateEvent`). Building a frame with `new\nWsServerStatusEvent(...)` sets the wire `type` for you, so no `type:\n\"...\"` literal is hand-written at call sites; receivers `switch` on\n`event.type`.\n\n- `types/ws/client.ts` — frames the frontend sends, unioned as\n`WsClientCommand`:\n  - `WsClientPromptCommand` — a user prompt for the agent to run.\n  - `WsClientStopCommand` — stop the in-flight run (payload-less).\n- `types/ws/server.ts` — frames agent-service sends back, unioned as\n`WsServerEvent`:\n- `WsServerSnapshotEvent` — full state, sent once when a client\nconnects.\n  - `WsServerStepEvent` — one step, streamed live as the agent runs.\n- `WsServerStatusEvent` — a lifecycle change (GENERATING / AVAILABLE /\nSTOPPING).\n  - `WsServerErrorEvent` — an error to surface to the user.\n- `WsServerHeadChangeEvent` — unused by the frontend/UI (still reachable\nvia `/agents/:id/checkout`); slated for removal in #5930.\n- Operator result summaries are no longer pushed over WebSocket — they\nare pulled on demand via `GET /agents/:id/operator-results`. The\ncorresponding REST DTO `OperatorResultSummary` lives in\n`types/execution.ts` (not under `ws/`).\n\nThis PR also adds unit tests and renames the `agent-service` test files\nfrom `*.test.ts` to `*.spec.ts` to align with the frontend's naming\nconvention.\n\n### Any related issues, documentation, discussions?\n\nCloses #5749\n\n### How was this PR tested?\n\nAdded/updated unit tests in both `agent-service` and `frontend` all\npass; a local end-to-end run was also verified.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Opus 4.8 (1M context)\n\n---------\n\nCo-authored-by: Claude Opus 4.8 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-06-28T09:46:50Z",
          "url": "https://github.com/apache/texera/commit/a24d1d1701dfffc46279ecda1db34268e2dca44c"
        },
        "date": 1782652963563,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 629.6321169096885,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1122.7866167710467,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1176.295884964202,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 848.3293311580297,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1127.62395785478,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1184.7727283490974,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 863.7158031317904,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1140.4840257817527,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1171.390831917027,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 725.8422811195763,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 921.1966341034391,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 948.7723297876665,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 759.3957743891441,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 932.9703289771704,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 941.7038756948331,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 744.7339578839125,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 912.5585192608825,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 927.637168078939,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 462.9130924354061,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 526.1714437261043,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 538.4734640318966,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 461.86339248948866,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 528.841795364716,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 536.9348492395007,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 425.65533728126036,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 504.5457768217761,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 508.88512744247424,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Yicong Huang",
            "username": "Yicong-Huang",
            "email": "17627829+Yicong-Huang@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "613b76e467713efffdeef353b4c831e0f7adba29",
          "message": "ci: require 60% patch coverage in Codecov (#6022)\n\n### What changes were proposed in this PR?\n\nThe Codecov patch status was `informational: true`, so it never gated\nPRs. This makes it a real requirement:\n\n- set `target: 60%` on `coverage.status.patch.default`;\n- add `threshold: 10%` so the patch passes when coverage is at least\n`target - threshold` = 50%, softening the quantization on tiny diffs\n(e.g. a 2-coverable-line change passes at 1 line / 50%);\n- drop the `informational` flag so the patch check can fail.\n\nPRs must now cover at least 50% of the lines they change (60% target,\n10% slack). Project status (auto target, 1% threshold), flag\ncarryforward, and ignore rules are unchanged. The explanatory comment in\n`codecov.yml` is updated to match.\n\n### Any related issues, documentation, discussions?\n\nCloses #6021\n\n### How was this PR tested?\n\nConfig-only change; `codecov.yml` validated as well-formed YAML. Codecov\napplies the new policy on the next PR upload.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Claude Opus 4.8)",
          "timestamp": "2026-06-29T06:18:20Z",
          "url": "https://github.com/apache/texera/commit/613b76e467713efffdeef353b4c831e0f7adba29"
        },
        "date": 1782742625463,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 736.3867959894528,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1293.0418129576594,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1422.3839263017499,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 921.8556094802523,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1379.8608158432235,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1432.5769324823377,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 949.4465454153644,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1352.9415930492632,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1445.7700252766172,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 783.9699574308155,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 1079.4396867271903,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 1132.9367893954002,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 839.1088302566768,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 1054.1898561489488,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1125.6096597875364,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 821.7401875017096,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 1075.4870417936104,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 1089.227244893782,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 493.0240519468994,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 578.4831769782711,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 591.3881611008218,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 470.54129099489126,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 575.0308275845204,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 588.7980275492929,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 475.2178103332307,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 555.6590527650733,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 561.3799878376168,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Yicong Huang",
            "username": "Yicong-Huang",
            "email": "17627829+Yicong-Huang@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "1a584332af50eb21e715df91a108a3d7c81736cc",
          "message": "test(amber): make PveResourceSpec hermetic via process-runner seam (#6024)\n\n### What changes were proposed in this PR?\n\n`PveResourceSpec` runs in the `amber` unit CI job\n(`AMBER_TEST_FILTER=skip-integration`) but is not tagged\n`@IntegrationTest`, so every run shelled out to **real `pip` over the\nnetwork**:\n\n- each `createNewPve` ran `pip install -r requirements.txt` (the full\namber dependency set), and\n- the lazy `resolveSystemPackages()` installed `requirements.txt` into a\nthrowaway venv, then `pip freeze`\n\n— roughly 6 full installs per spec run. That made a PR-blocking unit\nspec **network-dependent (flaky)** and **slow**.\n\nThis PR funnels every child process in `PveManager` (venv creation, pip\ninstall / uninstall / freeze) through a single injectable seam:\n\n```scala\nprivate[pythonvirtualenvironment] var runProcess: ProcessRunner =\n  (command, env, logger) => Process(command, None, env: _*).!(logger)\n```\n\nProduction wiring is unchanged (the default runner executes the command\nfor real). `PveResourceSpec` swaps in a **ScalaMock `mockFunction`**\nwhose handler fabricates the `<venv>/bin/{python,pip}` layout, emits the\nresolved system set on `freeze`, and returns a configurable exit code —\nso the spec is **fully hermetic: no venv, no pip, no network**.\n`PveManager` still owns the metadata files (`user-packages.txt`) and\nqueue messages, so that logic stays under test.\n\nBecause failures are now cheap to trigger, this also adds negative\ncoverage that was previously impractical: venv-create failure,\nrequirements-install failure, user-package install failure, and\nsystem-package rejection (using `pyarrow`, a hard amber dependency).\n\n| Before | After |\n| --- | --- |\n| ~6 real `pip install` per run, hits PyPI | 0 network calls |\n| flaky on network hiccups, ~minutes | deterministic, ~8s |\n| only happy-path assertions | + 4 negative/failure cases |\n\n### Any related issues, documentation, discussions?\n\nCloses #6023\n\n### How was this PR tested?\n\n`PveManager` is an `object`, so the test points the shared `runProcess`\nat the ScalaMock `mockFunction` in `beforeAll` and restores the real\nrunner in `afterAll`. ScalaMock expectations are per-test, so\n`expectProcessCalls()` (an `anyNumberOfTimes` handler) is invoked at the\ntop of each test that exercises a process. Run with JDK 17:\n\n```bash\nSTORAGE_JDBC_USERNAME=texera STORAGE_JDBC_PASSWORD=password \\\n  sbt \"WorkflowExecutionService/testOnly org.apache.texera.web.resource.pythonvirtualenvironment.PveResourceSpec\"\n# -> Tests: succeeded 25, failed 0, in ~8s\n```\n\nAlso green:\n\n```bash\nsbt scalafmtCheckAll\nsbt \"WorkflowExecutionService/scalafixAll --check\"\n```\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Claude Opus 4.8)",
          "timestamp": "2026-06-30T04:15:16Z",
          "url": "https://github.com/apache/texera/commit/1a584332af50eb21e715df91a108a3d7c81736cc"
        },
        "date": 1782826181223,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 681.2959889589445,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1312.778569827959,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1423.978197080452,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 922.5130691803814,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1369.736165246893,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1428.764271018029,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 971.8370741462542,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1356.8031697475005,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1433.183283986549,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 782.4761747047497,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 1064.4517303000414,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 1122.727218782603,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 818.4003571171799,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 1075.655070655937,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1112.4987833349658,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 816.1147854673045,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 1061.2812101651248,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 1092.8918803895347,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 459.6282325675716,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 558.0175195359557,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 586.7974078997694,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 485.14321046614253,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 575.4585023055071,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 581.6297953131998,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 447.2204251042123,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 550.1748854002384,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 554.2242150100697,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xiaozhen Liu",
            "username": "Xiao-zhen-Liu",
            "email": "xiaozl3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "104bcc40cbda04621b37bcc5fb6f3ca58cb99f46",
          "message": "feat(dao): add operator_port_cache table (#5967)\n\n### What changes were proposed in this PR?\n\nAdds the `operator_port_cache` table that records a materialized output\nport\nresult so it can be reused across executions. It is keyed by\n`(workflow_id, global_port_id, cache_key)` and stores the JSON the cache\nkey was\ncomputed from, the result location, an optional tuple count and source\nexecution\nid, and a database-managed `updated_at`. The foreign key to\n`workflow(wid)` is\n`ON DELETE CASCADE`. The stored JSON (`cache_key_json`) lets a lookup\nconfirm a\nhash match by comparing the full JSON, so a hash collision never reuses\nthe wrong\nresult.\n\nThe change is additive: a new table in `sql/texera_ddl.sql` (fresh\ninstalls) plus\na Liquibase migration `sql/updates/26.sql` registered in\n`sql/changelog.xml`\n(existing deployments). No code reads or writes the table yet; the cache\nread/write\nlogic and its tests land with the cache service that uses it, following\nthe\nconvention of testing a table through its consumer (as `feedback` is\ntested via\n`FeedbackResourceSpec`).\n\n### Any related issues, documentation, discussions?\n\nCloses #5969. Part of the storage foundation #5882 (umbrella #5881).\nDesign discussion: #5880.\n\n### How was this PR tested?\n\nVerified the schema directly against Postgres: the migration applies\ncleanly, the\ncolumns and primary key `(workflow_id, global_port_id, cache_key)` are\ncorrect,\nthe foreign key's delete rule is `CASCADE`, the schema file and the\nmigration\ndefine identical columns/keys, and `changelog.xml` is well-formed and\nregisters\n`26.sql`. The generated jOOQ classes build from the table. The table's\nruntime\nbehavior is exercised by the cache service tests in the follow-up PR.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Opus 4.8 (Claude Code)",
          "timestamp": "2026-07-01T06:34:40Z",
          "url": "https://github.com/apache/texera/commit/104bcc40cbda04621b37bcc5fb6f3ca58cb99f46"
        },
        "date": 1782913459858,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 735.5412473372833,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1361.5029699251231,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1456.9297938896964,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 961.6253730547503,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1367.0123255004726,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1446.6996873600917,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 956.6016199407511,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1372.2447323163062,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1449.5385930758175,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 796.5419564735224,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 1080.471507907829,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 1147.1421266106638,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 860.247949275022,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 1090.805486640292,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1137.9854322159567,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 845.3061099513875,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 1086.6339264063856,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 1117.912791870422,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 505.1695367707192,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 597.0213782475134,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 608.1181272359718,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 505.01037508969904,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 592.3938463528016,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 597.2740486594646,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 477.84133266602794,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 558.6807736696218,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 571.1931372419966,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "24b587fcbf30bce1a4614a22bbf1c5a6ad1e158e",
          "message": "test(workflow-core): fill uncovered branches in tuple and type utilities (#6055)\n\n### What changes were proposed in this PR?\n\nFill uncovered code paths in five `workflow-core` tuple/type utility\nclasses, selected from the Codecov report (not spec-name gaps). No\nproduction-code changes.\n\n| File | Codecov before | Missed lines targeted |\n| --- | --- | --- |\n| `Tuple.scala` | 54.1% | 24 — `getField` miss/Attribute overload,\n`enforceSchema`, equals branches (incl. binary contents / non-Tuple),\n`getPartialTuple`, `toString`, size/type-mismatch throws, 3-arg builder\n`add` + null guards, case-class synthetics |\n| `TupleLike.scala` | 12.8% | 32 — `SeqTupleLike`/`MapTupleLike`\n`enforceSchema` (positional vs name-based, null fill, extras dropped),\n`inMemSize` `???`, all seven `TupleLike.apply` overloads, the\n`NotAnIterable` guard implicit |\n| `AttributeType.java` | 47.2% | 18 — `getName` ANY branch (`\"\"`), the\nfull `getAttributeType(Class)` mapping incl. primitive/unknown fallback\nto ANY |\n| `AttributeTypeUtils.scala` | 71.7% | 20 (+29 partials) —\n`SchemaCasting`, `tupleCasting`, both `parseFields` overloads,\nnull-inference early-returns, BINARY/ANY inference fallback, LONG/DOUBLE\n`compare`, unsupported-type throws, forced-parse failures |\n| `ArrowUtils.scala` | 67.8% | 15 —\n`appendTexeraTuple`/`setTexeraTuple`/`getTexeraTuple` round-trip (all 7\ntypes + nulls + row indices), parse-failure → null field,\n`fromAttributeType(null)` throw, unrecognized `texera_type` metadata\nfallback |\n\nExtends the existing `TupleSpec`, `AttributeTypeUtilsSpec`, and\n`ArrowUtilsSpec` (the operator-module `ArrowUtilsSpec` doesn't attribute\ncoverage to `workflow-core` sources), and adds `TupleLikeSpec` /\n`AttributeTypeSpec`.\n\nBehavior notes pinned along the way: `$attrType` error messages render\nthe lowercase wire name; the `NotAnIterable` iterable-guard implicit is\nnot summoned for multi-iterable varargs under Scala 2.13 inference\n(pinned by direct invocation).\n\n### Any related issues, documentation, discussions?\n\nFollow-up to the review feedback on #6043: prioritize tests that fill\nuncovered code paths.\n\n### How was this PR tested?\n\n- `sbt \"WorkflowCore/testOnly *TupleSpec *TupleLikeSpec\n*AttributeTypeSpec *AttributeTypeUtilsSpec\norg.apache.texera.amber.util.ArrowUtilsSpec\"` — 105 tests, all green\n- `sbt \"WorkflowCore/Test/scalafmtCheck\"` and `sbt\n\"WorkflowCore/scalafixAll --check\"` — clean\n- CI + Codecov delta to confirm\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-02T07:11:04Z",
          "url": "https://github.com/apache/texera/commit/24b587fcbf30bce1a4614a22bbf1c5a6ad1e158e"
        },
        "date": 1782999506038,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 588.7047598450578,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1064.1000721050223,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1142.975328040915,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 797.0912768311435,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1089.8785455299235,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1139.8040062760597,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 808.0953662045206,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1103.1893799261784,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1129.5223204413157,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 653.6539914390048,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 886.0517365004655,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 910.5230187065033,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 663.7194633103622,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 875.0491161896864,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 912.0864069663311,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 676.2363125890427,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 858.6936003507558,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 898.5233819225396,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 420.8437738133003,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 503.09077355980054,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 516.4183775008889,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 422.43115820320907,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 500.9279268690094,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 509.01057304311416,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 414.0423321467976,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 485.72921753212154,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 494.40281862077387,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "48ab6ca5d627e6d8c9c23e5edfa6556a3681fb07",
          "message": "test(workflow-operator): add unit test coverage for scan source executors (#6076)\n\n### What changes were proposed in this PR?\n\nAdd unit test coverage for the four file-reading scan source\n**executors**, selected from the Codecov report. No production-code\nchanges. Every test is driven by a local temp file (`file:` URI →\n`ReadonlyLocalFileDocument`); no live DB/network/S3/Iceberg.\n\n| File | Codecov before | Missed lines targeted |\n| --- | --- | --- |\n| `ParallelCSVScanSourceOpExec.scala` | 0% | 43 — construct + schema\ninit, `open` byte-range partitioning (both `endOffset` branches +\nmid-file partial-line skip + header skip), `produceTuple` happy path,\nshort-row null padding, all-null (blank) line discard, `close` |\n| `JSONLScanSourceOpExec.scala` | 0% | 23 — construct, `produceTuple`,\n`open` line counting + worker partitioning (both ternary branches), row\nlimit, `close` |\n| `CSVOldScanSourceOpExec.scala` | 0% | 21 — construct + header-based\nschema, `open` (header vs no-header start offset), `produceTuple` +\nlimit, `close` null-guard before open |\n| `ArrowSourceOpExec.scala` | 0% | 30 — construct, `open` success +\nerror-wrapping path, `produceTuple` batch iteration + offset/limit,\n`close` no-op before open |\n\nNew specs live in each executor's own package so their `private[...]`\nconstructors are reachable, mirroring the existing\n`FileScanSourceOpExecSpec` temp-file/URI pattern.\n\n### Any related issues, documentation, discussions?\n\nFollow-up to the review feedback on #6043: prioritize tests that fill\nuncovered code paths.\n\n### How was this PR tested?\n\n- `sbt \"WorkflowOperator/testOnly *ParallelCSVScanSourceOpExecSpec\n*JSONLScanSourceOpExecSpec *CSVOldScanSourceOpExecSpec\n*ArrowSourceOpExecSpec\"` — 17 tests, all green\n- `sbt \"WorkflowOperator/Test/scalafmtCheck\"` and `sbt\n\"WorkflowOperator/scalafixAll --check\"` — clean\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-03T09:24:10Z",
          "url": "https://github.com/apache/texera/commit/48ab6ca5d627e6d8c9c23e5edfa6556a3681fb07"
        },
        "date": 1783085357287,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 697.0359116637802,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1176.9461154513542,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1222.9743218695237,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 895.5181221856138,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1206.4328288351471,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1231.1328003542994,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 913.1748307379884,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1214.901418223681,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1235.6412036604415,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 754.9730269578702,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 969.1679701417786,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 987.6712071705803,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 771.6571602890104,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 975.8931799457536,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 990.9893089660884,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 787.682331145418,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 953.7409171695004,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 985.6329380130454,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 479.5673381840291,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 556.1770524202138,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 557.9507054844521,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 482.88230563058374,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 548.4916041448256,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 563.7031353804261,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 468.20844216959523,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 529.7033333155732,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 521.699286930587,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "4c9d30a132672b9a0e8ecfe1d480a473dae16e4d",
          "message": "test(config): add unit test coverage for the remaining config objects (#6094)\n\n### What changes were proposed in this PR?\n\nAdd unit test coverage for the remaining `common/config` objects,\nselected from the Codecov report (all 0%). No production-code changes.\n\n| File | Codecov before | What the tests pin |\n| --- | --- | --- |\n| `UdfConfig.scala` | 0% | python/R path + log handler defaults from\nudf.conf |\n| `DefaultsConfig.scala` | 0% | the `reinit` flag and the flattened\n`allDefaults` short-key/value map from default.conf |\n| `ComputingUnitConfig.scala` | 0% | local/sharing enabled flags |\n| `LLMConfig.scala` | 0% | LiteLLM base URL + master key |\n| `PekkoConfig.scala` | 0% | actor/serialization, remote/artery, and\ncluster/failure-detector settings from cluster.conf (no ActorSystem\nstarted) |\n| `PythonUtils.scala` | 0% | `getPythonExecutable` blank→`python3`\nfallback and trimmed-path branch |\n\nReading each value forces resolution from the backing `.conf`;\nenv/system-property-overridable values are guarded on the override being\nunset (mirroring `StorageConfigSpec`).\n\n### Any related issues, documentation, discussions?\n\nFollow-up to the review feedback on #6043: prioritize tests that fill\nuncovered code paths.\n\n### How was this PR tested?\n\n- `sbt \"Config/testOnly *UdfConfigSpec *DefaultsConfigSpec\n*ComputingUnitConfigSpec *LLMConfigSpec *PekkoConfigSpec\n*PythonUtilsSpec\"` — 13 tests, all green\n- `sbt \"Config/Test/scalafmtCheck\"` and `sbt \"Config/scalafixAll\n--check\"` — clean\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-04T08:27:46Z",
          "url": "https://github.com/apache/texera/commit/4c9d30a132672b9a0e8ecfe1d480a473dae16e4d"
        },
        "date": 1783171115987,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 628.6774493292364,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1099.5336379477842,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1160.926796366704,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 825.6405953107962,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1138.5462319485064,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1163.7590553257671,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 855.2383928634995,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1138.308185665074,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1167.0969087159767,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 690.4664123538936,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 901.5390593850734,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 949.8059214079989,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 748.6239839193831,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 929.4526526030608,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 945.0581745651697,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 736.4539831222856,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 906.0498549715942,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 924.545574281402,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 455.084668130474,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 532.0603534413926,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 539.7429007518159,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 459.1515536747201,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 529.4018061923457,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 539.1787075621016,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 443.9245127550138,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 512.3245609702237,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 515.1297233233323,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "PJ Fanning",
            "username": "pjfanning",
            "email": "pjfanning@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "6fdfe19106d64cfb33fb431ac82390966afc3e52",
          "message": "chore(deps): Upgrade to Pekko 1.6.0 (#6151)\n\n### What changes were proposed in this PR?\n\nBump Apache Pekko from 1.2.1 to 1.6.0: `pekkoVersion` in\n`amber/build.sbt`, plus the 11 corresponding `org.apache.pekko.*` jar\nentries in `amber/LICENSE-binary-java`.\n\nPekko is amber's actor runtime (actor, remote, cluster, persistence,\nstream, …); all modules move together on the shared version. The jump\nspans the 1.3.x → 1.6.0 maintenance lines — bug-fix and performance\nreleases that stay binary compatible within the 1.x series ([1.6.x\nrelease\nnotes](https://pekko.apache.org/docs/pekko/current/release-notes/releases-1.6.html)).\nNo other bundled jars change (transitive dependencies are unaffected),\nso the LICENSE-binary diff is limited to the Pekko version strings.\n\n### Any related issues, documentation, discussions?\n\nPekko release notes:\n[1.6.x](https://pekko.apache.org/docs/pekko/current/release-notes/releases-1.6.html)\n(see the linked notes for 1.3.x–1.5.x covered by this jump).\n\n### How was this PR tested?\n\nExisting test cases via the label-gated `engine` CI stack (amber unit\ntests + amber-integration on Linux/macOS); dependency bump only, no code\nchange. The binary licensing check verifies the bundled jars against\n`amber/LICENSE-binary-java`.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nThe dependency bump was authored by @pjfanning (no AI declaration). This\nPR description was written with generative AI tooling: Generated-by:\nClaude Code (Claude Fable 5).",
          "timestamp": "2026-07-05T09:52:42Z",
          "url": "https://github.com/apache/texera/commit/6fdfe19106d64cfb33fb431ac82390966afc3e52"
        },
        "date": 1783257789374,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 632.5292182513958,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1088.8320862538214,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1133.3048641787143,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 833.6507468972779,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1116.3247562978831,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1140.2907795312951,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 862.9556338328229,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1110.8919702660019,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1154.9971067216218,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 692.948144673162,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 903.3276806644683,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 926.3806795967888,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 741.6442392212653,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 908.7378255764775,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 925.3923938844937,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 720.6241830486313,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 893.3756394130886,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 913.9300796005045,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 443.02230641457516,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 517.5400539017661,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 524.190147588425,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 435.88876475106207,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 517.3913612176849,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 520.2368964269174,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 425.0539925046506,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 496.99935672953296,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 507.2166312523217,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "233c4deb2bd646a5e52eea042384caa28400dbf6",
          "message": "test(workflow-core): add unit tests for PhysicalFileNode, Schema, and VFSURIFactory (#6127)\n\n### What changes were proposed in this PR?\n\nAdds unit-test coverage for three value/utility classes in\n`common/workflow-core` that Codecov reports as having uncovered lines:\n\n- **`PhysicalFileNodeSpec`** (new) — covers the `PhysicalFileNode` value\nclass: constructor/getters, `isFile`/`isDirectory`, `addChildNode`\nsuccess and the direct-subpath guard failure, `equals`/`hashCode` (which\ndeliberately excludes `size`), and the recursive\n`getAllFileRelativePaths` traversal.\n- **`SchemaSpec`** (extended) — adds `hashCode`/`equals` cases\n(including non-`Schema` operands), both duplicate-name rejection paths\n(`add(Iterable)` and `add(Attribute)`), and a Jackson JSON round-trip.\n- **`VFSURIFactorySpec`** (extended) — adds the missing-value guard\nwhere a required key is the final path segment.\n\nNo production code is changed; this is test-only.\n\n### Any related issues, documentation, discussions?\n\nCoverage gaps identified from the Codecov report for `apache/texera`.\n\n### How was this PR tested?\n\nNew/extended ScalaTest specs, run locally:\n\n```\nsbt \"WorkflowCore/testOnly *PhysicalFileNodeSpec *SchemaSpec *VFSURIFactorySpec\"\n```\n\nAll pass. `scalafmt` and `scalafixAll --check` are clean.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-06T07:35:15Z",
          "url": "https://github.com/apache/texera/commit/233c4deb2bd646a5e52eea042384caa28400dbf6"
        },
        "date": 1783346671450,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 673.2294620814026,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1241.59703084049,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1344.4990473840664,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 874.189574195804,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1277.3011375768021,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1376.0093784693224,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 928.518055164968,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1314.9250017617283,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1367.5097424729922,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 750.272201849704,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 1028.4462485917052,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 1072.241016381267,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 770.0082175045969,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 1042.7979210619428,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1066.8313950099348,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 775.643435341436,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 1020.9011547140534,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 1041.882146664755,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 452.87360842350563,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 558.9014988042602,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 564.0104044711536,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 461.2365346482118,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 552.3583760385278,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 565.9613855774332,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 434.344510693005,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 533.9810977434512,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 547.7026928948011,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "40526b8e1783e7782ce6043e7ca72840b7fbe04d",
          "message": "test(amber): add NetworkOutputGateway SELF-rewrite and FIFO-state tests (#6159)\n\n### What changes were proposed in this PR?\n\nAdds `NetworkOutputGatewaySpec` (new) for the amber messaging layer's\n`NetworkOutputGateway`, which had no spec. Using a hand-rolled capture\nhandler (no actor system), it covers the previously-untested paths:\n\n- `sendTo(ChannelIdentity, payload)` rewriting a `SELF` receiver to the\nactor id, and passing a non-`SELF` channel through verbatim;\n- `sendTo(SELF, DataPayload)` folding `SELF` to the actor id;\n- per-channel sequence-number increment and `getFIFOState`.\n\nNo production code changed.\n\n### Any related issues, documentation, discussions?\n\nCoverage gaps identified from the Codecov report for `apache/texera`\n(amber module coverage push toward 80%).\n\n### How was this PR tested?\n\n```\nsbt \"WorkflowExecutionService/testOnly *NetworkOutputGatewaySpec\"\n```\n\nAll 5 pass. `scalafmt` and `scalafixAll --check` are clean.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-07T08:18:50Z",
          "url": "https://github.com/apache/texera/commit/40526b8e1783e7782ce6043e7ca72840b7fbe04d"
        },
        "date": 1783431814887,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 657.365238224735,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1079.4549671291245,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1139.269698664376,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 714.6690691553831,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1128.9564587442926,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1152.794075374318,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 832.2110080198444,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1111.7623719968822,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1142.0745700764576,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 690.652357960411,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 900.117723998308,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 927.0268406072661,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 693.2132476512147,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 898.0691317663436,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 919.8657384685348,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 729.7952946967989,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 899.1106359481232,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 915.8496642510331,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 444.97523473533266,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 522.2348145816264,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 519.2399046365994,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 449.77029565340945,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 517.77612395326,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 522.3335931883134,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 429.96440504630726,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 491.0950697477734,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 494.7431117194836,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "18ccda990415e70fc835b1728585cea4d211888f",
          "message": "chore(deps, frontend): bump the frontend-patch group in /frontend with 4 updates (#6233)\n\nBumps the frontend-patch group in /frontend with 4 updates:\n[@vitest/browser](https://github.com/vitest-dev/vitest/tree/HEAD/packages/browser),\n[@vitest/browser-playwright](https://github.com/vitest-dev/vitest/tree/HEAD/packages/browser-playwright),\n[@vitest/coverage-v8](https://github.com/vitest-dev/vitest/tree/HEAD/packages/coverage-v8)\nand\n[vitest](https://github.com/vitest-dev/vitest/tree/HEAD/packages/vitest).\n\nUpdates `@vitest/browser` from 4.1.9 to 4.1.10\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/vitest-dev/vitest/releases\">@​vitest/browser's\nreleases</a>.</em></p>\n<blockquote>\n<h2>v4.1.10</h2>\n<h3>   🐞 Bug Fixes</h3>\n<ul>\n<li><strong>browser</strong>: Check fs access in builtin commands\n[backport to v4]  -  by <a\nhref=\"https://github.com/hi-ogawa\"><code>@​hi-ogawa</code></a>,\n<strong>Hiroshi Ogawa</strong> and <strong>OpenCode\n(claude-opus-4-8)</strong> in <a\nhref=\"https://redirect.github.com/vitest-dev/vitest/issues/10680\">vitest-dev/vitest#10680</a>\n<a href=\"https://github.com/vitest-dev/vitest/commit/5c18dd267\"><!-- raw\nHTML omitted -->(5c18d)<!-- raw HTML omitted --></a></li>\n<li><strong>vm</strong>: Fix external module resolve error with deps\noptimizer query for encoded URI [backport to v4]  -  by <a\nhref=\"https://github.com/SveLil\"><code>@​SveLil</code></a> and <a\nhref=\"https://github.com/hi-ogawa\"><code>@​hi-ogawa</code></a> in <a\nhref=\"https://redirect.github.com/vitest-dev/vitest/issues/10661\">vitest-dev/vitest#10661</a>\n<a href=\"https://github.com/vitest-dev/vitest/commit/bae52b511\"><!-- raw\nHTML omitted -->(bae52)<!-- raw HTML omitted --></a></li>\n</ul>\n<h5>    <a\nhref=\"https://github.com/vitest-dev/vitest/compare/v4.1.9...v4.1.10\">View\nchanges on GitHub</a></h5>\n</blockquote>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a\nhref=\"https://github.com/vitest-dev/vitest/commit/db616d227b6e0cb07a94f5d1bba262ee95db7e46\"><code>db616d2</code></a>\nchore: release v4.1.10 (<a\nhref=\"https://github.com/vitest-dev/vitest/tree/HEAD/packages/browser/issues/10718\">#10718</a>)</li>\n<li><a\nhref=\"https://github.com/vitest-dev/vitest/commit/5c18dd267ff7f47f24cab2f615a16b37d90feb7f\"><code>5c18dd2</code></a>\nfix(browser): check fs access in builtin commands [backport to v4] (<a\nhref=\"https://github.com/vitest-dev/vitest/tree/HEAD/packages/browser/issues/10680\">#10680</a>)</li>\n<li>See full diff in <a\nhref=\"https://github.com/vitest-dev/vitest/commits/v4.1.10/packages/browser\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\nUpdates `@vitest/browser-playwright` from 4.1.9 to 4.1.10\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/vitest-dev/vitest/releases\">@​vitest/browser-playwright's\nreleases</a>.</em></p>\n<blockquote>\n<h2>v4.1.10</h2>\n<h3>   🐞 Bug Fixes</h3>\n<ul>\n<li><strong>browser</strong>: Check fs access in builtin commands\n[backport to v4]  -  by <a\nhref=\"https://github.com/hi-ogawa\"><code>@​hi-ogawa</code></a>,\n<strong>Hiroshi Ogawa</strong> and <strong>OpenCode\n(claude-opus-4-8)</strong> in <a\nhref=\"https://redirect.github.com/vitest-dev/vitest/issues/10680\">vitest-dev/vitest#10680</a>\n<a href=\"https://github.com/vitest-dev/vitest/commit/5c18dd267\"><!-- raw\nHTML omitted -->(5c18d)<!-- raw HTML omitted --></a></li>\n<li><strong>vm</strong>: Fix external module resolve error with deps\noptimizer query for encoded URI [backport to v4]  -  by <a\nhref=\"https://github.com/SveLil\"><code>@​SveLil</code></a> and <a\nhref=\"https://github.com/hi-ogawa\"><code>@​hi-ogawa</code></a> in <a\nhref=\"https://redirect.github.com/vitest-dev/vitest/issues/10661\">vitest-dev/vitest#10661</a>\n<a href=\"https://github.com/vitest-dev/vitest/commit/bae52b511\"><!-- raw\nHTML omitted -->(bae52)<!-- raw HTML omitted --></a></li>\n</ul>\n<h5>    <a\nhref=\"https://github.com/vitest-dev/vitest/compare/v4.1.9...v4.1.10\">View\nchanges on GitHub</a></h5>\n</blockquote>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a\nhref=\"https://github.com/vitest-dev/vitest/commit/db616d227b6e0cb07a94f5d1bba262ee95db7e46\"><code>db616d2</code></a>\nchore: release v4.1.10 (<a\nhref=\"https://github.com/vitest-dev/vitest/tree/HEAD/packages/browser-playwright/issues/10718\">#10718</a>)</li>\n<li><a\nhref=\"https://github.com/vitest-dev/vitest/commit/5c18dd267ff7f47f24cab2f615a16b37d90feb7f\"><code>5c18dd2</code></a>\nfix(browser): check fs access in builtin commands [backport to v4] (<a\nhref=\"https://github.com/vitest-dev/vitest/tree/HEAD/packages/browser-playwright/issues/10680\">#10680</a>)</li>\n<li>See full diff in <a\nhref=\"https://github.com/vitest-dev/vitest/commits/v4.1.10/packages/browser-playwright\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\nUpdates `@vitest/coverage-v8` from 4.1.9 to 4.1.10\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/vitest-dev/vitest/releases\">@​vitest/coverage-v8's\nreleases</a>.</em></p>\n<blockquote>\n<h2>v4.1.10</h2>\n<h3>   🐞 Bug Fixes</h3>\n<ul>\n<li><strong>browser</strong>: Check fs access in builtin commands\n[backport to v4]  -  by <a\nhref=\"https://github.com/hi-ogawa\"><code>@​hi-ogawa</code></a>,\n<strong>Hiroshi Ogawa</strong> and <strong>OpenCode\n(claude-opus-4-8)</strong> in <a\nhref=\"https://redirect.github.com/vitest-dev/vitest/issues/10680\">vitest-dev/vitest#10680</a>\n<a href=\"https://github.com/vitest-dev/vitest/commit/5c18dd267\"><!-- raw\nHTML omitted -->(5c18d)<!-- raw HTML omitted --></a></li>\n<li><strong>vm</strong>: Fix external module resolve error with deps\noptimizer query for encoded URI [backport to v4]  -  by <a\nhref=\"https://github.com/SveLil\"><code>@​SveLil</code></a> and <a\nhref=\"https://github.com/hi-ogawa\"><code>@​hi-ogawa</code></a> in <a\nhref=\"https://redirect.github.com/vitest-dev/vitest/issues/10661\">vitest-dev/vitest#10661</a>\n<a href=\"https://github.com/vitest-dev/vitest/commit/bae52b511\"><!-- raw\nHTML omitted -->(bae52)<!-- raw HTML omitted --></a></li>\n</ul>\n<h5>    <a\nhref=\"https://github.com/vitest-dev/vitest/compare/v4.1.9...v4.1.10\">View\nchanges on GitHub</a></h5>\n</blockquote>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a\nhref=\"https://github.com/vitest-dev/vitest/commit/db616d227b6e0cb07a94f5d1bba262ee95db7e46\"><code>db616d2</code></a>\nchore: release v4.1.10 (<a\nhref=\"https://github.com/vitest-dev/vitest/tree/HEAD/packages/coverage-v8/issues/10718\">#10718</a>)</li>\n<li>See full diff in <a\nhref=\"https://github.com/vitest-dev/vitest/commits/v4.1.10/packages/coverage-v8\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\nUpdates `vitest` from 4.1.9 to 4.1.10\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/vitest-dev/vitest/releases\">vitest's\nreleases</a>.</em></p>\n<blockquote>\n<h2>v4.1.10</h2>\n<h3>   🐞 Bug Fixes</h3>\n<ul>\n<li><strong>browser</strong>: Check fs access in builtin commands\n[backport to v4]  -  by <a\nhref=\"https://github.com/hi-ogawa\"><code>@​hi-ogawa</code></a>,\n<strong>Hiroshi Ogawa</strong> and <strong>OpenCode\n(claude-opus-4-8)</strong> in <a\nhref=\"https://redirect.github.com/vitest-dev/vitest/issues/10680\">vitest-dev/vitest#10680</a>\n<a href=\"https://github.com/vitest-dev/vitest/commit/5c18dd267\"><!-- raw\nHTML omitted -->(5c18d)<!-- raw HTML omitted --></a></li>\n<li><strong>vm</strong>: Fix external module resolve error with deps\noptimizer query for encoded URI [backport to v4]  -  by <a\nhref=\"https://github.com/SveLil\"><code>@​SveLil</code></a> and <a\nhref=\"https://github.com/hi-ogawa\"><code>@​hi-ogawa</code></a> in <a\nhref=\"https://redirect.github.com/vitest-dev/vitest/issues/10661\">vitest-dev/vitest#10661</a>\n<a href=\"https://github.com/vitest-dev/vitest/commit/bae52b511\"><!-- raw\nHTML omitted -->(bae52)<!-- raw HTML omitted --></a></li>\n</ul>\n<h5>    <a\nhref=\"https://github.com/vitest-dev/vitest/compare/v4.1.9...v4.1.10\">View\nchanges on GitHub</a></h5>\n</blockquote>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a\nhref=\"https://github.com/vitest-dev/vitest/commit/db616d227b6e0cb07a94f5d1bba262ee95db7e46\"><code>db616d2</code></a>\nchore: release v4.1.10 (<a\nhref=\"https://github.com/vitest-dev/vitest/tree/HEAD/packages/vitest/issues/10718\">#10718</a>)</li>\n<li><a\nhref=\"https://github.com/vitest-dev/vitest/commit/bae52b5112a6fd8200101b88bf8af9685d077295\"><code>bae52b5</code></a>\nfix(vm): fix external module resolve error with deps optimizer query for\nenco...</li>\n<li>See full diff in <a\nhref=\"https://github.com/vitest-dev/vitest/commits/v4.1.10/packages/vitest\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\n\nDependabot will resolve any conflicts with this PR as long as you don't\nalter it yourself. You can also trigger a rebase manually by commenting\n`@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits\nthat have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all\nof the ignore conditions of the specified dependency\n- `@dependabot ignore <dependency name> major version` will close this\ngroup update PR and stop Dependabot creating any more for the specific\ndependency's major version (unless you unignore this specific\ndependency's major version or upgrade to it yourself)\n- `@dependabot ignore <dependency name> minor version` will close this\ngroup update PR and stop Dependabot creating any more for the specific\ndependency's minor version (unless you unignore this specific\ndependency's minor version or upgrade to it yourself)\n- `@dependabot ignore <dependency name>` will close this group update PR\nand stop Dependabot creating any more for the specific dependency\n(unless you unignore this specific dependency or upgrade to it yourself)\n- `@dependabot unignore <dependency name>` will remove all of the ignore\nconditions of the specified dependency\n- `@dependabot unignore <dependency name> <ignore condition>` will\nremove the ignore condition of the specified dependency and ignore\nconditions\n\n\n</details>\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>\nCo-authored-by: Xinyuan Lin <xinyual3@uci.edu>",
          "timestamp": "2026-07-08T06:26:15Z",
          "url": "https://github.com/apache/texera/commit/18ccda990415e70fc835b1728585cea4d211888f"
        },
        "date": 1783516935727,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 738.389529624065,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1316.3451863847793,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1447.403107466287,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 903.0017576753128,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1352.0891821242399,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1443.2101960881923,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 960.5931644346996,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1357.8381486028052,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1437.4133425288583,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 812.6546920204539,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 1075.0843232551924,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 1104.3727890607338,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 832.8921007610725,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 1077.1817148899186,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1102.489418035491,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 834.1160361947989,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 1056.994668130237,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 1084.4994328016942,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 490.6326563563334,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 569.649397963647,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 572.129137136071,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 470.7727278030421,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 565.2911892555774,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 571.7315256128732,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 464.2501371976959,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 520.9758513922707,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 548.2133332939372,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "29233cc86ed8687f5eafd6b5b7dd4e51b7000c84",
          "message": "fix(deps, pyamber): bump aiobotocore to 3.7.0 with s3fs/botocore/boto3 in /amber (#6228)\n\nBumps [aiobotocore](https://github.com/aio-libs/aiobotocore) from 2.26.0\nto 3.7.0.\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/aio-libs/aiobotocore/releases\">aiobotocore's\nreleases</a>.</em></p>\n<blockquote>\n<h2>3.7.0</h2>\n<ul>\n<li>replace per-PR <code>CHANGES.rst</code> / <code>__init__.py</code>\nceremony with an AI-drafted\nrelease flow: contributors no longer touch version or changelog files; a\nworkflow-triggered agent synthesizes merged PRs into a release PR at\nrelease\ntime (closes <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1167\">#1167</a>)\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1592\">#1592</a>)</li>\n<li>add <code>AioSession.warm_up_loader_caches()</code> and\n<code>warm_up_loader_caches</code>\noption in <code>AioConfig</code> to pre-populate botocore loader caches\noff the event\nloop, avoiding blocking file I/O on first client/waiter/paginator use\n(closes <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1199\">#1199</a>)\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1462\">#1462</a>)</li>\n<li>fix race condition in\n<code>AioAssumeRoleProvider._visited_profiles</code> causing\nfalse <code>InfiniteLoopConfigError</code> under concurrent async usage\n(closes <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1455\">#1455</a>)\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1537\">#1537</a>)</li>\n<li>fall back to synchronous <code>subprocess.run</code> (via\n<code>asyncio.to_thread</code>) for\n<code>credential_process</code> when the running event loop does not\nimplement\nsubprocess transports — notably <code>asyncio.SelectorEventLoop</code>\non Windows\n(closes <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1415\">#1415</a>)\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1588\">#1588</a>)</li>\n</ul>\n<h2>3.6.0</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>Relax <code>botocore</code> dependency specification by <a\nhref=\"https://github.com/jakob-keller\"><code>@​jakob-keller</code></a>\nin <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1578\">aio-libs/aiobotocore#1578</a></li>\n<li>Relax <code>botocore</code> dependency specification by <a\nhref=\"https://github.com/jakob-keller\"><code>@​jakob-keller</code></a>\nin <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1579\">aio-libs/aiobotocore#1579</a></li>\n</ul>\n<!-- raw HTML omitted -->\n<ul>\n<li>Bump actions/cache from 5.0.4 to 5.0.5 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1575\">aio-libs/aiobotocore#1575</a></li>\n<li>Bump packaging from 26.0 to 26.1 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1576\">aio-libs/aiobotocore#1576</a></li>\n<li>Bump anthropics/claude-code-action from 1.0.101 to 1.0.105 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1580\">aio-libs/aiobotocore#1580</a></li>\n<li>Bump anthropic from 0.96.0 to 0.97.0 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1581\">aio-libs/aiobotocore#1581</a></li>\n<li>ci(claude): skip dependabot and fork PRs in pull_request trigger by\n<a href=\"https://github.com/thehesiod\"><code>@​thehesiod</code></a> in\n<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1582\">aio-libs/aiobotocore#1582</a></li>\n<li>Revert pull_request → pull_request_target switch (App token 401) by\n<a href=\"https://github.com/thehesiod\"><code>@​thehesiod</code></a> in\n<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1583\">aio-libs/aiobotocore#1583</a></li>\n</ul>\n<!-- raw HTML omitted -->\n<p><strong>Full Changelog</strong>: <a\nhref=\"https://github.com/aio-libs/aiobotocore/compare/3.5.0...3.6.0\">https://github.com/aio-libs/aiobotocore/compare/3.5.0...3.6.0</a></p>\n<h2>3.5.0</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>Relax botocore dependency specification by <a\nhref=\"https://github.com/claude\"><code>@​claude</code></a>[bot] in <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1546\">aio-libs/aiobotocore#1546</a></li>\n<li>Bump botocore dependency specification by <a\nhref=\"https://github.com/claude\"><code>@​claude</code></a>[bot] in <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1571\">aio-libs/aiobotocore#1571</a></li>\n</ul>\n<!-- raw HTML omitted -->\n<ul>\n<li>Fix sync prompt template vars, add MCP commit fallback by <a\nhref=\"https://github.com/thehesiod\"><code>@​thehesiod</code></a> in <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1523\">aio-libs/aiobotocore#1523</a></li>\n<li>Fix Claude bot failing on issue comments by <a\nhref=\"https://github.com/thehesiod\"><code>@​thehesiod</code></a> in <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1536\">aio-libs/aiobotocore#1536</a></li>\n<li>Add dev environment to Claude CI workflow by <a\nhref=\"https://github.com/thehesiod\"><code>@​thehesiod</code></a> in <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1538\">aio-libs/aiobotocore#1538</a></li>\n<li>Bump cryptography from 46.0.6 to 46.0.7 in the uv group across 1\ndirectory by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1541\">aio-libs/aiobotocore#1541</a></li>\n<li>Bump anthropics/claude-code-action from 1.0.75 to 1.0.87 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1542\">aio-libs/aiobotocore#1542</a></li>\n<li>Bump astral-sh/setup-uv from 7.6.0 to 8.0.0 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1543\">aio-libs/aiobotocore#1543</a></li>\n</ul>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Changelog</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/aio-libs/aiobotocore/blob/main/CHANGES.rst\">aiobotocore's\nchangelog</a>.</em></p>\n<blockquote>\n<p>3.7.0 (2026-05-09)\n^^^^^^^^^^^^^^^^^^</p>\n<ul>\n<li>replace per-PR <code>CHANGES.rst</code> / <code>__init__.py</code>\nceremony with an AI-drafted\nrelease flow: contributors no longer touch version or changelog files; a\nworkflow-triggered agent synthesizes merged PRs into a release PR at\nrelease\ntime (closes <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1167\">#1167</a>)\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1592\">#1592</a>)</li>\n<li>add <code>AioSession.warm_up_loader_caches()</code> and\n<code>warm_up_loader_caches</code>\noption in <code>AioConfig</code> to pre-populate botocore loader caches\noff the event\nloop, avoiding blocking file I/O on first client/waiter/paginator use\n(closes <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1199\">#1199</a>)\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1462\">#1462</a>)</li>\n<li>fix race condition in\n<code>AioAssumeRoleProvider._visited_profiles</code> causing\nfalse <code>InfiniteLoopConfigError</code> under concurrent async usage\n(closes <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1455\">#1455</a>)\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1537\">#1537</a>)</li>\n<li>fall back to synchronous <code>subprocess.run</code> (via\n<code>asyncio.to_thread</code>) for\n<code>credential_process</code> when the running event loop does not\nimplement\nsubprocess transports — notably <code>asyncio.SelectorEventLoop</code>\non Windows\n(closes <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1415\">#1415</a>)\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1588\">#1588</a>)</li>\n</ul>\n<p>3.6.0 (2026-04-30)\n^^^^^^^^^^^^^^^^^^</p>\n<ul>\n<li>relax botocore dependency specification to support\n<code>&quot;botocore &gt;= 1.42.90, &lt; 1.43.1&quot;</code></li>\n<li>drop support for Python 3.9 (EOL)</li>\n</ul>\n<p>3.5.0 (2026-04-19)\n^^^^^^^^^^^^^^^^^^</p>\n<ul>\n<li>bump botocore dependency specification to support\n<code>&quot;botocore &gt;= 1.42.90, &lt; 1.42.92&quot;</code></li>\n</ul>\n<p>3.4.0 (2026-04-07)\n^^^^^^^^^^^^^^^^^^</p>\n<ul>\n<li>bump botocore dependency specification to support\n<code>&quot;botocore &gt;= 1.42.79, &lt; 1.42.85&quot;</code></li>\n</ul>\n<p>3.3.0 (2026-03-18)\n^^^^^^^^^^^^^^^^^^</p>\n<ul>\n<li>bump botocore dependency specification to support\n<code>&quot;botocore &gt;= 1.42.62, &lt; 1.42.71&quot;</code></li>\n</ul>\n<p>3.2.1 (2026-03-04)\n^^^^^^^^^^^^^^^^^^</p>\n<ul>\n<li>relax botocore dependency specification to support\n<code>&quot;botocore &gt;= 1.42.53, &lt; 1.42.62&quot;</code></li>\n</ul>\n<p>3.2.0 (2026-02-23)\n^^^^^^^^^^^^^^^^^^</p>\n<ul>\n<li>bump botocore dependency specification to support\n<code>&quot;botocore &gt;= 1.42.53, &lt; 1.42.56&quot;</code></li>\n</ul>\n<p>3.1.3 (2026-02-14)\n^^^^^^^^^^^^^^^^^^</p>\n<ul>\n<li>relax botocore dependency specification to support\n<code>&quot;botocore &gt;= 1.41.0, &lt; 1.42.50&quot;</code></li>\n</ul>\n<p>3.1.2 (2026-02-05)\n^^^^^^^^^^^^^^^^^^</p>\n<ul>\n<li>relax botocore dependency specification to support\n<code>&quot;botocore &gt;= 1.41.0, &lt; 1.42.43&quot;</code></li>\n</ul>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a\nhref=\"https://github.com/aio-libs/aiobotocore/commit/d232742cfc2e1098cb3a448f9e1d78eb0899203d\"><code>d232742</code></a>\nRelease v3.7.0 (<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1595\">#1595</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiobotocore/commit/0fb20e5732b50c1b8336bd952929185fe0528e25\"><code>0fb20e5</code></a>\nfix(draft-release): tighten window boundary + unique-PR count (<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1597\">#1597</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiobotocore/commit/e8bb50e26d754f5753b75a5b1b5f70be90ea1367\"><code>e8bb50e</code></a>\ndocs(CLAUDE.md): canonical branch-creation pattern for Claude workflows\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1596\">#1596</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiobotocore/commit/b8c2266f589dcbfe9f0ad04b3a4fc7491969c850\"><code>b8c2266</code></a>\nfix(draft-release): provision uv + Python in workflow (<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1594\">#1594</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiobotocore/commit/392dfcea287c2348c9924f9578855fb1df065776\"><code>392dfce</code></a>\nfeat: AI-drafted release flow (closes <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1167\">#1167</a>)\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1592\">#1592</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiobotocore/commit/0cbf43792b4a1d8b1286faa13657ea57d2f2b062\"><code>0cbf437</code></a>\nchore(release): consolidate unreleased 3.6.1/3.7.0/3.7.1 into 3.7.0 (<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1593\">#1593</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiobotocore/commit/e332cd4727756da8db07e5a8bfc7ff71a0dc9def\"><code>e332cd4</code></a>\nfix: credential_process on Windows + Selector event loop (closes <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1415\">#1415</a>)\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1588\">#1588</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiobotocore/commit/4d1dbadaa56133a75f1df0267d619ffe4b43ec3f\"><code>4d1dbad</code></a>\nBump anthropics/claude-code-action from 1.0.105 to 1.0.111 (<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1584\">#1584</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiobotocore/commit/bc3c5b5827fd8a8f3d3e89589c5abae67ef42584\"><code>bc3c5b5</code></a>\nSupport warm-up of loader caches (<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1462\">#1462</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiobotocore/commit/804cf28a6f92e9c5fe66d12e02d68783e94b6f02\"><code>804cf28</code></a>\nBump pre-commit from 4.5.1 to 4.6.0 (<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1586\">#1586</a>)</li>\n<li>Additional commits viewable in <a\nhref=\"https://github.com/aio-libs/aiobotocore/compare/2.26.0...3.7.0\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility\nscore](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=aiobotocore&package-manager=pip&previous-version=2.26.0&new-version=3.7.0)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't\nalter it yourself. You can also trigger a rebase manually by commenting\n`@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits\nthat have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all\nof the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop\nDependabot creating any more for this major version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop\nDependabot creating any more for this minor version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop\nDependabot creating any more for this dependency (unless you reopen the\nPR or upgrade to it yourself)\n\n\n</details>\n\n---------\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>\nCo-authored-by: probe <probe@x>\nCo-authored-by: Meng Wang <mengw15@uci.edu>\nCo-authored-by: Xinyuan Lin <xinyual3@uci.edu>",
          "timestamp": "2026-07-09T08:56:32Z",
          "url": "https://github.com/apache/texera/commit/29233cc86ed8687f5eafd6b5b7dd4e51b7000c84"
        },
        "date": 1783605072951,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 798.1122232428943,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1457.688358073928,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1529.8790042741393,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 1011.9827849341614,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1411.520461728827,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1544.1833277589699,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 1077.5297335399944,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1476.334955081573,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1502.4391847040229,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 880.7438961642827,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 1169.2656383690774,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 1209.09339055268,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 897.8977290607681,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 1168.0807255692846,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1188.0810318101412,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 888.6660414628541,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 1139.2568064045022,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 1180.0222443764226,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 542.1844173173074,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 656.4658696455855,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 660.9135337952727,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 568.0489946097862,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 688.67126063579,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 689.4665315273121,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 531.2915785219736,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 623.683599718664,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 636.8665590829294,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Kunwoo (Chris)",
            "username": "kunwp1",
            "email": "143021053+kunwp1@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "45c82a59a3efc3501e5b9edcb5c5cc7b9d6b02b1",
          "message": "fix(frontend): speed up dataset bulk upload with many files (#6306)\n\n### What changes were proposed in this PR?\n\nBulk-uploading many files to a dataset was extremely slow (~1,100 files\ntook ~12 minutes) because the Pending and Finished lists fully\nre-rendered on every change-detection pass, on the same thread driving\nthe uploads (root cause detailed in #5586).\n\nAs proposed in the issue: replace the `queuedFileNames` getter with a\ncached field refreshed only when the queue changes, virtualize the\nPending and Finished lists with `cdk-virtual-scroll` so only visible\nrows are in the DOM, and add `trackBy` to both lists.\n\n### Any related issues, documentation, discussions?\n\nCloses #5586.\n\n### How was this PR tested?\n\nScreen recording of a successful 1,100-file upload with the fix:\n\n\nhttps://github.com/user-attachments/assets/c23d7612-0317-4d18-970b-c98540005eed\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code, Claude Fable 5",
          "timestamp": "2026-07-10T05:17:26Z",
          "url": "https://github.com/apache/texera/commit/45c82a59a3efc3501e5b9edcb5c5cc7b9d6b02b1"
        },
        "date": 1783689755334,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 908.8817716770935,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1710.9051137497954,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1848.354567817492,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 1230.6805717834975,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1786.9541092570737,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1855.2343249849255,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 1234.7829989085544,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1810.5367394001548,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1877.794506045351,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 1018.2390766161657,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 1395.0586333489791,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 1461.5894645454894,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 1057.1888043957226,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 1398.9281463463678,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1440.6643719676526,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 1051.9220204504823,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 1393.8208067908463,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 1420.3604751719429,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 631.3266117704082,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 749.9013264836586,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 758.183777829515,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 633.3621815239687,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 752.6966708665709,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 749.0416625159155,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 607.5354329351657,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 705.8721839804065,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 718.4753112663799,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Meng Wang",
            "username": "mengw15",
            "email": "mengw15@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "ac39ac84aca97b72a35c17ba218c061b52ef1d18",
          "message": "test(computing-unit-managing-service): add unit test coverage for ComputingUnitHelpers (#6330)\n\n### What changes were proposed in this PR?\n\nAdd unit test coverage for `ComputingUnitHelpers.getComputingUnitStatus`\nand `getComputingUnitMetrics`, on their pure `local` and unknown-type\nbranches. Each constructs the jOOQ `WorkflowComputingUnit` pojo via\n`setType` (no DB). No production-code changes.\n\nThe `kubernetes` branch calls the static `KubernetesClient`\n(`getPodByName` / `getPodMetrics`) and needs a mock seam — out of scope\nhere, per the issue. (`WorkflowComputingUnitTypeEnum` defines only\n`local` and `kubernetes`, so an untyped unit exercises the `unknown`\nbranch.)\n\n### Any related issues, documentation, discussions?\n\nCloses #6326.\n\n### How was this PR tested?\n\n```\nsbt \"ComputingUnitManagingService/testOnly *ComputingUnitHelpersSpec\"\n```\n\nAll 4 pass. `scalafmt` and `scalafix --check` are clean. The spec\ntouches no cluster or DB — it constructs pojos and exercises only the\npure `local` / unknown branches.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-11T06:52:52Z",
          "url": "https://github.com/apache/texera/commit/ac39ac84aca97b72a35c17ba218c061b52ef1d18"
        },
        "date": 1783774890323,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 806.5811878944206,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1410.69004154485,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1560.587864323109,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 1052.6965519326045,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1487.5049944532773,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1538.7832705583755,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 1089.103499286766,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1449.5503762682204,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1526.2267279556602,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 892.3610438974238,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 1178.6386352114973,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 1240.05707523797,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 912.051098202711,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 1198.640681966127,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1211.7601591633563,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 897.2897159994825,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 1155.935066089079,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 1179.702422729758,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 560.755209953945,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 671.2949955363746,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 654.9399032807196,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 564.010561109037,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 677.5385062708463,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 656.6505081259908,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 534.2708338950412,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 628.1802705291459,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 635.4583393076158,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Meng Wang",
            "username": "mengw15",
            "email": "mengw15@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "afa8af22b2ac0279fbcc0c610cfe54e3ee0ef0b8",
          "message": "test(frontend): add unit test coverage for formly-utils field helpers (#6365)\n\n### What changes were proposed in this PR?\n\nAdd unit test coverage for the pure `FormlyFieldConfig` helpers in\n`frontend/src/app/common/formly/formly-utils.ts` — `getFieldByName`,\n`setHideExpression`, and the `createShouldHideFieldFunc` predicate\nfactory. A Vitest spec on plain objects (no `TestBed`). No\nproduction-code changes.\n\nThe rxjs `createOutputFormChangeEventStream` (debounce/distinct/share\nstream) is out of scope for this issue.\n\n### Any related issues, documentation, discussions?\n\nCloses #6361. See `frontend/TESTING.md`.\n\n### How was this PR tested?\n\n```\nng test --watch=false --include src/app/common/formly/formly-utils.spec.ts\n```\n\n9 pass. ESLint and Prettier on the new spec are clean. The spec is pure\n— it builds plain `FormlyFieldConfig` objects (no `TestBed`) and covers:\n`getFieldByName` (match / no-match / first-of-duplicate-keys);\n`setHideExpression` (sets the hide expression on present fields, no-op\nfor absent names); and the `createShouldHideFieldFunc` predicate\n(missing model → `false`, null target → `hideOnNull`, anchored regex\nmatch, and `equals` via `toString()`).\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])\n\n---------\n\nSigned-off-by: Meng Wang <mengw15@uci.edu>\nCo-authored-by: Copilot Autofix powered by AI <175728472+Copilot@users.noreply.github.com>",
          "timestamp": "2026-07-12T07:27:05Z",
          "url": "https://github.com/apache/texera/commit/afa8af22b2ac0279fbcc0c610cfe54e3ee0ef0b8"
        },
        "date": 1783862038815,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 616.553348030171,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1060.9214859861086,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1123.781370445201,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 683.1668394089886,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1078.507724165791,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1128.2421013126677,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 815.4770159185297,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1079.0055709966693,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1130.8732442312796,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 683.0325544780166,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 868.2286827289545,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 917.7272282012459,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 700.9889409678419,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 885.8323420698533,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 899.3155711555212,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 707.264099615806,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 871.4110082498714,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 884.8401553633173,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 426.92712372505304,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 501.15873099380497,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 514.0825536614508,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 431.156297049877,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 493.33958897042487,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 505.24652761912245,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 406.4067782791857,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 479.17205701439195,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 486.12225383037566,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Meng Wang",
            "username": "mengw15",
            "email": "mengw15@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "0cdba7b5cd13ad9b19f0349ce402b3165e876c8c",
          "message": "test(frontend): add unit test coverage for UserProjectService (#6392)\n\n### What changes were proposed in this PR?\n\nAdds a Vitest spec for `UserProjectService`\n\n(`frontend/src/app/dashboard/service/user/project/user-project.service.ts`),\nwhich previously had no dedicated spec (codecov ~33%). The service is an\n`HttpClient` wrapper for the project dashboard, plus two static color\nhelpers.\n\n19 tests, using `HttpClientTestingModule` + `HttpTestingController` (no\nbackend):\n\n- **HTTP methods (14)** — each asserts request method / URL (built from\nthe\n  service's exported URL constants) / body, and the emitted response:\n`getProjectList`, `retrieveWorkflowsOfProject`,\n`retrieveFilesOfProject`,\n  `retrieveProject`, `createProject`, `updateProjectName`,\n  `updateProjectDescription` (raw-string body), `deleteProject`,\n`addWorkflowToProject`, `removeWorkflowFromProject`, `addFileToProject`,\n  `updateProjectColor`, `deleteProjectColor`, `removeFileFromProject`.\n- **Cached file list (3)** — `getProjectFiles` starts empty;\n`refreshFilesOfProject` fetches and caches;\n`deleteDashboardUserFileEntry`\n  deletes then refreshes the cache (two-request flow).\n- **Static helpers (2)** — `isInvalidColorFormat` and `isLightColor`\nacross\n  null / wrong-length / non-hex / 3- and 6-digit / white / black inputs.\n\n`NotificationService` (unused by this service, but a constructor\ndependency\nthat pulls in ng-zorro) is stubbed so `TestBed` stays hermetic. No\nproduction\ncode was changed.\n\n### Any related issues, documentation, discussions?\n\nCloses #6388\n\n### How was this PR tested?\n\nNew unit tests, run locally in `frontend/` (all green; the failure path\nwas\nverified by breaking a URL assertion to confirm the suite goes red):\n\n```\nng test --watch=false --include src/app/dashboard/service/user/project/user-project.service.spec.ts\n# Tests  19 passed (19)\neslint <spec>       # clean\nprettier --check    # clean\n```\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-13T07:36:40Z",
          "url": "https://github.com/apache/texera/commit/0cdba7b5cd13ad9b19f0349ce402b3165e876c8c"
        },
        "date": 1783950138157,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 628.6604244353706,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1102.9115544963865,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1190.933111558125,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 814.0229854424945,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1148.9066537912588,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1199.2857841176926,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 841.5324839053673,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1166.4600028288964,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1189.4570014266378,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 729.9903819204742,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 926.7265521955081,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 961.7693947852977,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 748.9138430451359,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 935.1508190246788,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 947.2127016569978,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 740.7920968387727,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 923.8707663385852,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 942.2594294727555,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 459.8369480673994,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 534.2583010841123,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 540.7032669145849,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 457.4430647406327,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 528.0233936455884,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 540.1515995739181,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 435.60489219081336,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 509.07423977147937,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 512.618757419611,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Prateek Ganigi",
            "username": "PG1204",
            "email": "91584519+PG1204@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "0485d022d9aabc818c8ed1589ec7854c66645af8",
          "message": "refactor(frontend): have both callers supply Validation to applyOperatorBorder (#6075)\n\n### What changes were proposed in this PR?\n\nFollow-up to #5702. `WorkflowEditorComponent.applyOperatorBorder`\ndecides an operator's border color and has two callers:\n\n- `handleOperatorValidation`: the validation-stream subscriber, which\nalready had the `Validation` and passed it in.\n- The operator-add stream subscriber: which passed no `Validation` and\nrelied on an optional-parameter fallback that recomputed it inside the\nhelper.\n\nThis PR unifies the two paths so both callers obtain the `Validation`\nthemselves and pass it in:\n\n- The operator-add subscriber now computes it via\n`validationWorkflowService.validateOperator(...)` and passes it,\nmirroring the validation-stream caller.\n- `applyOperatorBorder`'s `validation` parameter becomes **required**,\nand the `?? validateOperator(...)` fallback is removed. The color\ndecision no longer silently depends on a recompute hidden inside the\nhelper.\n\nThis is a non-functional cleanup and no behavioral change. In\nparticular, the operator-add subscriber still paints the border\nimmediately (and restores cached run statistics), preserving the\nnavigation-reset behavior from #3614.\n\nNote on scope: the issue's summary mentions \"painted exactly once.\"\nTruly collapsing to a single paint conflicts with the #3614 requirement\nthat borders render immediately on reload without waiting for validation\nevents, and would need fragile \"just-added\" state tracking in the\nvalidation handler. This PR implements the safe, behavior-preserving\nunification (consistent validation acquisition, fallback removed) rather\nthan suppressing either paint.\n\n### Any related issues, documentation, discussions?\n\nPart of #5726. \nRelated: #5702 (added the optional parameter; this issue arose from its\nreview), #5146 (introduced `applyOperatorBorder` and the #3614\nnavigation fix).\n\n### How was this PR tested?\n\nUpdated `workflow-editor.component.spec.ts`:\n- Renamed the \"uses the Validation passed in instead of recomputing it\"\ntest to \"relies solely on the passed-in Validation (never recomputes\ninside the helper)\": the old name implied a recompute path that no\nlonger exists.\n- Added \"supplies a computed Validation from the operator-add path\",\nasserting the operator-add subscriber calls `applyOperatorBorder` with a\n`Validation` object.\n- Existing border-outcome tests (green / gray / red /\ninvalid-over-cached priority) remain and continue to pass.\n\nVerified locally:\n- `tsc --noEmit`: clean\n- `prettier --check` / `eslint`: clean\n- `ng test` (jsdom): 30/30 pass in the editor spec\n- `ng run gui:test-browser`: 13/13 pass\n\n### Was this PR authored or co-authored using generative AI tooling?\nCo-authored with Claude Opus 4.8 in compliance with ASF",
          "timestamp": "2026-07-14T04:56:00Z",
          "url": "https://github.com/apache/texera/commit/0485d022d9aabc818c8ed1589ec7854c66645af8"
        },
        "date": 1784034904617,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 729.2878086265739,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1314.7328644853992,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1450.0159785953526,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 964.7129171076301,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1328.772321807221,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1455.6228200346238,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 965.2618095388043,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1370.2957653025765,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1448.8050405180286,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 781.692171505176,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 1072.0896249927441,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 1123.3728678330199,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 803.0077545178057,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 1094.4374345614276,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1121.1954166965822,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 816.2629907020372,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 1054.7155911508553,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 1095.6342170209025,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 458.4500062524902,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 554.1686134141322,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 582.127320305553,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 476.98147103110455,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 572.4705744681244,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 578.2717191867455,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 476.6588001575522,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 548.2509558449354,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 542.1905505592515,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "aa0bc44a86f9b8ac5d6ccab6dd767c669057d86f",
          "message": "chore(deps, pyright-language-service): bump @types/node from 26.1.0 to 26.1.1 in /pyright-language-service in the pyright-patch group (#6404)\n\nBumps the pyright-patch group in /pyright-language-service with 1\nupdate:\n[@types/node](https://github.com/DefinitelyTyped/DefinitelyTyped/tree/HEAD/types/node).\n\nUpdates `@types/node` from 26.1.0 to 26.1.1\n<details>\n<summary>Commits</summary>\n<ul>\n<li>See full diff in <a\nhref=\"https://github.com/DefinitelyTyped/DefinitelyTyped/commits/HEAD/types/node\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility\nscore](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=@types/node&package-manager=npm_and_yarn&previous-version=26.1.0&new-version=26.1.1)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't\nalter it yourself. You can also trigger a rebase manually by commenting\n`@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits\nthat have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all\nof the ignore conditions of the specified dependency\n- `@dependabot ignore <dependency name> major version` will close this\ngroup update PR and stop Dependabot creating any more for the specific\ndependency's major version (unless you unignore this specific\ndependency's major version or upgrade to it yourself)\n- `@dependabot ignore <dependency name> minor version` will close this\ngroup update PR and stop Dependabot creating any more for the specific\ndependency's minor version (unless you unignore this specific\ndependency's minor version or upgrade to it yourself)\n- `@dependabot ignore <dependency name>` will close this group update PR\nand stop Dependabot creating any more for the specific dependency\n(unless you unignore this specific dependency or upgrade to it yourself)\n- `@dependabot unignore <dependency name>` will remove all of the ignore\nconditions of the specified dependency\n- `@dependabot unignore <dependency name> <ignore condition>` will\nremove the ignore condition of the specified dependency and ignore\nconditions\n\n\n</details>\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>\nCo-authored-by: Xinyuan Lin <xinyual3@uci.edu>",
          "timestamp": "2026-07-15T08:27:39Z",
          "url": "https://github.com/apache/texera/commit/aa0bc44a86f9b8ac5d6ccab6dd767c669057d86f"
        },
        "date": 1784121406472,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 700.469070443914,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1303.7314150151765,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1385.7346134677846,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 963.3195017462808,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1358.3033748052017,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1410.9659646796279,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 988.9268877342774,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1343.2658620972013,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1397.7669662757223,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 818.0058261246137,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 1074.9676799773913,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 1116.8399228628232,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 825.4596878207627,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 1072.2504417227506,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1107.9372327239498,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 818.3918911963693,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 1057.520822535821,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 1084.6931781867593,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 479.04849789242814,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 575.8827912241766,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 588.6900940413223,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 493.76123934513487,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 572.9572774308681,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 589.3521213780842,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 456.4247910915433,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 541.7259420698283,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 553.3676957439598,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "c7473a1d8c8bf68f5cfab4ec4968982173ac5ae2",
          "message": "fix(deps): bump sttp client core to 4.0.26 (#6408)\n\nUpdates `com.softwaremill.sttp.client4:core_2.13` from `4.0.25` to\n`4.0.26`.\n\nNotes:\n- The `org.postgresql:postgresql` update was removed from this PR.\n- `project/plugins.sbt` keeps pgjdbc pinned at `42.7.4` for jOOQ 3.19.x\ncodegen compatibility, as documented in that file.\n- `computing-unit-managing-service/LICENSE-binary` was updated to match\nthe bundled sttp `4.0.26` jar.\n\nValidation:\n- `bin/licensing/check_binary_deps.py --ignore-transitive-version jar\n--license-binary computing-unit-managing-service/LICENSE-binary ...`\npasses locally for the service jar set.\n- GitHub Actions `build / platform (computing-unit-managing-service)`\npassed on commit `986cf34a99`.\n- Required Checks passed on commit `986cf34a99`.\n\n---------\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>\nCo-authored-by: Xinyuan Lin <xinyual3@uci.edu>",
          "timestamp": "2026-07-16T00:45:12Z",
          "url": "https://github.com/apache/texera/commit/c7473a1d8c8bf68f5cfab4ec4968982173ac5ae2"
        },
        "date": 1784208081325,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 628.7087292170742,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1271.842304560094,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1423.6744493294925,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 902.3377681174298,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1352.4039458923044,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1428.2122705229415,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 952.3400652474844,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1345.2766827725013,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1411.764569512539,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 780.3241088671365,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 1051.896097569621,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 1099.8080572168517,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 821.2895727734675,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 1064.5601261604456,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1105.4585049525342,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 827.730737474386,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 1057.0029996610965,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 1103.2232482783986,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 480.34782647073354,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 573.9446407018426,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 577.1609433104126,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 493.32907560412457,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 576.2616173528089,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 575.5031478612968,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 475.0161465885369,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 544.6557591855519,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 561.0234301916707,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "ali risheh",
            "username": "aicam",
            "email": "ali.risheh876@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "ee062a20d3777d1409508766dba18187e3719035",
          "message": "refactor(k8s): consolidate gateway resources under the gateway folder (#6472)\n\n### What changes were proposed in this PR?\n\nA small, behavior-preserving refactor of the Helm chart layout under\n`bin/k8s`.\n\nThe agent-service **`BackendTrafficPolicy`** is a gateway-layer [Envoy\nGateway](https://gateway.envoyproxy.io/) resource — it configures\nconsistent-hash load balancing on the gateway for the agent-service\n`HTTPRoute` (so all requests for a given workflow pin to the same\nreplica). Despite being a gateway concern, it was living under\n`templates/base/agent-service/` next to the app-tier\n`Deployment`/`Service`/`Secret`.\n\nIt is the same class of resource as the `SecurityPolicy`\n(`gateway-security-policy.yaml`) that already lives in\n`templates/base/gateway/`, so this PR moves it there and renames it to\nthe folder's `gateway-*` convention:\n\n```\ntemplates/base/agent-service/agent-service-backend-traffic-policy.yaml\n  -> templates/base/gateway/gateway-agent-traffic-policy.yaml\n```\n\nAfter this move, **every** gateway-related resource (`GatewayClass`,\n`Gateway`, `HTTPRoute`, `Backend`, `SecurityPolicy`,\n`BackendTrafficPolicy`) lives under `templates/base/gateway/`, and no\ngateway resource remains scattered in a service folder (verified by\ngrepping all templates for the `gateway.envoyproxy.io` /\n`gateway.networking.k8s.io` API groups — this was the only stray one).\n\nThis is purely organizational: no values, resource names, gating\nconditions, or rendered output change. It continues the\n`base`/`aws`/`on-prem` template reorganization from #5757.\n\n### Any related issues, documentation, discussions?\n\nPart of the deployment-unification effort tracked in #5891 (housekeeping\nfollow-up to the template reorg in #5757). No functional dependency on\nthe object-storage work (#5932, #6295).\n\n### How was this PR tested?\n\nVerified the move is byte-for-byte behavior-preserving by diffing the\nrendered manifests before and after:\n\n```bash\ncd bin/k8s\nhelm dependency build   # fetch subcharts\n\n# render with the file in its OLD location\nhelm template test . > /tmp/before.txt\n\n# render with the file in its NEW location (this PR)\nhelm template test . > /tmp/after.txt\n\ndiff /tmp/before.txt /tmp/after.txt\n```\n\nThe only differences are:\n1. The `# Source:` provenance comment path (`.../agent-service/...` →\n`.../gateway/...`) — expected, cosmetic.\n2. The auto-generated `encryptionKey` value — Helm regenerates this\nrandom secret on every invocation; unrelated to this change.\n\nThe `BackendTrafficPolicy` resource itself (name\n`<release>-agent-service-traffic-policy`, its `targetRefs`, and the\n`X-Agent-Workflow-Id` consistent-hash config) renders identically. `helm\ntemplate` succeeds for the default value set.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Claude Opus 4.8)\n\nCo-authored-by: Claude Opus 4.8 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-07-17T07:19:16Z",
          "url": "https://github.com/apache/texera/commit/ee062a20d3777d1409508766dba18187e3719035"
        },
        "date": 1784294034969,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 674.0414585160362,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1326.6693733936845,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1454.1387410028462,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 912.9746872258164,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1377.928282475175,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1435.6032287004107,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 927.9372513983608,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1387.4143369346925,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1450.666396897494,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 724.7608461247894,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 1059.6568677327264,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 1098.1932681871137,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 782.8374077273951,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 1061.3045532208878,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1104.2269186560768,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 809.7711554728673,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 1045.1387107096134,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 1087.538775376799,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 494.782426662831,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 541.983405319856,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 579.869803902717,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 490.32541305577894,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 576.9493558511704,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 583.4427351099104,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 478.17517733016473,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 556.0646996857905,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 553.8032469280978,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Matthew B.",
            "username": "Ma77Ball",
            "email": "mgball@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "39db110e74b4bdcf0e4b0df961c25973e6bbb20c",
          "message": "test(frontend): add unit tests for AuthService (#6470)\n\n### What changes were proposed in this PR?\n- Adds auth.service.spec.ts covering AuthService; no production code\nchanged.\n- Verifies the static access-token helpers and the login, register and\nGoogle HTTP endpoints.\n- Covers logout and the loginWithExistingToken branches (no token,\nexpired, valid, invite-only inactive).\n### Any related issues, documentation, discussions?\nCloses: #6458\n### How was this PR tested?\n- Run `cd frontend && npx nx test gui\n--include=\"**/auth.service.spec.ts\"`, expect 10 passed.\n- Full frontend suite runs in CI via `yarn test:ci`, selected by the\nauto-applied `frontend` label.\n### Was this PR authored or co-authored using generative AI tooling?\nCo-authored with Claude Opus 4.8 in compliance with ASF",
          "timestamp": "2026-07-18T04:00:53Z",
          "url": "https://github.com/apache/texera/commit/39db110e74b4bdcf0e4b0df961c25973e6bbb20c"
        },
        "date": 1784380400267,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 600.562456227874,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1064.7187524656008,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1155.9568888883778,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 775.6046226902638,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1103.2170258697229,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1158.8824100429347,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 817.5395771502015,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1104.7661735868169,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1153.7503353911122,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 686.8537210892752,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 883.70828351825,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 924.9656082092514,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 696.4520710397326,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 890.105396329655,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 917.9233684628931,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 703.1284163251928,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 883.4475652804862,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 899.5740027175636,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 420.1591483780104,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 511.62064440866436,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 513.5705770807074,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 421.0842029654475,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 496.1486745908817,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 505.9066441010097,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 406.62036954095896,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 478.51493801610394,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 487.8403398758089,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "80c45be89dbbc64abafe0f70a1c05ef7117fbaa8",
          "message": "fix(amber): run Iceberg local storage on Windows in the Python worker (#6545)\n\n### What changes were proposed in this PR?\n\nFollow-up to #6488, which made Iceberg local storage work on Windows\nwithout\nwinutils **on the Scala/JVM side only** (`IcebergUtil` +\n`WinutilsFreeLocalFileSystem`).\nThe Python UDF worker writes its output-port Iceberg storage through its\n**own**\npyiceberg path — `core/storage/iceberg/iceberg_document.py` and\n`document_factory.py`\ncall `IcebergCatalogInstance.get_instance()` → `create_postgres_catalog`\ninside the\nPython process — so #6488 does not reach it. On a Windows dev machine\nwith a\nlocal-filesystem warehouse (`postgres` catalog type), that path fails\ntwo ways:\n\n| # | Symptom | Root cause |\n|---|---|---|\n| 1 | `ValueError: Unrecognized filesystem type in URI: c` at table\ncreation | a bare drive path `C:\\...` is parsed by pyiceberg as URI\nscheme `c` |\n| 2 | `OSError: [WinError 123] ... The filename, directory name, or\nvolume label syntax is incorrect` | even a `file:///C:/...` URI is\nrejected by pyiceberg's default **pyarrow** FileIO (`/C:/...`) |\n\nFix (in `iceberg_utils.py`, mirroring #6488's tight, self-gating\napproach):\n\n| Change | Detail |\n|---|---|\n| `_is_windows_local_warehouse(warehouse)` (new) | True only when the\nwarehouse carries a **Windows drive letter** — bare (`C:\\...`, `C:/...`)\nor `file:///C:/...`. Excludes POSIX paths, colon-stripped paths\n(`C/...`), and remote object stores (`s3://...`). |\n| `_to_file_uri(warehouse)` (new) | Normalizes a bare drive path to a\n`file:///` URI via `PureWindowsPath.as_posix()` — not `.as_uri()`, which\nwould percent-encode a space to `%20` (deterministic on every host OS);\nalready-URI values pass through. |\n| `create_postgres_catalog` | When (and only when) the warehouse is\nWindows-local, register the normalized `file:///` URI and select\n`pyiceberg.io.fsspec.FsspecFileIO`, whose `LocalFileSystem` handles\nWindows drive paths. |\n| `create_rest_catalog` | Unchanged — its `warehouse_name` is a logical\nidentifier resolved server-side via `S3FileIO`, not a local path\n(checked, exempt). |\n\nThe gate is deliberately narrow so the fix cannot alter the\ncross-runtime\nwarehouse convention that Linux, macOS, CI, and the Scala engine rely\non: PyIceberg\npersists the `warehouse` value into table metadata, so a `postgres`\ncatalog with a\nPOSIX or remote warehouse must keep the plain-path / default-FileIO\nbehavior\n(regression fixed in #4409). Only genuine Windows drive-letter\nwarehouses are\nnormalized.\n\nThe drive path is normalized with `PureWindowsPath.as_posix()` (not\n`.as_uri()`)\nso a warehouse containing a space — e.g. `C:\\Users\\John Doe\\...`,\n`C:\\Program\nFiles\\...`, OneDrive-redirected profiles — keeps a **raw** space rather\nthan\n`%20`; fsspec's `LocalFileSystem` does not URL-decode, so a `%20` would\nwrite\ninto a literally `%20`-named directory.\n\nScope note: on Windows the two runtimes register different warehouse\nstrings into\nthe shared `postgres` catalog — the Scala side its existing\ncolon-stripped path\n(`C/Users/...`), the Python worker the absolute `file:///C:/...` URI.\nThis is fine\nfor the actual data flow here (a Python UDF's output-port table is\nwritten by the\nPython worker and read back by the engine/result service via the\nabsolute metadata\npointer, verified end-to-end); reconciling the Scala side's\ncolon-stripped\nconvention is out of scope for this fix and would belong with #6488's\nfollow-ups.\n\nBefore → after:\n\n| Environment (`postgres` catalog) | Before | After |\n|---|---|---|\n| Windows, local warehouse (`C:\\...`) | worker crashes at first table\ncreate | works |\n| Linux / macOS / CI, local warehouse (`/tmp/...`) | works | unchanged\n(gate off) |\n| Any host, remote warehouse (`s3://...`) | works | unchanged (gate off)\n|\n| `rest` catalog | unaffected (`S3FileIO`) | unaffected |\n\n### Any related issues, documentation, discussions?\n\nFollow-up to #6488 (Scala/JVM side); closes the Python-worker half of\n#6487.\nPreserves the cross-runtime warehouse invariant from #4409.\n\n### How was this PR tested?\n\n- New `TestCreatePostgresCatalogWindowsLocal` in\n`test_iceberg_utils_catalog.py`\n(written first, TDD): a Windows drive-letter warehouse is normalized to\na\n`file:///` URI and `FsspecFileIO` is selected; POSIX, colon-stripped,\nand\nremote (`s3://`) warehouses are left untouched (no FileIO override). The\ntests\nassert on the computed `SqlCatalog` props and use `PureWindowsPath`, so\nthey are\n  OS-independent and pass on Linux CI.\n- The pre-existing `TestCreatePostgresCatalog` tests (the #4409\nplain-path\n  invariant) still pass unchanged.\n- Verified end-to-end on a real Windows filesystem: a bare `C:\\...`\nwarehouse\nfed through the productionized helpers into a real catalog creates a\ntable and\nround-trips rows (both failure modes above reproduced on stock `main`\nfirst).\n- `amber/` `ruff check` and `ruff format --check` pass on the changed\nfiles.\n(The `test_iceberg_document.py` / REST integration tests require a live\npostgres / REST catalog server and are environment-gated locally; they\nare\nunaffected by this change — confirmed identical pass/fail with the diff\nstashed.)\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-19T06:02:30Z",
          "url": "https://github.com/apache/texera/commit/80c45be89dbbc64abafe0f70a1c05ef7117fbaa8"
        },
        "date": 1784466374495,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 641.0323950826678,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1330.7297494005013,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1410.7476660700545,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 942.0592887556938,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1346.0708234693989,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1418.3581213681846,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 902.63933913611,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1340.097663187225,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1395.1794508407363,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 762.0160470185604,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 1052.9471370073677,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 1118.0432012069487,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 784.5324983767346,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 1031.000425506299,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1073.6836160456676,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 780.8181986909177,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 1034.8309668052061,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 1088.0195773518133,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 454.67137516279945,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 571.9502972831795,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 571.8528558708039,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 456.949757590941,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 557.4881711497828,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 576.0766694212174,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 460.86832772181054,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 540.9749447663148,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 554.5056675831621,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "c543d7796a027382828648826266a0e45232934c",
          "message": "test(frontend): extend WorkflowGraph unit test coverage (#6586)\n\n### What changes were proposed in this PR?\n\nExtends the existing `workflow-graph.spec.ts` with 44 new tests (24 ->\n68) for the `TexeraGraph` in-memory model, targeting\npreviously-uncovered behavior:\n- operator/link/port add-remove-get and their guard/throw branches;\n- disabled / view-result / reuse-cache state toggles and their getters\n(incl. sink-ignore and idempotent no-ops);\n- comment-box create/edit/delete and the `assert*` existence helpers;\n- operator debug-state lifecycle;\n- `getSubDAG` (full DAG from terminals, targeted root,\ndisabled-operator/link exclusion, visited dedup on a diamond);\n- `setPortProperty`; and every event-stream getter.\n\nReuses the existing direct-construction setup; adds an `afterEach` that\ntears down the shared model. Statement coverage for the file went from\n262 uncovered lines to 18 (the remainder are unreachable defensive\nbranches). No existing tests modified.\n\n### Any related issues, documentation, discussions?\n\nCloses #6583.\n\n### How was this PR tested?\n\n`npx ng test --watch=false --include='**/workflow-graph.spec.ts'` ->\n68/68 passing. `yarn format:ci` passes.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-20T07:38:03Z",
          "url": "https://github.com/apache/texera/commit/c543d7796a027382828648826266a0e45232934c"
        },
        "date": 1784554752149,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 659.8501167088119,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1049.3285690273067,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1128.3483879073883,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 775.0370529132979,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1082.052535916004,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1145.55044753552,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 843.0171425550633,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1077.6904631713198,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1122.357896729696,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 682.2267199549955,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 879.6932524442761,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 877.3558651194166,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 675.3251002708021,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 861.3066864533045,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 888.118189338234,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 695.3104076860446,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 849.4078687730783,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 894.6952936189314,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 428.59555237823383,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 507.2354130872188,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 507.226893917852,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 436.5441116365526,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 501.71937406998154,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 509.651155182702,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 394.2559143195099,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 472.15108115775354,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 480.9718886225128,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Ryan Zhang",
            "username": "zyratlo",
            "email": "97552093+zyratlo@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "0467c76529981b3951bceb33eedf6bcc776f16a3",
          "message": "feat(python-notebook-migration): add notebook migration orchestration service (#5262)\n\n### What changes were proposed in this PR?\nIntroduces `NotebookMigrationService`, the frontend orchestration\nservice that sits between the migration-tool UI and the lower layers:\nthe LLM client (`migration-tool-llm-client`) and the backend\nnotebook-migration microservice\n(`migration-tool-backend-notebook-migration-service`).\n\n**`notebook-migration.service.ts`**\n- `getAvailableModels()` — `GET /api/models` against the existing\nLiteLLM proxy, returns the model dropdown options.\n- `sendToAIGenerateWorkflow(notebook, modelType)` — drives the full\n`NotebookMigrationLLM` lifecycle (initialize → verify connection →\nconvert → close in `finally`) and returns `{ workflowContent,\nmappingContent }`.\n- `sendNotebookToJupyter(notebookData)` — `POST\n/api/notebook-migration/set-notebook`; surfaces a `NotificationService`\ntoast on success and failure; returns `1` / `0`.\n- `getJupyterURL()`, `getJupyterIframeURL()` — calls the matching\nmicroservice endpoints to retrieve URLs to embed.\n- `storeNotebookAndMapping(wid, vid, mappingContent, notebookContent)` —\n`POST /api/notebook-migration/store-notebook-and-mapping`; returns the\n`HttpClient` observable directly so callers can compose with\n`switchMap`.\n- Mapping cache — small in-memory dictionary `{ [key: string]:\nMappingContent }` keyed by `mapping_wid_<workflowId>`, with\n`hasMapping`, `getMapping`, `setMapping`, `deleteMapping`.\n\n  **`notebook-migration.service.spec.ts`**\n- `getAvailableModels`: maps the LiteLLM `data[].id` array correctly;\nfalls back to an empty array on HTTP error.\n- `sendNotebookToJupyter`: success → returns `1`; error → returns `0`\nand toasts.\n- `getJupyterURL` / `getJupyterIframeURL`: success → returns the URL;\nnon-OK response or thrown error → returns `null`.\n- Mapping cache: `setMapping` then `getMapping` round-trips;\n`deleteMapping` removes the entry.\n- `storeNotebookAndMapping`: makes the expected `POST` to the\npersistence endpoint.\n\n\n### Any related issues, documentation, discussions?\nCloses #5261 \nParent issue #4301 \n\n\n### How was this PR tested?\nThe new `notebook-migration.service.spec.ts` adds\n`HttpClientTestingModule`-driven test cases\n\n\n\n### Was this PR authored or co-authored using generative AI tooling?\nGenerated-by: Claude Code (Claude Opus 4.7)",
          "timestamp": "2026-07-20T23:11:48Z",
          "url": "https://github.com/apache/texera/commit/0467c76529981b3951bceb33eedf6bcc776f16a3"
        },
        "date": 1784640032377,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 693.233627426296,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1326.7313979605633,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1435.3178019848385,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 858.9364059332423,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1331.4569899971118,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1435.3317608994626,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 940.1238760430085,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1365.7218544925156,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1424.2588571900608,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 796.4017676169088,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 1076.3358249011214,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 1118.7611311145383,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 802.4106344453904,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 1085.5295579734561,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1112.1095871261784,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 786.9007580117819,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 1041.5105920190704,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 1077.1185709012498,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 473.20315246302886,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 577.3737562513514,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 575.1929478784659,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 484.4146026157064,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 575.7502687373178,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 576.8553584459413,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 449.7626265410214,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 549.3102832055514,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 535.556608399068,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "a9f384eab44c8e1595792e02b32c2c1bfc16e77e",
          "message": "test(frontend): extend computing-unit util unit test coverage (#6738)\n\n### What changes were proposed in this PR?\n\nExtends `computing-unit.util.spec.ts` with 15 tests (34 -> 49) covering\nthe ComputingUnitMetadataComponent render branches (owner vs\naccess-privilege, present/empty gpuLimit), the cpu/memory\nresource-conversion unknown-unit and unitless fallbacks, the\n`memoryPercentage` non-positive-limit guard, and the JVM memory-slider /\n`memoryToGb` Mi/floor/fallback paths. Coverage -> ~100%. No existing\ntests modified.\n\n### Any related issues, documentation, discussions?\n\nCloses #6731.\n\n### How was this PR tested?\n\n`ng test --include='**/computing-unit.util.spec.ts'` -> 49/49 passing.\n`yarn format:ci` passes.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-22T05:39:27Z",
          "url": "https://github.com/apache/texera/commit/a9f384eab44c8e1595792e02b32c2c1bfc16e77e"
        },
        "date": 1784729836841,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 627.729861814781,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1114.0858136310333,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1191.9476111916179,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 853.2489061970828,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1167.5110970197477,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1195.2053796274167,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 833.0570811919965,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1140.4150165643987,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1202.1599033921307,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 716.2950085074376,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 924.9689497103899,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 953.7057740370741,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 758.5816025871477,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 926.987565989107,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 959.9569035242848,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 757.38182307467,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 936.0975412233809,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 948.5570953894111,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 457.7809598539214,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 531.1999772235049,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 539.5995185095705,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 460.51545635025985,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 528.3310547258511,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 539.2746966114719,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 448.7894555070211,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 512.0666565605703,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 516.1795060551675,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "dda9e830f54e0fa74dcf0fd5d41a3c2e1a0c989c",
          "message": "ci: discover staged Codecov flags instead of a hardcoded matrix (#6824)\n\n### What changes were proposed in this PR?\n\nFollow-up to #6730. The deferred `Codecov Upload` workflow hardcoded all\n11 flags in its matrix and tried to download each from the source run.\nOn a label-gated PR — e.g. a frontend-only PR that only stages\n`codecov-frontend` — the other 10 legs hit `##[error] Artifact not found\nfor name: codecov-<flag>`, which looks like a name mismatch; and because\nthe download used `continue-on-error: true`, a genuinely failed download\nwas swallowed, the leg went green, and `notify-failure` never fired.\n\n**Fix:** add a `discover` job that lists the source run's *actual*\n`codecov-*` artifacts and drives the upload matrix from that\n(`fromJSON`), and drop `continue-on-error` on the download.\n\n- **No more spurious errors** — only the flags the source run actually\nbuilt are processed; a frontend-only PR runs one clean leg.\n- **Failures surface** — a listed artifact that then fails to download\nor upload fails the leg, so `notify-failure` fires (previously it went\nsilently green).\n- Also removes the hardcoded flag list, so adding a `platform` service\nno longer needs a matching matrix row here.\n\nObserved on the first live run after #6730 merged ([run\n29984668372](https://github.com/apache/texera/actions/runs/29984668372),\ntriggered by a frontend-only PR's checks): `frontend` uploaded fine, but\n10 legs logged `Artifact not found` for the label-gated flags and the\nrun still went green.\n\n### Any related issues, documentation, discussions?\n\nFollow-up to #6730 (reported on #6685).\n\n### How was this PR tested?\n\n- `workflow_run` only activates from the copy of the file on the default\nbranch, so this can't run on its own PR. Validated by YAML lint and an\nadversarial review of the discover → upload → notify flow: empty-flags\ncleanly skips (no failure, no notify), the dynamic `fromJSON` matrix\nhandles one or many flags, a download/upload failure now fails the leg\nand fires `notify-failure`, `amber-integration` (results-only) skips the\ncoverage step via `hashFiles`, and the `workflow_dispatch` path resolves\nthe run id.\n- The `workflow_dispatch` (`run_id`) entry remains for manual post-merge\nvalidation against a finished *Required Checks* run.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nYes.\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-23T11:58:13Z",
          "url": "https://github.com/apache/texera/commit/dda9e830f54e0fa74dcf0fd5d41a3c2e1a0c989c"
        },
        "date": 1784812818154,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 740.3941279719312,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1327.34922200536,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1426.2210084645144,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 931.3537187138081,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1337.2460181876393,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1434.6042761826702,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 984.1163478756008,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1378.1282900726353,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1401.5637406755343,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 787.9436204529286,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 1082.2138450178036,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 1123.9441530058398,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 819.8364455231408,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 1066.7288959893574,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 1111.564596477707,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 806.7706178129666,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 1052.7448356747257,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 1101.7568793258167,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 497.9128341688243,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 580.1329550400349,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 586.900172957913,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 479.1918267683038,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 572.8631372938198,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 575.9417584892051,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 463.8210077773729,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 546.001519084662,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 554.5441300324028,
            "unit": "tuples/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Matthew B.",
            "username": "Ma77Ball",
            "email": "mgball@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "a351f44fbf08920d7d9dfe806614ffc4c8ed5a83",
          "message": "test(frontend): cover WorkflowUtilService untested helpers (#6689)\n\n### What changes were proposed in this PR?\n- Add unit tests for updateOperatorVersion covering port rebuild, port\npreservation, and the unknown-type error.\n- Add unit tests for the static parseWorkflowInfo string parsing and\nobject passthrough, and for getNewCommentBox default position and unique\nids.\n### Any related issues, documentation, discussions?\nCloses: #6688\n### How was this PR tested?\n- Run: `cd frontend && node --max-old-space-size=8192\n./node_modules/nx/dist/bin/nx.js test gui --watch=false\n--include=src/app/workspace/service/workflow-graph/util/workflow-util.service.spec.ts`,\nexpect the suite passing.\n- Test-only change; no production code is modified.\n### Was this PR authored or co-authored using generative AI tooling?\nCo-authored with Claude Opus 4.8 in compliance with ASF\n\n---------\n\nCo-authored-by: Claude Opus 4.8 <noreply@anthropic.com>\nCo-authored-by: Xinyuan Lin <xinyual3@uci.edu>",
          "timestamp": "2026-07-24T07:20:06Z",
          "url": "https://github.com/apache/texera/commit/a351f44fbf08920d7d9dfe806614ffc4c8ed5a83"
        },
        "date": 1784903053177,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "throughput / bs=10 sw=1 sl=8",
            "value": 679.8816884027381,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=8",
            "value": 1126.2972229138907,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=8",
            "value": 1207.2792788217748,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=64",
            "value": 868.9623747438258,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=64",
            "value": 1158.2195196331554,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=64",
            "value": 1197.6800990807576,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=1 sl=512",
            "value": 872.7231242775707,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=1 sl=512",
            "value": 1154.8555308266104,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=1 sl=512",
            "value": 1192.9271523447992,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=8",
            "value": 730.5716271745075,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=8",
            "value": 941.7132613726275,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=8",
            "value": 955.1376725553397,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=64",
            "value": 744.6842858804876,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=64",
            "value": 923.9800062803059,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=64",
            "value": 962.009973655997,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=10 sl=512",
            "value": 735.7946238155565,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=10 sl=512",
            "value": 914.6918802986005,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=10 sl=512",
            "value": 936.5042911455099,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=8",
            "value": 465.3815459015617,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=8",
            "value": 532.0038390275895,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=8",
            "value": 538.5085286047289,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=64",
            "value": 463.9277835871549,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=64",
            "value": 529.8748686787103,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=64",
            "value": 535.4005495003669,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=10 sw=50 sl=512",
            "value": 447.0073964170485,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=100 sw=50 sl=512",
            "value": 508.2342488297562,
            "unit": "tuples/sec"
          },
          {
            "name": "throughput / bs=1000 sw=50 sl=512",
            "value": 513.6178846277331,
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
      },
      {
        "commit": {
          "author": {
            "email": "mgball@uci.edu",
            "name": "Matthew B.",
            "username": "Ma77Ball"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "6723f074bc50f8e43f29e1e46bb7c665a0e032be",
          "message": "ci: warn when a PR or issue does not follow the template (#5622)\n\n### What changes were proposed in this PR?\n- Adds a non-blocking GitHub Actions workflow\n(`.github/workflows/template-compliance-warning.yml`) that comments when\na PR or issue is opened/edited without following the template, and\ndeletes the comment automatically once the description is fixed.\n- For PRs it strips the template's `<!-- -->` guidance and flags any\nrequired section that is missing or blank; for issues (GitHub form\ntemplates that already enforce required fields) it only flags a fully\nblank body.\n- Keeps the warning wording in `.github/template-compliance-warning.txt`\nso editing the message does not touch workflow logic.\n- Kept cheap on CI: a single `github-script` job with no build and only\na sparse-checkout of the message file, triggered on `opened`/`edited`\n(never `synchronize`), skipping drafts and bots, and posting one\nself-resolving sticky comment instead of duplicates.\n### Any related issues, documentation, discussions?\nCloses: #5621\n### How was this PR tested?\n- Validated the workflow YAML parses: `python3 -c \"import yaml;\nyaml.safe_load(open('.github/workflows/template-compliance-warning.yml'))\"`.\n- Exercised the detection logic in Node against the real\n`.github/PULL_REQUEST_TEMPLATE`: an unfilled template flags all three\nrequired sections empty, a properly filled body returns no problems, an\nempty body and a template with headings deleted are both flagged, and an\nissue with content passes.\n- The workflow itself runs only on real `pull_request_target`/`issues`\nevents, so end-to-end behavior (comment posted then auto-removed) is\nverifiable once merged; it cannot run from the PR branch beforehand.\n\ntested here: https://github.com/Ma77Ball/texera/issues/60\n<img width=\"1404\" height=\"980\" alt=\"image\"\nsrc=\"https://github.com/user-attachments/assets/1301fc83-8b28-481c-ae96-e137359d28af\"\n/>\n\n\n### Was this PR authored or co-authored using generative AI tooling?\nCo-authored with Claude Opus 4.8 in compliance with ASF",
          "timestamp": "2026-06-12T08:40:15Z",
          "tree_id": "976136e6a35d92bd7fe780b216d1b68a626105ab",
          "url": "https://github.com/apache/texera/commit/6723f074bc50f8e43f29e1e46bb7c665a0e032be"
        },
        "date": 1781254622766,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 25024.055,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 39905.506,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 39905.506,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 123515.93,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 153160.598,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 153160.598,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1069479.197,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1117828.04,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1117828.04,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "lie18@uci.edu",
            "name": "lie18uci",
            "username": "lie18uci"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "ebaea080b5d64c5b19a2a91c18cbcd1ed33c8e50",
          "message": "fix(storage): close Files.walk stream in deleteRepo (#5633)\n\n### What changes were proposed in this PR?\n\nThis PR updates GitVersionControlLocalFileStorage.deleteRepo to close\nthe stream returned by Files.walk(directoryPath) using\ntry-with-resources.\n\nFiles.walk(...) returns a closeable stream backed by directory\nresources. Wrapping it in try-with-resources ensures the stream is\nclosed properly even if traversal or deletion throws.\n\nThis keeps the existing deletion behavior unchanged while fixing the\nstream lifecycle.\n\n### Any related issues, documentation, discussions?\n\nCloses #5548\n\n### How was this PR tested?\n\nAdded GitVersionControlLocalFileStorageSpec, which creates a temporary\nnested repository directory, calls deleteRepo, and verifies that the\nrepository directory is deleted recursively.\n\nRan formatting locally:\nsbt scalafmtAll\nsbt scalafmtCheckAll\nscalafmtCheckAll passed successfully.\n\nAttempted to run the targeted test locally:\n\nsbt \"WorkflowCore / testOnly\norg.apache.texera.amber.core.storage.util.dataset.GitVersionControlLocalFileStorageSpec\"\n\nbut my local backend setup could not generate jOOQ classes because\nPostgreSQL was not running on localhost:5432. The failure occurred\nbefore the test ran, due to missing generated\norg.apache.texera.dao.jooq.generated classes. I am relying on GitHub CI\nto run the backend test in the configured environment.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: ChatGPT",
          "timestamp": "2026-06-12T08:47:26Z",
          "tree_id": "6621a6bda9a9421f7af344395ad04700a3325c15",
          "url": "https://github.com/apache/texera/commit/ebaea080b5d64c5b19a2a91c18cbcd1ed33c8e50"
        },
        "date": 1781255022310,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 27093.272,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 39852.872,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 39852.872,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 110119.235,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 123367.286,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 123367.286,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 917080.427,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 965383.923,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 965383.923,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "142070420+EmilySun621@users.noreply.github.com",
            "name": "EmilySun621",
            "username": "EmilySun621"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "b7b50798cbdab928d3928be36bd200984879d14c",
          "message": "test(frontend): add spec for VisualizationFrameContentComponent (#5585)\n\n### What changes were proposed in this PR?\n\nAdds a behavior-focused unit test spec for\n`VisualizationFrameContentComponent`. Tests cover:\n- `drawChart()` guard clauses (no-op when data is missing)\n- Render path through DomSanitizer to iframe `srcdoc`\n- `auditTime`-throttled subscription (tested with `fakeAsync`/`tick`)\n\n### Any related issues, documentation, discussions?\n\nRelated to #5474 \n\n### How was this PR tested?\n\nSpec verified with `npx ng test --watch=false\n--include='**/visualization-frame-content.component.spec.ts'`. 7 tests\npassing.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Anthropic)\n\nCo-authored-by: Claude Opus 4.7 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-06-12T08:49:49Z",
          "tree_id": "51eb74c19345b89f13dc1cd076c417ddd74a2f6f",
          "url": "https://github.com/apache/texera/commit/b7b50798cbdab928d3928be36bd200984879d14c"
        },
        "date": 1781255311656,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 24438.839,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 35453.173,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 35453.173,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 107940.753,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 125287.137,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 125287.137,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 921055.808,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 981230.218,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 981230.218,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "yangz75@uci.edu",
            "name": "yangzhang75",
            "username": "yangzhang75"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5d74b610cf3c1990f7a70d3445dbdf2e6701f3a0",
          "message": "chore(pyright-language-service): remove unused hocon-parser and hoconjs dependencies (#5581)\n\n<!--\nThanks for sending a pull request (PR)! Here are some tips for you:\n1. If this is your first time, please read our contributor guidelines:\n[Contributing to\nTexera](https://github.com/apache/texera/blob/main/CONTRIBUTING.md)\n  2. Ensure you have added or run the appropriate tests for your PR\n  3. If the PR is work in progress, mark it a draft on GitHub.\n  4. Please write your PR title to summarize what this PR proposes, we \n    are following Conventional Commits style for PR titles as well.\n  5. Be sure to keep the PR description updated to reflect all changes.\n-->\n\n### What changes were proposed in this PR?\n<!--\nPlease clarify what changes you are proposing. The purpose of this\nsection\nis to outline the changes. Here are some tips for you:\n  1. If you propose a new API, clarify the use case for a new API.\n  2. If you fix a bug, you can clarify why it is a bug.\n  3. If it is a refactoring, clarify what has been changed.\n  3. It would be helpful to include a before-and-after comparison using \n     screenshots or GIFs.\n  4. Please consider writing useful notes for better and faster reviews.\n-->\n\nRemoves the dead hocon-parser integration from pyright-language-service.\nThe hoconParser call was removed in #3150 (when the language server\nbecame a standalone microservice) and the leftover import in #3415, but\nthe two dependencies and the type stub were never cleaned up.\n\n- Delete src/types/hocon-parser.d.ts (type stub for an unused module)\n- Remove hocon-parser and hoconjs from package.json\n- Regenerate yarn.lock via yarn install\n\n### Any related issues, documentation, discussions?\n<!--\nPlease use this section to link other resources if not mentioned\nalready.\n1. If this PR fixes an issue, please include `Fixes #1234`, `Resolves\n#1234`\nor `Closes #1234`. If it is only related, simply mention the issue\nnumber.\n  2. If there is design documentation, please add the link.\n  3. If there is a discussion in the mailing list, please add the link.\n-->\nCloses #5442\n\n### How was this PR tested?\n<!--\nIf tests were added, say they were added here. Or simply mention that if\nthe PR\nis tested with existing test cases. Make sure to include/update test\ncases that\ncheck the changes thoroughly including negative and positive cases if\npossible.\nIf it was tested in a way different from regular unit tests, please\nclarify how\nyou tested step by step, ideally copy and paste-able, so that other\nreviewers can\ntest and check, and descendants can verify in the future. If tests were\nnot added,\nplease describe why they were not added and/or why it was difficult to\nadd.\n-->\n\n- `grep -rn \"hocon\" pyright-language-service/src` returns nothing\n- The TypeScript build passes (`tsc --noEmit -p tsconfig.json`, exit 0)\n- No code in the service imports hocon-parser/hoconjs, so this is a pure\ndead-code removal\n\n### Was this PR authored or co-authored using generative AI tooling?\n<!--\nIf generative AI tooling has been used in the process of authoring this\nPR,\nplease include the phrase: 'Generated-by: ' followed by the name of the\ntool\nand its version. If no, write 'No'. \nPlease refer to the [ASF Generative Tooling\nGuidance](https://www.apache.org/legal/generative-tooling.html) for\ndetails.\n-->\nGenerated-by: Claude Code (Claude Opus 4.8)",
          "timestamp": "2026-06-12T08:56:51Z",
          "tree_id": "ced167a58d68b82ec2145a72bac159594ed50cb3",
          "url": "https://github.com/apache/texera/commit/5d74b610cf3c1990f7a70d3445dbdf2e6701f3a0"
        },
        "date": 1781255586073,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 25220.842,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 38958.255,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 38958.255,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 122446.822,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 143857.996,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 143857.996,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1093008.207,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1139429.056,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1139429.056,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "149845903+suyashj1231@users.noreply.github.com",
            "name": "Suyash Jain",
            "username": "suyashj1231"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "7b1c8dc7abca17465039aa5c043a302d3580b419",
          "message": "fix(file-service): apply LakeFS error handling to all call sites (#5607)\n\n### What changes were proposed in this PR?\n\n#4177 introduced `LakeFSExceptionHandler.withLakeFSErrorHandling`, but\nonly the multipart-upload and dataset-version paths used it. The\nremaining LakeFS call sites in `DatasetResource` either leaked raw\n`io.lakefs.clients.sdk.ApiException` to Dropwizard (an opaque 500 for\nthe frontend) or caught `Exception` and rewrapped it as a generic 500,\ndiscarding the real LakeFS status code (401/403/404/409/...).\n\n```\nBefore:  LakeFS 404  ->  raw ApiException / catch(Exception)  ->  500 \"Failed to ...\"\nAfter:   LakeFS 404  ->  withLakeFSErrorHandling              ->  404 \"Error while deleting file 'a.csv' ...: LakeFS resource not found. ...\"\n```\n\nChanges:\n\n| Change | Where |\n| --- | --- |\n| New overload `withLakeFSErrorHandling(operation: String)(call)` that\nprefixes the user-visible message with the failed operation |\n`LakeFSExceptionHandler.scala` |\n| 8 bare LakeFS calls now wrapped (size lookup, version listing, zip\ndownload, presigned URLs, cover image) | `DatasetResource.scala` |\n| 5 `catch Exception -> generic 500` blocks now use the handler;\ncompensation logic (DB rollback on failed repo init, multipart abort) is\npreserved, and the abort-on-failure cleanup no longer masks the original\nerror | `DatasetResource.scala` |\n\nIntentionally unchanged: best-effort cleanup sites that deliberately\nswallow errors, the per-dataset skip in `listDatasets`, and the\n`FileService` startup health check (failing fast at boot is correct\nthere).\n\n### Any related issues, documentation, discussions?\n\nCloses #4176\n\n### How was this PR tested?\n\nNew `LakeFSExceptionHandlerSpec` (7 unit cases): status-code mapping\n(400/401/403/404/409/4xx/5xx/unknown), operation context included in the\nfrontend-visible message, success passthrough, and non-LakeFS exceptions\npropagating untouched.\n\nNew integration case in `DatasetResourceSpec`: deleting a dataset whose\nLakeFS repository does not exist now yields `NotFoundException` (404)\ninstead of a generic 500.\n\n```\nsbt \"FileService/testOnly org.apache.texera.service.util.LakeFSExceptionHandlerSpec\"\n# Tests: succeeded 7, failed 0\nsbt \"FileService/testOnly org.apache.texera.service.resource.DatasetResourceSpec\"\n# Tests: succeeded 94, failed 0  (Testcontainers: LakeFS 1.51 + MinIO + Postgres)\n```\n\n`sbt FileService/scalafixAll` and `sbt FileService/scalafmtAll` produce\nno further diff.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nYes, partially. I (Suyash Jain) worked on this PR together with Claude\nCode as a pair-programming assistant. I reviewed the final diff and ran\nthe unit and Testcontainers-based integration suites locally before\nopening the PR.\n\nGenerated-by: Claude Code (Claude Opus 4.7)",
          "timestamp": "2026-06-12T16:47:17Z",
          "tree_id": "28e0db6c1d142960f9b551f3c515e2e6d775cf4b",
          "url": "https://github.com/apache/texera/commit/7b1c8dc7abca17465039aa5c043a302d3580b419"
        },
        "date": 1781283807638,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 26825.897,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 39676.273,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 39676.273,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 122633.622,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 150185.891,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 150185.891,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1099091.525,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1139162.117,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1139162.117,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "lie18@uci.edu",
            "name": "lie18uci",
            "username": "lie18uci"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "397d2757f3094818c96681261324cc9a9ff17763",
          "message": "test(frontend): add ConflictingFileModalContentComponent unit tests (#5631)\n\n### What changes were proposed in this PR?\n\nFrontend unit tests for ConflictingFileModalContentComponent are added\nin this PR.\n\nThe updated specification confirms that:\n\n1. The component has been successfully generated.\n2. The modal data inserted through NZ_MODAL_DATA is exposed by the\ncomponent.\n\nWithout altering current behavior, this increases test coverage for a\nminor presentational modal component.\n\n\n### Any related issues, documentation, discussions?\n\nCloses #5465\n\n\n### How was this PR tested?\nRan the following command locally from the frontend directory:\nyarn test\n--include='**/conflicting-file-modal-content.component.spec.ts'\nThe test passed successfully with 1 test file passed and 2 tests passed.\n\nAlso ran:\nyarn lint\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: ChatGPT\n\nChatGPT assisted me in finding the location of the component,\nstructuring the unit test, and executing the test command. I read\nthrough the guidance provided by ChatGPT, made the necessary changes\nlocally, and ran the test myself.",
          "timestamp": "2026-06-12T16:52:41Z",
          "tree_id": "a86659b9a1c4878619b19e3eb1f9d3e455059ddd",
          "url": "https://github.com/apache/texera/commit/397d2757f3094818c96681261324cc9a9ff17763"
        },
        "date": 1781284156891,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 24245.524,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 35243.738,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 35243.738,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 106982.496,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 123408.574,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 123408.574,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 927677.535,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 985220.118,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 985220.118,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "xinyual3@uci.edu",
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "d76a51e347f54c6c3ff43a7f8cd11f14ae5739ea",
          "message": "test(amber): add unit test coverage for FutureBijection and ElidableStatement (#5555)\n\n### What changes were proposed in this PR?\n\nPin behavior of two utility modules in `engine/common`. No\nproduction-code changes.\n\n| Spec | Source class | Tests |\n| --- | --- | --- |\n| `FutureBijectionSpec` | `FutureBijection` | 11 |\n| `ElidableStatementSpec` | `ElidableStatement` | 9 |\n\nBoth spec files follow the `<srcClassName>Spec.scala` one-to-one\nconvention.\n\n**Behavior pinned — `FutureBijection`**\n\n| Surface | Contract |\n| --- | --- |\n| `TwitterFuture.value.asScala` | resolves to the same value (type\npreserved, `null` preserved) |\n| `TwitterFuture.exception.asScala` | resolves with the same `Throwable`\ninstance (type, message, `eq` identity) |\n| `ScalaFuture.successful.asTwitter` | resolves to the same value (type\npreserved, `null` preserved) |\n| `ScalaFuture.failed.asTwitter` | resolves with the same `Throwable`\ninstance |\n| Twitter → Scala on an already-resolved future | the resulting Scala\nfuture is already completed when the implicit returns |\n| Twitter → Scala → Twitter round-trip | preserves both values and\nexceptions |\n| Scala → Twitter → Scala round-trip | preserves values |\n\n**Behavior pinned — `ElidableStatement`**\n\nThe texera build sets `-Xelide-below WARNING` (`amber/build.sbt`). Every\n`ElidableStatement` helper is annotated with an elide level **strictly\nbelow WARNING** (FINEST / FINER / FINE / INFO), so the Scala compiler\nreplaces every CALL to these helpers with a `()` Unit value at *compile*\ntime. The spec pins this silent-in-production contract:\n\n| Surface | Contract |\n| --- | --- |\n| `info` / `fine` / `finer` / `finest` (with side-effect block) | the\nside effect MUST NOT fire (counter stays at 0) |\n| same methods (with throwing block) | the exception MUST NOT propagate\n|\n| 1000 successive elided calls | no side-effect accumulation |\n| Return type | `Unit` (compile-time enforced) |\n| Parameter shape | accepts a `=> Unit` by-name block (compile-time\nenforced) |\n\nA regression that bumped a method's elide level above WARNING, removed\nthe `@elidable` annotation, or relaxed `-Xelide-below` in the build\nwould re-enable side effects in production — and this spec would catch\nit.\n\n### Any related issues, documentation, discussions?\n\nCloses #5551.\n\n### How was this PR tested?\n\nPure unit-test additions; verified locally with:\n\n- `sbt \"WorkflowExecutionService/testOnly\norg.apache.texera.amber.engine.common.FutureBijectionSpec\norg.apache.texera.amber.engine.common.ElidableStatementSpec\"` — 20\ntests, all green\n- `sbt scalafmtCheckAll` — clean\n- CI to confirm\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Sonnet 4.5)",
          "timestamp": "2026-06-12T17:06:53Z",
          "tree_id": "338e5b84790f546125613e0fa7259f4cfccdc911",
          "url": "https://github.com/apache/texera/commit/d76a51e347f54c6c3ff43a7f8cd11f14ae5739ea"
        },
        "date": 1781284964440,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 22228.472,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 36960.943,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 36960.943,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 103440.046,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 114705.18,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 114705.18,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 931752.226,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 975956.21,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 975956.21,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "xinyual3@uci.edu",
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "7e9cabf2bf4edc0540ab7397dd644bd96cc2a042",
          "message": "test(amber): add unit test coverage for WorkerBatchInternalQueue (#5553)\n\n### What changes were proposed in this PR?\n\nPin behavior of `WorkerBatchInternalQueue` — the per-DP-thread mailbox\ntrait used by the Python worker. Previously uncovered; the only\nuncovered module in the `pythonworker` package whose contract is\nunit-testable without standing up a real Python subprocess. No\nproduction-code changes.\n\n| Spec | Source class | Tests |\n| --- | --- | --- |\n| `WorkerBatchInternalQueueSpec` | `WorkerBatchInternalQueue` (trait +\ncompanion) | 17 |\n\nSpec file name follows the `<srcClassName>Spec.scala` one-to-one\nconvention.\n\n**Behavior pinned**\n\n| Surface | Contract |\n| --- | --- |\n| `enqueueData` + `getElement` | round-trip a `DataElement` (with both\n`DataFrame` and `StateFrame` payloads) |\n| `enqueueCommand` + `getElement` | round-trip a `ControlElement` |\n| `enqueueActorCommand` + `getElement` | round-trip an\n`ActorCommandElement` |\n| Multi-priority dispatch | control elements are returned **before**\ndata elements when both are queued (sub-queue 0 < 1) |\n| FIFO within the control queue | `ControlElement` enqueued first comes\nout before `ActorCommandElement` enqueued second |\n| `getDataQueueLength` | reports only data-queue items (control is\nexcluded) |\n| `getControlQueueLength` / `isControlQueueEmpty` | report all\ncontrol-queue items (`ControlElement` + `ActorCommandElement`) |\n| `disableDataQueue` | hides queued data from `getElement` until\n`enableDataQueue` is called; control flow still moves |\n| `getQueuedCredit(sender)` | `0` initially; tracks bytes-in minus\nbytes-out for `DataFrame` payloads per sender; stays `0` for control /\n`StateFrame` payloads; per-sender accounting is independent; accumulates\nacross multiple enqueues for the same sender |\n| Companion constants | `CONTROL_QUEUE == 0`, `DATA_QUEUE == 1`, and\n`CONTROL_QUEUE < DATA_QUEUE` (relied on by the multi-priority semantics)\n|\n\nThe trait is exercised through a small test-only subclass (`class\nTestQueue extends WorkerBatchInternalQueue`), with\n`DirectControlMessagePayload` represented by a local marker case object\nsince the production trait carries no behavior.\n\n### Any related issues, documentation, discussions?\n\nCloses #5552.\n\n### How was this PR tested?\n\nPure unit-test addition; verified locally with:\n\n- `sbt \"WorkflowExecutionService/testOnly\norg.apache.texera.amber.engine.architecture.pythonworker.WorkerBatchInternalQueueSpec\"`\n— 17 tests, all green\n- `sbt scalafmtCheckAll` — clean\n- CI to confirm\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Sonnet 4.5)\n\n---------\n\nSigned-off-by: Yicong Huang <17627829+Yicong-Huang@users.noreply.github.com>\nCo-authored-by: Yicong Huang <17627829+Yicong-Huang@users.noreply.github.com>\nCo-authored-by: Copilot Autofix powered by AI <175728472+Copilot@users.noreply.github.com>",
          "timestamp": "2026-06-12T17:20:18Z",
          "tree_id": "45f935bf4d928bf2f5aac288a4f68467dece18c6",
          "url": "https://github.com/apache/texera/commit/7e9cabf2bf4edc0540ab7397dd644bd96cc2a042"
        },
        "date": 1781285784738,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 21867.172,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 32549.324,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 32549.324,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 107208.565,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 146772.039,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 146772.039,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 927133.047,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 981102.548,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 981102.548,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mgball@uci.edu",
            "name": "Matthew B.",
            "username": "Ma77Ball"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "cb07f2d8b36ce89172c31461e9f3ef3f5b54de2e",
          "message": "feat: Add Card View to Workflows (#4216)\n\n<!--\nThanks for sending a pull request (PR)! Here are some tips for you:\n1. If this is your first time, please read our contributor guidelines:\n[Contributing to\nTexera](https://github.com/apache/texera/blob/main/CONTRIBUTING.md)\n  2. Ensure you have added or run the appropriate tests for your PR\n  3. If the PR is work in progress, mark it a draft on GitHub.\n  4. Please write your PR title to summarize what this PR proposes, we \n    are following Conventional Commits style for PR titles as well.\n  5. Be sure to keep the PR description updated to reflect all changes.\n-->\n\n### What changes were proposed in this PR?\n<!--\nPlease clarify what changes you are proposing. The purpose of this\nsection\nis to outline the changes. Here are some tips for you:\n  1. If you propose a new API, clarify the use case for a new API.\n  2. If you fix a bug, you can clarify why it is a bug.\n  3. If it is a refactoring, clarify what has been changed.\n  3. It would be helpful to include a before-and-after comparison using \n     screenshots or GIFs.\n  4. Please consider writing useful notes for better and faster reviews.\n-->\nThis PR adds a Grid View (Tile View) for workflows in the dashboard.\n\n- New Card Component: Displays workflows as tiles with a preview image.\n- Grid Layout: Responsive grid that adapts to screen size.\n- Enhanced Metadata: Shows size, dates, and view counts; pinned to the\nbottom.\n- Quick Actions: Edit description, rename, duplicate, and share directly\nfrom the card.\n- Toggle: Added a button to switch between List and Grid views.\n### Old View\n<img width=\"2269\" height=\"1009\" alt=\"image\"\nsrc=\"https://github.com/user-attachments/assets/0174952f-e760-4590-aed7-72c2dfdccd99\"\n/>\n\n### New View\n<img width=\"2560\" height=\"1410\" alt=\"image\"\nsrc=\"https://github.com/user-attachments/assets/d36ba290-a28f-44be-b406-c70ad43cace4\"\n/>\n\n### Any related issues, documentation, discussions?\n<!--\nPlease use this section to link other resources if not mentioned\nalready.\n1. If this PR fixes an issue, please include `Fixes #1234`, `Resolves\n#1234`\nor `Closes #1234`. If it is only related, simply mention the issue\nnumber.\n  2. If there is design documentation, please add the link.\n  3. If there is a discussion in the mailing list, please add the link.\n-->\nN/A\n\n### How was this PR tested?\n<!--\nIf tests were added, say they were added here. Or simply mention that if\nthe PR\nis tested with existing test cases. Make sure to include/update test\ncases that\ncheck the changes thoroughly, including negative and positive cases if\npossible.\nIf it was tested in a way different from regular unit tests, please\nclarify how\nyou tested step by step, ideally copy and paste-able, so that other\nreviewers can\ntest and check, and descendants can verify in the future. If tests were\nnot added,\nplease describe why they were not added and/or why it was difficult to\nadd.\n-->\n- Manually verified switching between views.\n- Checked card layout responsiveness.\n- Tested all card actions (edit, like, share, delete).\n\n### Was this PR authored or co-authored using generative AI tooling?\n<!--\nIf generative AI tooling has been used in the process of authoring this\nPR,\nplease include the phrase: 'Generated-by: ' followed by the name of the\ntool\nand its version. If no, write 'No'. \nPlease refer to the [ASF Generative Tooling\nGuidance](https://www.apache.org/legal/generative-tooling.html) for\ndetails.\n-->\nReviewed by Gemini 3\n\n---------\n\nCo-authored-by: Chen Li <chenli@gmail.com>\nCo-authored-by: Claude Opus 4.7 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-06-12T18:30:37Z",
          "tree_id": "8a1462d267d0135b957ddd89a32b5ddad38febb9",
          "url": "https://github.com/apache/texera/commit/cb07f2d8b36ce89172c31461e9f3ef3f5b54de2e"
        },
        "date": 1781289978340,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 21186.84,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 37576.136,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 37576.136,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 104039.09,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 127767.888,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 127767.888,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 906863.803,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1012403.904,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1012403.904,
            "unit": "us"
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
          "id": "227cbd73960afbcaa734b30f3ac108dc669324f3",
          "message": "fix(workflow-core): paginate S3 deleteDirectory deletions (#5569)\n\n### What changes were proposed in this PR?\n\n`S3StorageClient.deleteDirectory` listed objects with a single\n`listObjectsV2` call and issued one `deleteObjects` batch. Both S3 APIs\ncap at 1000 keys per call, so for any prefix holding more than 1000\nobjects only the first 1000 were deleted and the rest causes a storage\nleak. This affects dataset deletion (`DatasetResource`) and\nper-execution cleanup (`LargeBinaryManager`), either of which can exceed\n1000 objects under one prefix.\n\nThis PR:\n- Lists via `listObjectsV2Paginator`, which follows the continuation\ntoken across all pages, and deletes in batches of at most 1000 keys.\nKeys are streamed so memory stays bounded to a single batch.\n- Inspects each `DeleteObjects` response and throws if any key failed.\n\n### Any related issues, documentation, discussions?\n\nCloses #5281\n\n### How was this PR tested?\n\n1. Create more than 1000 files `for i in {1..1100}; do printf 'x' >\n\"file_$i.txt\"; done`\n2. Upload them in a dataset. (There is a frontend memory issue when you\nupload all 1100 files at the same time. Try to upload batch-by-batch)\n3. Delete the dataset.\n4. Check if all the files are removed in the minio console. (Before this\nfix, some files remain)\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Claude Opus 4.8)",
          "timestamp": "2026-06-12T18:53:01Z",
          "tree_id": "15289e189e9647c0225659d1cb1ad61c963e39ff",
          "url": "https://github.com/apache/texera/commit/227cbd73960afbcaa734b30f3ac108dc669324f3"
        },
        "date": 1781291263071,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 22134.895,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 34198.014,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 34198.014,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 107333.848,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 160327.366,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 160327.366,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 928059.124,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 950455.443,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 950455.443,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mgball@uci.edu",
            "name": "Matthew B.",
            "username": "Ma77Ball"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "3ab70f4d325a52e13b3c86806002ea2b5836d1ec",
          "message": "perf(pyamber): avoid per-read deepcopy in Tuple.as_dict() (#5599)\n\n### What changes were proposed in this PR?\n- Replace the per-read `deepcopy` in `Tuple.as_dict()`\n(`amber/src/main/python/core/models/tuple.py`) with a shallow copy, so\nreading a tuple no longer recursively clones every field value; cost now\nscales with field count instead of total field byte size.\n- This path is hot: `as_dict()` backs `as_series()` (per-tuple in the\nbatch operator path) and `as_key_value_pairs()`; a tuple carrying a\nlarge binary field previously duplicated that whole payload on every\nread.\n- The deepcopy's isolation was unnecessary: `as_dict()` has no callers\noutside `Tuple`, its two users immediately build a new container, and\nthe Tuple's mutators only reassign dict slots (never mutate a value in\nplace), so a shallow copy preserves the independent-dict contract.\n- Remove the now-unused `from copy import deepcopy` import and document\nwhy the shallow copy is safe.\n### Any related issues, documentation, discussions?\nCloses: #5598\n### How was this PR tested?\n- Existing tests only, no behavior change. Run `cd amber/src/main/python\n&& python -m pytest ../../test/python/core/models/test_tuple.py -q`,\nexpect 23 passed (covers `as_dict`/`as_series`/`as_key_value_pairs`).\n- Run `cd amber/src/main/python && python -m pytest\n../../test/python/core/runnables/test_main_loop.py\n../../test/python/core/architecture/managers/test_tuple_processing_manager.py\n-q`, expect 22 passed (exercises the batch read path that calls\n`as_series`).\n### Was this PR authored or co-authored using generative AI tooling?\nGenerated-by: Claude Opus 4.8\n\n---------\n\nCo-authored-by: Yicong Huang <17627829+Yicong-Huang@users.noreply.github.com>",
          "timestamp": "2026-06-12T20:15:59Z",
          "tree_id": "c6e3abfbba3a7794a4eae487f7cab98f78a712cf",
          "url": "https://github.com/apache/texera/commit/3ab70f4d325a52e13b3c86806002ea2b5836d1ec"
        },
        "date": 1781296483328,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 24038.723,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 33579.65,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 33579.65,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 123658.392,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 187084.801,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 187084.801,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1066695.725,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1101965.911,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1101965.911,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "149845903+suyashj1231@users.noreply.github.com",
            "name": "Suyash Jain",
            "username": "suyashj1231"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "d5f5e12fb6879f15dbcf0c9cf6aaae3b532784e6",
          "message": "fix(workflow-operator): no null padding in reservoir sampling (#5606)\n\n### What changes were proposed in this PR?\n\n`ReservoirSamplingOpExec` allocates a fixed-size reservoir of length\n`count` (the per-worker share of `k`). When a worker receives fewer\ntuples than `count`, only the first `n` slots are filled, but `onFinish`\nreturned the whole array, yielding `count - n` trailing `null` entries.\nThe nulls are currently swallowed by a distant null-guard in\n`DataProcessor`, so the bug is latent — but the operator violates the\n\"do not emit null tuples\" contract and breaks if that guard is ever\nnarrowed or bypassed.\n\n```\nBefore:  input < k  ->  onFinish emits [t0 .. tn-1, null, ..., null]  (engine guard hides them)\nAfter:   input < k  ->  onFinish emits [t0 .. tn-1]                   (no nulls emitted at all)\n```\n\nThe fix emits only the filled prefix:\n\n```scala\noverride def onFinish(port: Int): Iterator[TupleLike] = reservoir.iterator.take(n)\n```\n\n`take(n)` is a no-op when `n >= count` (input ≥ k), so the sampled\noutput is unchanged in the normal case.\n\n### Any related issues, documentation, discussions?\n\nCloses #5592\n\n### How was this PR tested?\n\nAdded three regression cases to `ReservoirSamplingOpExecSpec`:\n\n| Case | Asserts |\n| --- | --- |\n| `input size < k` | only the received tuples are emitted, in order, no\nnulls |\n| empty input | `onFinish` emits nothing |\n| skewed partitioning (`k=10`, 3 workers, worker 0 gets 2 tuples) | no\nnull padding for an under-filled worker share |\n\nAll three fail against the old `reservoir.iterator` and pass with\n`reservoir.iterator.take(n)`; the 9 pre-existing cases stay green (TDD\nred → green verified by stashing the source fix).\n\n```\nsbt \"WorkflowOperator/testOnly org.apache.texera.amber.operator.reservoirsampling.ReservoirSamplingOpExecSpec\"\n# Tests: succeeded 12, failed 0, canceled 0, ignored 0, pending 0\n```\n\n`sbt WorkflowOperator/scalafixAll` and `sbt\nWorkflowOperator/scalafmtAll` produce no further diff.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nYes, partially. I (Suyash Jain) worked on this PR together with Claude\nCode as a pair-programming assistant. I reviewed the final diff, ran the\nspec locally, and verified the red → green behavior of the new\nregression tests myself before opening the PR.\n\nGenerated-by: Claude Code (Claude Opus 4.7)\n\nCo-authored-by: Xuan Gu <162244362+xuang7@users.noreply.github.com>",
          "timestamp": "2026-06-12T20:26:57Z",
          "tree_id": "164ab7d040ed744e4bbdbed13ea4b521b4438ecd",
          "url": "https://github.com/apache/texera/commit/d5f5e12fb6879f15dbcf0c9cf6aaae3b532784e6"
        },
        "date": 1781297020463,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 23593.478,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 37873.262,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 37873.262,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 121474.068,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 154071.734,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 154071.734,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1060887.513,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1093200.823,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1093200.823,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "sarah_asad@live.com",
            "name": "Sarah Asad",
            "username": "SarahAsad23"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "fa5fcbb60b6f0a305a21635e2560ca0b04b823e2",
          "message": "feat: Make Python Virtual Environment Persistent: Add Environments to Left Panel  (#5577)\n\n<!--\nThanks for sending a pull request (PR)! Here are some tips for you:\n1. If this is your first time, please read our contributor guidelines:\n[Contributing to\nTexera](https://github.com/apache/texera/blob/main/CONTRIBUTING.md)\n  2. Ensure you have added or run the appropriate tests for your PR\n  3. If the PR is work in progress, mark it a draft on GitHub.\n  4. Please write your PR title to summarize what this PR proposes, we \n    are following Conventional Commits style for PR titles as well.\n  5. Be sure to keep the PR description updated to reflect all changes.\n-->\n\n### What changes were proposed in this PR?\n<!--\nPlease clarify what changes you are proposing. The purpose of this\nsection\nis to outline the changes. Here are some tips for you:\n  1. If you propose a new API, clarify the use case for a new API.\n  2. If you fix a bug, you can clarify why it is a bug.\n  3. If it is a refactoring, clarify what has been changed.\n  3. It would be helpful to include a before-and-after comparison using \n     screenshots or GIFs.\n  4. Please consider writing useful notes for better and faster reviews.\n-->\n\nThis PR introduces persistent Python Virtual Environments (PVEs) by\nmoving them out of the Computing Unit (CU) lifecycle and storing them in\nthe database.\n\nPreviously, PVEs were managed through Computing Units and existed only\nwithin the CU they were created in. As a result, PVEs were lost when the\ncorresponding CU was terminated. This PR adds a new\n`virtual_environments` table to persist PVE configurations and\nintroduces a dedicated dashboard interface for managing them.\n\nUsers can now create, view, update, and delete their own Python virtual\nenvironments through a new \"Environments\" page in the dashboard sidebar.\nPVE definitions are stored as user-owned resources in the database and\ncan be managed independently of Computing Units.\n\n<img width=\"1689\" height=\"652\" alt=\"Screenshot 2026-06-08 at 6 39 55 PM\"\nsrc=\"https://github.com/user-attachments/assets/82711baf-b1ce-4cc6-9e84-a29a230ddc3a\"\n/>\n\n<img width=\"1461\" height=\"500\" alt=\"Screenshot 2026-06-08 at 6 40 19 PM\"\nsrc=\"https://github.com/user-attachments/assets/5bbbc360-0adf-401b-8ae8-6d9597d486c2\"\n/>\n\nNote: This PR only introduces persistence for PVE metadata and\nconfiguration. Creating, updating, and deleting a PVE in this PR only\naffects the corresponding database records. The execution-time behavior\nof materializing and using these virtual environments inside a Computing\nUnit is not part of this change and will be introduced in a future PR.\n\nK8s configurations for this feature will be added in a future PR. \n\n### Any related issues, documentation, discussions?\n<!--\nPlease use this section to link other resources if not mentioned\nalready.\n1. If this PR fixes an issue, please include `Fixes #1234`, `Resolves\n#1234`\nor `Closes #1234`. If it is only related, simply mention the issue\nnumber.\n  2. If there is design documentation, please add the link.\n  3. If there is a discussion in the mailing list, please add the link.\n-->\n\nRelated discussions and issues: #5360, #5361.\n\n### How was this PR tested?\n<!--\nIf tests were added, say they were added here. Or simply mention that if\nthe PR\nis tested with existing test cases. Make sure to include/update test\ncases that\ncheck the changes thoroughly including negative and positive cases if\npossible.\nIf it was tested in a way different from regular unit tests, please\nclarify how\nyou tested step by step, ideally copy and paste-able, so that other\nreviewers can\ntest and check, and descendants can verify in the future. If tests were\nnot added,\nplease describe why they were not added and/or why it was difficult to\nadd.\n-->\n\nTested manually and tests added to PveResourceSpec. \n\n### Was this PR authored or co-authored using generative AI tooling?\n<!--\nIf generative AI tooling has been used in the process of authoring this\nPR,\nplease include the phrase: 'Generated-by: ' followed by the name of the\ntool\nand its version. If no, write 'No'. \nPlease refer to the [ASF Generative Tooling\nGuidance](https://www.apache.org/legal/generative-tooling.html) for\ndetails.\n-->\n\nCo-authored using: Claude Code",
          "timestamp": "2026-06-12T20:38:25Z",
          "tree_id": "eb69a6fef9622a398e2fd5de0467a0bd3fa96d5c",
          "url": "https://github.com/apache/texera/commit/fa5fcbb60b6f0a305a21635e2560ca0b04b823e2"
        },
        "date": 1781297586853,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 26516.417,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 44316.137,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 44316.137,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 117126.437,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 143069.393,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 143069.393,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1043251.696,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1077172.723,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1077172.723,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "162244362+xuang7@users.noreply.github.com",
            "name": "Xuan Gu",
            "username": "xuang7"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "73c76f51920b0900de67bbc0baa1ee5be5b87bf0",
          "message": "chore(licensing): refresh transitive versions in LICENSE-binary-python (#5650)\n\n### What changes were proposed in this PR?\n\nRefresh three transitive Python dependency versions in\n`amber/LICENSE-binary-python` to match the bundled wheels, as reported\nby the License Binary Checker ([release\nrun](https://github.com/apache/texera/actions/runs/27444261750) and this\nPR's CI):\n\n- filelock 3.29.1 → 3.29.3\n- huggingface-hub 1.18.0 → 1.19.0\n- matplotlib 3.10.9 → 3.11.0\n\nVersion-only refresh; license groupings unchanged. The file is identical\non `main` and `release/v1.2`, so this PR fixes both via the\n`release/v1.2` backport label.\n\n### Any related issues, documentation, discussions?\n\nResolves #5649\n\n### How was this PR tested?\n\nLicense Binary Checker CI on this PR.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Claude Fable 5)",
          "timestamp": "2026-06-12T22:19:18Z",
          "tree_id": "2891f97620ae3450ebfd91d239d9f383db28c3aa",
          "url": "https://github.com/apache/texera/commit/73c76f51920b0900de67bbc0baa1ee5be5b87bf0"
        },
        "date": 1781303757354,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 23004.083,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 31219.877,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 31219.877,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 119184.212,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 160656.345,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 160656.345,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1048502.978,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1107533.754,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1107533.754,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "xinyual3@uci.edu",
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "190823f7562ba8c0bb2a515b0ce4823cf640e049",
          "message": "test(workflow-operator): add unit test coverage for CaseSensitiveAnalyzer (#5658)\n\n### What changes were proposed in this PR?\n\nPin behavior of the Lucene `Analyzer` used by the keyword-search\noperator when the user opts into case-sensitive matching. The\nabstraction skips the lowercasing pipeline used by `StandardAnalyzer`,\nso a regression here would silently downgrade case-sensitive search. No\nproduction-code changes.\n\n| Spec | Source class | Tests |\n| --- | --- | --- |\n| `CaseSensitiveAnalyzerSpec` | `CaseSensitiveAnalyzer` | 13 |\n\nSpec file name follows the `<srcClassName>Spec.scala` one-to-one\nconvention.\n\n**Behavior pinned**\n\n| Surface | Contract |\n| --- | --- |\n| Mixed-case input | every emitted token preserves its original case |\n| All-uppercase / all-lowercase tokens | preserved (no normalization in\neither direction) |\n| Single-space splitting | tokens are separated cleanly |\n| Tabs and newlines | also split tokens |\n| Collapsed whitespace runs | no empty tokens emitted |\n| Embedded punctuation (`abc,def`) | stays one token\n(`WhitespaceTokenizer` only splits on whitespace) |\n| Sentence-final punctuation (`Hello, world!`) | stays attached\n(`Hello,`, `world!`) |\n| Empty input | no tokens |\n| Pure-whitespace input | no tokens |\n| `StopFilter` with `CharArraySet.EMPTY_SET` | English stop words (`the`\n/ `and` / `a`) are NOT removed (vs `StandardAnalyzer`'s default\nbehavior) |\n| Different field names | same tokenization (field-name independent) |\n| Successive `tokenStream` calls | each gets its own independent stream\n|\n\nThe harness uses the canonical Lucene `reset → incrementToken → end →\nclose` lifecycle and collects `CharTermAttribute` values into a buffer —\nsame pattern any future analyzer spec in this codebase should follow.\n\n### Any related issues, documentation, discussions?\n\nCloses #5654.\n\n### How was this PR tested?\n\nPure unit-test addition; verified locally with:\n\n- `sbt \"WorkflowOperator/testOnly\norg.apache.texera.amber.operator.keywordSearch.CaseSensitiveAnalyzerSpec\"`\n— 13 tests, all green\n- `sbt scalafmtCheckAll` — clean\n- CI to confirm\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.7 [1M context])",
          "timestamp": "2026-06-13T00:19:41Z",
          "tree_id": "f611d8cc0bcd54dca2e32c8d9c294bd948752aed",
          "url": "https://github.com/apache/texera/commit/190823f7562ba8c0bb2a515b0ce4823cf640e049"
        },
        "date": 1781310930028,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 23385.296,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 35616.585,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 35616.585,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 117791.334,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 150001.411,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 150001.411,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1037502.723,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1056481.771,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1056481.771,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "xinyual3@uci.edu",
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "e0a96478817a5c7a2f585de1952e40f7c8ba534f",
          "message": "test(workflow-operator): add unit test coverage for AutoClosingIterator and UnionOpExec (#5657)\n\n### What changes were proposed in this PR?\n\nPin behavior of two previously-uncovered helpers in\n`common/workflow-operator`. No production-code changes.\n\n| Spec | Source class | Tests |\n| --- | --- | --- |\n| `AutoClosingIteratorSpec` | `AutoClosingIterator` | 10 |\n| `UnionOpExecSpec` | `UnionOpExec` | 7 |\n\nBoth spec files follow the `<srcClassName>Spec.scala` one-to-one\nconvention.\n\n**Behavior pinned — `AutoClosingIterator`**\n\n| Surface | Contract |\n| --- | --- |\n| `hasNext` (non-empty source) | `true`; `onClose` not invoked |\n| `hasNext` (exhausted source) | `false`; `onClose` invoked exactly once\n|\n| Repeated `hasNext` after exhaustion | does NOT re-fire `onClose`\n(`alreadyClosed` guard) |\n| `next()` | delegates straight to the wrapped iterator (in order) |\n| Full traversal via `toList` | yields every element; `onClose` fires\nonce at the end |\n| Already-empty source | first `hasNext` returns `false` and fires\n`onClose` |\n| Mid-iteration | `onClose` stays un-fired between elements |\n| `onClose` throws | exception propagates (no swallowing) |\n| `onClose` throws + retry | current impl re-fires `onClose` (assignment\nto `alreadyClosed` runs AFTER `onClose()`); characterization pins this\nbehavior |\n\n**Behavior pinned — `UnionOpExec`**\n\n| Surface | Contract |\n| --- | --- |\n| `processTuple(tuple, port = 0)` | yields a single-element iterator\ncontaining the tuple |\n| `processTuple` (any port) | port-agnostic — same tuple passes through\nfor ports 0, 1, 5, 99, MaxValue, -1 |\n| Tuple identity | pass-through preserves the exact `Tuple` reference\n(no copy) |\n| Successive calls | each returns an independent fresh iterator (no\nshared cursor) |\n| Per-call iterator | yields exactly one element |\n| `null` tuple | passes through as `null` (the impl does not null-check)\n|\n| Type contract | `UnionOpExec` is an `OperatorExecutor` |\n\n### Any related issues, documentation, discussions?\n\nCloses #5653.\n\n### How was this PR tested?\n\nPure unit-test additions; verified locally with:\n\n- `sbt \"WorkflowOperator/testOnly\norg.apache.texera.amber.operator.source.scan.AutoClosingIteratorSpec\norg.apache.texera.amber.operator.union.UnionOpExecSpec\"` — 17 tests, all\ngreen\n- `sbt scalafmtCheckAll` — clean\n- CI to confirm\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.7 [1M context])",
          "timestamp": "2026-06-13T00:22:16Z",
          "tree_id": "ade9f49ac2e186c74ad0214bce109814859ccfa2",
          "url": "https://github.com/apache/texera/commit/e0a96478817a5c7a2f585de1952e40f7c8ba534f"
        },
        "date": 1781311179610,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 21125.837,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 32942.286,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 32942.286,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 103313.683,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 126729.69,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 126729.69,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 906162.735,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 978269.424,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 978269.424,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "xinyual3@uci.edu",
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "7ae9b35f12748616daf7bcc925fdde2e5def5187",
          "message": "test(workflow-operator): add unit test coverage for filter-family operator executors (#5656)\n\n### What changes were proposed in this PR?\n\nPin behavior of four previously-uncovered modules in the `FilterOpExec`\ninheritance hierarchy in `common/workflow-operator`. No production-code\nchanges.\n\n| Spec | Source class | Tests |\n| --- | --- | --- |\n| `FilterOpExecSpec` | `FilterOpExec` (abstract base) | 9 |\n| `RegexOpExecSpec` | `RegexOpExec` | 8 |\n| `SubstringSearchOpExecSpec` | `SubstringSearchOpExec` | 10 |\n| `RandomKSamplingOpExecSpec` | `RandomKSamplingOpExec` | 7 |\n\nAll four spec files follow the `<srcClassName>Spec.scala` one-to-one\nconvention. `SpecializedFilterOpExec` already has its own spec; this PR\ncovers the rest of the family.\n\n**Behavior pinned — `FilterOpExec`**\n\n| Surface | Contract |\n| --- | --- |\n| `processTuple` (matching predicate) | yields the input tuple as a\nsingle-element iterator |\n| `processTuple` (non-matching predicate) | yields an empty iterator |\n| `processTuple` | passes the actual tuple instance to the predicate;\nignores the `port` argument |\n| `setFilterFunc` | swapping the predicate changes the next\n`processTuple` result; value-aware predicates branch per-tuple |\n| Type contract | `FilterOpExec` is a `Serializable OperatorExecutor` |\n\n**Behavior pinned — `RegexOpExec`**\n\n| Surface | Contract |\n| --- | --- |\n| matching regex | yields the tuple |\n| find-semantics | unanchored substring match (not full-string\n`matches`) |\n| `caseInsensitive = true` / `false` | matches case-(in)sensitively |\n| invalid regex string | construction succeeds (lazy `Pattern`);\n`PatternSyntaxException` surfaces on first `processTuple` |\n| repeated invocations | pattern stays cached; results are stable |\n| malformed descriptor JSON | construction throws\n`JsonProcessingException` |\n\n**Behavior pinned — `SubstringSearchOpExec`**\n\n| Surface | Contract |\n| --- | --- |\n| substring present / absent | yields tuple / nothing |\n| position in value (start / middle / end) | irrelevant —\n`String.contains` semantics |\n| `isCaseSensitive = true` / `false` | case-(in)sensitive (lowercased\nequality on both sides) |\n| empty substring | matches every value, including the empty string |\n| repeated invocations | results stable |\n| malformed descriptor JSON | construction throws\n`JsonProcessingException` |\n\n**Behavior pinned — `RandomKSamplingOpExec`**\n\n| Surface | Contract |\n| --- | --- |\n| `percentage = 100` | accepts every tuple (1000-sample run) |\n| `percentage = 0` | rejects every tuple (1000-sample run) |\n| Same `workerCount` + `percentage` | identical emission count across\ntwo fresh instances (deterministic seed) |\n| `percentage = 50` | approximately half pass (within ±150 of 1000 over\n2000 draws) |\n| Different `workerCount` | divergent emission sequences (the seed is\n`workerCount`) |\n| malformed descriptor JSON | construction throws\n`JsonProcessingException` |\n\n`FilterOpExec` is abstract, so the spec uses a minimal test-only\nconcrete subclass that exposes `setFilterFunc` for behavior-only\nassertions. The three subclass specs build descriptor JSON via\n`objectMapper.writeValueAsString` of a fresh `*OpDesc` (same fixture\npattern as the existing `SpecializedFilterOpExecSpec`).\n\n### Any related issues, documentation, discussions?\n\nCloses #5652.\n\n### How was this PR tested?\n\nPure unit-test additions; verified locally with:\n\n- `sbt \"WorkflowOperator/testOnly\norg.apache.texera.amber.operator.filter.FilterOpExecSpec\norg.apache.texera.amber.operator.regex.RegexOpExecSpec\norg.apache.texera.amber.operator.substringSearch.SubstringSearchOpExecSpec\norg.apache.texera.amber.operator.randomksampling.RandomKSamplingOpExecSpec\"`\n— 34 tests, all green\n- `sbt scalafmtCheckAll` — clean\n- CI to confirm\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.7 [1M context])",
          "timestamp": "2026-06-13T00:30:08Z",
          "tree_id": "57e3eee2ddc27b399f843bfc0bdff1f893477272",
          "url": "https://github.com/apache/texera/commit/7ae9b35f12748616daf7bcc925fdde2e5def5187"
        },
        "date": 1781311468840,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 24805.263,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 38344.907,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 38344.907,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 133219.446,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 173178.494,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 173178.494,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1059717.997,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1104221.303,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1104221.303,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "xinyual3@uci.edu",
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "6032413f6f1e65b6a32e53542c4668f00698ec26",
          "message": "test(amber): add unit test coverage for LogicalPlan (#5441)\n\n### What changes were proposed in this PR?\n\nAdds `LogicalPlanSpec` covering\n`amber/src/main/scala/org/apache/texera/workflow/LogicalPlan.scala` —\nthe user-facing logical workflow graph case class plus its companion\nfactory.\n\n| Surface | Pinned |\n| --- | --- |\n| Construction | `LogicalPlan(operators, links)` exposes both fields\nverbatim. |\n| `LogicalPlan.apply(LogicalPlanPojo)` | Lifts the POJO's operators +\nlinks into a `LogicalPlan`, ignoring the POJO-only `opsToViewResult` /\n`opsToReuseResult` fields. |\n| `getTopologicalOpIds` | Topological order on a linear chain; respects\nedge directionality across a fan-out (`a → b, a → c`) — source first,\ntwo sinks unordered in the tail. |\n| `getOperator` | Returns the operator with the requested id; throws\n`NoSuchElementException` for an unknown id. |\n| `getTerminalOperatorIds` | Single sink in a linear chain; every\nout-degree-0 operator in a fan-out plan; every operator when there are\nno links; empty list for an empty plan. |\n| `getUpstreamLinks` | Returns every link whose `toOpId` matches the\nargument; preserves construction order when multiple links flow into the\nsame target; returns an empty list when nothing flows in. |\n| `resolveScanSourceOpFileName` | Failures with `Some(errorList)` are\nappended per-operator instead of throwing; with `None` the first failure\nrethrows; non-`ScanSourceOpDesc` operators are left untouched (no\nerrors, no resolution). Failures are forced deterministically by\npointing a `ScanSourceOpDesc` fixture at a non-existent file path. |\n\nA happy-path `resolveScanSourceOpFileName` test is intentionally\nomitted: `FileResolver` reaches the LakeFS / dataset service in\nproduction and is environment-dependent, so a deterministic unit test\nwould have to mock that surface — out of scope for this spec.\n\nNo production code changed; this is test-only.\n\n### Any related issues, documentation, discussions?\n\nCloses #5438\n\n### How was this PR tested?\n\n```\nsbt \"WorkflowExecutionService/Test/testOnly org.apache.texera.workflow.LogicalPlanSpec\"\n# → 16 tests, all pass\n\nsbt \"WorkflowExecutionService/Test/scalafmtCheck\"\n# → clean\n```\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Claude Opus 4.7)",
          "timestamp": "2026-06-13T17:17:17Z",
          "tree_id": "9dc71d1c975c6d1272c2d271d85a342f4cc0310f",
          "url": "https://github.com/apache/texera/commit/6032413f6f1e65b6a32e53542c4668f00698ec26"
        },
        "date": 1781372025844,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 22337.377,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 40265.725,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 40265.725,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 100560.926,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 135435.331,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 135435.331,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 889259.687,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 925686.333,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 925686.333,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mengw15@uci.edu",
            "name": "Meng Wang",
            "username": "mengw15"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "515d37221adb3d3e2ae282a8bd82151f547c3d51",
          "message": "test(frontend): extend GmailService spec to cover all methods (#5460)\n\n### What changes were proposed in this PR?\n\nExtends the existing `gmail.service.spec.ts` (added in #5164, which\ncovered `sendEmail`'s success/error toasts) to cover the remaining\nsurface of the 3-method service:\n\n- `sendEmail` request-body shape — explicit receiver, and the\nempty-string default when omitted\n- `sendEmail` error branch also logs `console.error(\"Send email error:\",\n…)`\n- `getSenderEmail()` — a `GET` to `/gmail/sender/email` (text) that\nemits the body with no `NotificationService` side-effect\n- `notifyUnauthorizedLogin` — POST body shape, success toast, and error\ntoast + `console.error` logging\n\nFollows `frontend/TESTING.md` (Vitest, `HttpClientTestingModule`).\n\n### Any related issues, documentation, discussions?\n\nCloses #5456. Builds on #5164.\n\n### How was this PR tested?\n\n`yarn test --include='**/gmail.service.spec.ts'` → 9 passed. `prettier\n--check` clean.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (claude-opus-4-7)",
          "timestamp": "2026-06-13T17:18:45Z",
          "tree_id": "47f15aad73a3f32861d6965c95a3df7035331d92",
          "url": "https://github.com/apache/texera/commit/515d37221adb3d3e2ae282a8bd82151f547c3d51"
        },
        "date": 1781372249546,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 18748.837,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 26351.459,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 26351.459,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 82212.681,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 104760.768,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 104760.768,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 689348.359,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 744235.069,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 744235.069,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "91584519+PG1204@users.noreply.github.com",
            "name": "Prateek Ganigi",
            "username": "PG1204"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "99b9ca281dd4524b0423bac9593ce1f9f136f14d",
          "message": "test(frontend): share workflow-editor TestBed setup, drop double-run (#5626)\n\n### What changes were proposed in this PR?\nAddresses the two review comments on\n[#5318](https://github.com/apache/texera/issues/5318)'s follow-up\nimplementation:\n\n1. Share a common TestBed setup so the jsdom and browser specs don't\ndrift.\n\nThe .browser.spec.ts split created two TestBed configurations for the\nsame component - one in workflow-editor.component.spec.ts (External\nModule Integration describe), one in workflow-editor.browser.spec.ts.\nBoth configured nearly identical imports/providers arrays, which would\ninevitably drift over time.\nExtracted them into a new sibling file workflow-editor.test-utils.ts\nexporting two arrays:\n\n\nexport const workflowEditorTestImports = [ ... ];\nexport const workflowEditorTestProviders: Provider[] = [ ... ];\nThis follows the existing project convention in\n[frontend/src/app/common/testing/test-utils.ts](vscode-webview://1epki5h79lmkghv36u4evg54fuvmk17ndjca203mcg7opnnd5sg9/frontend/src/app/common/testing/test-utils.ts)\n(which exports commonTestImports and commonTestProviders the same way).\nEach spec's TestBed now collapses to:\n\n\nawait TestBed.configureTestingModule({\n  imports: workflowEditorTestImports,\n  providers: workflowEditorTestProviders,\n}).compileComponents();\nAdding or removing a service from either spec's setup is now a single\nedit in one file.\n\n2. Drop the explicit workflow-editor.component.spec.ts entry from the\ntest-browser include in angular.json.\n\nWith the six mouse-event tests now living in\nworkflow-editor.browser.spec.ts (picked up by the **/*.browser.spec.ts\nglob), the explicit listing of workflow-editor.component.spec.ts in\ntest-browser's include array was causing the file's 25 jsdom-friendly\ntests to run twice, once in jsdom (the default test target) and once in\nreal Chrome (the test-browser target). Removed that explicit entry; the\ntest-browser target now picks up only **/*.browser.spec.ts files.\n\nScope note. The pre-existing JointJS Paper describe block at the top of\nworkflow-editor.component.spec.ts has its own deliberately-different\nTestBed setup (ContextMenuComponent instead of\nNzModalCommentBoxComponent, Overlay, MockComputingUnitStatusService) and\nwas left untouched, it wasn't part of the duplication introduced by the\n.browser.spec.ts split.\n\n### Any related issues, documentation, discussions?\nAddresses review feedback on the follow-up PR for\n[#5318](https://github.com/apache/texera/issues/5318).\nCloses #5318 \nRelated to #3614 / PR #5146 (this PR restores tests that were commented\nout during PR #5146 as collateral from the #3614 fix).\n\n\n### How was this PR tested?\nExisting test runs: Verified no test-count regressions in either target,\nand the double-run is gone:\n\nng test (jsdom, full suite): 947 pass / 0 fail / 2 skipped / 1 todo\nacross 103 files\nng run gui:test-browser: 13 pass / 0 fail across 2 files (6 from\nworkflow-editor.browser.spec.ts + 7 from the pre-existing\ncode-editor.component.browser.spec.ts)\nworkflow-editor.component.spec.ts under jsdom: 25/25 pass (unchanged\nfrom before this PR)\nBrowser test count dropped from 38 -> 13, confirming the 25\njsdom-friendly tests in workflow-editor.component.spec.ts no longer\ndouble-run in real Chrome.\n\nStatic checks: tsc --noEmit and eslint clean on all four changed files.\n\n### Was this PR authored or co-authored using generative AI tooling?\nCo-authored-by: Claude Code (Anthropic Claude Opus 4.7)\n\n---------\n\nCo-authored-by: Claude Opus 4.7 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-06-13T17:32:57Z",
          "tree_id": "1d7d21a43f56d77acba5d876421fcaacd0250392",
          "url": "https://github.com/apache/texera/commit/99b9ca281dd4524b0423bac9593ce1f9f136f14d"
        },
        "date": 1781372837241,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 23497.593,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 32827.079,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 32827.079,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 107585.77,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 118825.536,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 118825.536,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 896855.439,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 950291.512,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 950291.512,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "133594317+justinsiek@users.noreply.github.com",
            "name": "Justin Siek",
            "username": "justinsiek"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "1a65a3168c5ffbd171e8ed0d64dec495e45c1b24",
          "message": "fix(workflow-operator): set alreadyClosed before onClose (#5678)\n\n### What changes were proposed in this PR?\n\n`AutoClosingIterator.hasNext` only set `alreadyClosed = true` after\ncalling `onClose()`, and so if `onClose()` throws, `alreadyClosed` would\nstay false, and so a subsequent `hasNext` would reinvoke `onClose()`,\nrunning cleanup a second time on a resource whose close already failed.\n\nThe change makes `alreadyClosed = true` run before `onClose()`.\n\n### Any related issues, documentation, discussions?\n\nCloses #5660 \n\n### How was this PR tested?\n\nUpdated AutoClosingIteratorSpec — replaced the existing characterization\ntest (\"re-invoke onClose on a retry when the previous onClose threw\")\nwith a positive assertion that a second hasNext after a throwing close\ndoes NOT re-invoke onClose (closeCount stays at 1) and returns false.\n\n`sbt \"WorkflowOperator/testOnly\norg.apache.texera.amber.operator.source.scan.AutoClosingIteratorSpec\"`\n- 10/10 pass. `sbt scalafmtCheckAll` passes.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Claude Opus 4.8)\n\nCo-authored-by: Justin Siek <justinsiek@Justins-MacBook-Air.local>",
          "timestamp": "2026-06-13T17:51:17Z",
          "tree_id": "def41efc3df06efdc98d8d418cd0036e1f7a88f8",
          "url": "https://github.com/apache/texera/commit/1a65a3168c5ffbd171e8ed0d64dec495e45c1b24"
        },
        "date": 1781373961187,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 21179.594,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 32330.656,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 32330.656,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 101587.556,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 147869.303,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 147869.303,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 895292.224,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 934381.725,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 934381.725,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "123780557+KYinXu@users.noreply.github.com",
            "name": "Kyle Yin Xu",
            "username": "KYinXu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "6becb8596df32058f69473c39f4a9028f149954e",
          "message": "test(workflow-operator): add ImageUtilitySpec for encodeImageToHTML (#5679)\n\n<!--\nThanks for sending a pull request (PR)! Here are some tips for you:\n1. If this is your first time, please read our contributor guidelines:\n[Contributing to\nTexera](https://github.com/apache/texera/blob/main/CONTRIBUTING.md)\n  2. Ensure you have added or run the appropriate tests for your PR\n  3. If the PR is work in progress, mark it a draft on GitHub.\n  4. Please write your PR title to summarize what this PR proposes, we \n    are following Conventional Commits style for PR titles as well.\n  5. Be sure to keep the PR description updated to reflect all changes.\n-->\n\n### What changes were proposed in this PR?\n\nPin the Python snippet emitted by ImageUtility.encodeImageToHTML() — the\nhelper that visualization operators splice into generated UDF code to\nbase64-encode binary image data into an <img> tag. Any drift in the\nf-string template or the error fallback would silently break\nimage-rendering operators (e.g. Word Cloud). No production-code changes.\nNegative cases are covered implicitly: removing any pinned substring\nfails the corresponding assertion.\n\n| Spec | Source Class | Tests |\n| --- | --- | --- |\n| ImageUtilitySpec | ImageUtility | 8 |\n\nSpec file name follows the `<srcClassName>Spec.scala` one-to-one\nconvention.\n\n**Behavior pinned**\n\n| Surface | Contract |\n| :--- | :--- |\n| `encodeImageToHTML` | returns a non-empty `String` |\n| Snippet imports base64 | substring `import base64` appears |\n| Snippet calls `base64.b64encode(binary_image_data)` | substring\nappears verbatim |\n| Snippet decodes to UTF-8 | substring `.decode(\"utf-8\")` appears |\n| Snippet emits an `<img>` tag with `data:image;base64,...` | substring\n`data:image;base64,{encoded_image_str}` appears (verifies the f-string\ntemplate) |\n| Snippet handles binary-decode failure via try/except | substrings\n`except Exception` and `Binary input is not valid` both appear |\n| Snippet contains a top-level `html =` assignment | substring `html =\nf` appears so downstream code can reference `html` |\n| Determinism | two calls return the exact same string |\n\nThe harness pins via `contains` on canonical substrings — resilient to\nincidental whitespace changes from `stripMargin` while still catching\nreal template drift.\n### Any related issues, documentation, discussions?\n<!--\nPlease use this section to link other resources if not mentioned\nalready.\n1. If this PR fixes an issue, please include `Fixes #1234`, `Resolves\n#1234`\nor `Closes #1234`. If it is only related, simply mention the issue\nnumber.\n  2. If there is design documentation, please add the link.\n  3. If there is a discussion in the mailing list, please add the link.\n-->\nCloses #5665 \n\n### How was this PR tested?\n<!--\nIf tests were added, say they were added here. Or simply mention that if\nthe PR\nis tested with existing test cases. Make sure to include/update test\ncases that\ncheck the changes thoroughly including negative and positive cases if\npossible.\nIf it was tested in a way different from regular unit tests, please\nclarify how\nyou tested step by step, ideally copy and paste-able, so that other\nreviewers can\ntest and check, and descendants can verify in the future. If tests were\nnot added,\nplease describe why they were not added and/or why it was difficult to\nadd.\n-->\nUnit tests were added here, tested through local verification:\n\n- ```sbt \"WorkflowOperator/testOnly\norg.apache.texera.amber.operator.visualization.ImageUtilitySpec\"``` -\nAll 8 tests passed\n- ```sbt scalafmtCheckAll``` -- clean\n- ```sbt scalafixAll --check``` No lint errors\n\n### Was this PR authored or co-authored using generative AI tooling?\n<!--\nIf generative AI tooling has been used in the process of authoring this\nPR,\nplease include the phrase: 'Generated-by: ' followed by the name of the\ntool\nand its version. If no, write 'No'. \nPlease refer to the [ASF Generative Tooling\nGuidance](https://www.apache.org/legal/generative-tooling.html) for\ndetails.\n-->\nGenerated-by: Composer 2.5 Fast",
          "timestamp": "2026-06-13T18:00:43Z",
          "tree_id": "13f359fb502e155ab86032e7378f5d3f1ef762de",
          "url": "https://github.com/apache/texera/commit/6becb8596df32058f69473c39f4a9028f149954e"
        },
        "date": 1781374633590,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 18427.068,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 27720.763,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 27720.763,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 81828.97,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 108536.608,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 108536.608,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 694850.073,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 733876.887,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 733876.887,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "xinyual3@uci.edu",
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": false,
          "id": "7ae2374bead9361735f5538b6501337d3ea32c56",
          "message": "test(config): add unit test coverage for ConfigParserUtil (#5659)\n\n### What changes were proposed in this PR?\n\nPin behavior of `ConfigParserUtil.parseSizeStringToBytes` — the\nsize-string parser used by `StorageConfig` for S3 multipart sizing. The\n`common/config` module had no test infrastructure before this PR (no\n`src/test` directory existed); this PR adds the directory and configures\nthe standard ScalaTest dependency the way the other backend modules do.\n\n| Spec | Source class | Tests |\n| --- | --- | --- |\n| `ConfigParserUtilSpec` | `ConfigParserUtil` | 16 |\n\nSpec file name follows the `<srcClassName>Spec.scala` one-to-one\nconvention.\n\n**Behavior pinned**\n\n| Surface | Contract |\n| --- | --- |\n| `1KB` / `1MB` / `1GB` | parses to `1024L` / `1024 * 1024L` / `1024 *\n1024 * 1024L` |\n| Multi-digit values (`100MB`, `1024KB`, `128GB`) | scales correctly by\nthe unit multiplier |\n| `5GB` | preserves `Long` precision (result exceeds `Int.MaxValue`) |\n| `0010KB` | parses to `10 * 1024L` (decimal-only, no octal\ninterpretation) |\n| Missing unit (`100`) | throws `IllegalArgumentException` with\ndiagnostic |\n| Unsupported unit (`5TB`) | throws `IllegalArgumentException` |\n| Empty string | throws `IllegalArgumentException` |\n| Lowercase unit (`5mb`) | throws `IllegalArgumentException` (regex is\nanchored to `[KMG]B`) |\n| Embedded whitespace (`5 MB`) | throws `IllegalArgumentException` |\n| Non-numeric value (`abcMB`) | throws `IllegalArgumentException` |\n| Unit-only input (`MB`) | throws `IllegalArgumentException` |\n| Return type | `Long` (compile-time enforced) |\n\n**Build-config change**\n\nAdds `org.scalatest %% scalatest % 3.2.15 % Test` to\n`common/config/build.sbt`. The version matches the other backend modules\n(`common/workflow-operator`, `common/dao`, `amber`). Scope is `Test` so\nthe dependency does not leak into the production classpath.\n\n### Any related issues, documentation, discussions?\n\nCloses #5655.\n\n### How was this PR tested?\n\nPure unit-test addition (plus the build-config tweak above); verified\nlocally with:\n\n- `sbt \"Config/testOnly\norg.apache.texera.amber.util.ConfigParserUtilSpec\"` — 16 tests, all\ngreen\n- `sbt scalafmtCheckAll` — clean\n- CI to confirm\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.7 [1M context])",
          "timestamp": "2026-06-13T18:52:29Z",
          "tree_id": "2e5a358109f2310bebc03bf7975d3e3f51f1d54a",
          "url": "https://github.com/apache/texera/commit/7ae2374bead9361735f5538b6501337d3ea32c56"
        },
        "date": 1781377500046,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 26650.245,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 37431.85,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 37431.85,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 120028.73,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 150572.282,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 150572.282,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1088483.852,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1128733.869,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1128733.869,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "125538144+benjaminle22@users.noreply.github.com",
            "name": "Benjamin Le",
            "username": "benjaminle22"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "aceca29cbda1f223ac17753efb58eba726e58b2f",
          "message": "test(frontend): expand OperatorReuseCacheStatusService spec coverage (#5624)\n\n### What changes were proposed in this PR?\nExpands the existing spec for `OperatorReuseCacheStatusService`, which\npreviously only checked `should be created`. Adds behavior-focused tests\ncovering both constructor subscriptions: the `CacheStatusUpdateEvent`\nwebsocket handler and the `getReuseCacheOperatorsChangedStream` handler.\nTests verify that `JointUIService.changeOperatorReuseCacheStatus` is\ncalled correctly for each operator, that the `mainJointPaper` null guard\nis respected in both handlers, and that empty operator lists do not\nthrow.\n\n### Any related issues, documentation, discussions?\nCloses #5543\n\n### How was this PR tested?\nNew tests run via `yarn test -- operator-reuse-cache-status` and `yarn\nlint`. 6 tests passing.\n\n### Was this PR authored or co-authored using generative AI tooling?\nGenerated-by: Claude (Claude Sonnet 4.6)\n\nCo-authored-by: Benjamin Le <benjaminl@uci.edu>",
          "timestamp": "2026-06-13T18:54:04Z",
          "tree_id": "a6bb83aa00c102b61bfa1c9af3c242f86a6c33da",
          "url": "https://github.com/apache/texera/commit/aceca29cbda1f223ac17753efb58eba726e58b2f"
        },
        "date": 1781377774334,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 26423.355,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 36566.399,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 36566.399,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 126866.448,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 145386.579,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 145386.579,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1085677.606,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1152478.059,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1152478.059,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "49699333+dependabot[bot]@users.noreply.github.com",
            "name": "dependabot[bot]",
            "username": "dependabot[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4fd395b8f9875d82d8aaf3cc4884ab2366381ae0",
          "message": "chore(deps): bump shell-quote from 1.8.3 to 1.8.4 in /frontend (#5684)\n\nBumps [shell-quote](https://github.com/ljharb/shell-quote) from 1.8.3 to\n1.8.4.\n<details>\n<summary>Changelog</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/ljharb/shell-quote/blob/main/CHANGELOG.md\">shell-quote's\nchangelog</a>.</em></p>\n<blockquote>\n<h2><a\nhref=\"https://github.com/ljharb/shell-quote/compare/v1.8.3...v1.8.4\">v1.8.4</a>\n- 2026-05-22</h2>\n<h3>Commits</h3>\n<ul>\n<li>[Fix] <code>quote</code>: validate object-token shapes <a\nhref=\"https://github.com/ljharb/shell-quote/commit/4378a6e613db5948168684864e49b42b83134d2d\"><code>4378a6e</code></a></li>\n<li>[Dev Deps] update <code>@ljharb/eslint-config</code>,\n<code>auto-changelog</code>, <code>eslint</code>, <code>npmignore</code>\n<a\nhref=\"https://github.com/ljharb/shell-quote/commit/22ebec04349065a45ad8afc8cc8d53c4624634a6\"><code>22ebec0</code></a></li>\n<li>[Tests] increase coverage <a\nhref=\"https://github.com/ljharb/shell-quote/commit/9f3caa31900cc6ee64858b31134144c648ce206d\"><code>9f3caa3</code></a></li>\n<li>[readme] replace runkit CI badge with shields.io check-runs badge <a\nhref=\"https://github.com/ljharb/shell-quote/commit/3344a047dd1e95f71c4ca27522cbfd05c56277e0\"><code>3344a04</code></a></li>\n<li>[Dev Deps] update <code>@ljharb/eslint-config</code> <a\nhref=\"https://github.com/ljharb/shell-quote/commit/699c5113d135f4d4591574bebf173334ffa453d4\"><code>699c511</code></a></li>\n</ul>\n</blockquote>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a\nhref=\"https://github.com/ljharb/shell-quote/commit/ff166e2b63eb5f932bd131a8886a99e9afdf45ae\"><code>ff166e2</code></a>\nv1.8.4</li>\n<li><a\nhref=\"https://github.com/ljharb/shell-quote/commit/4378a6e613db5948168684864e49b42b83134d2d\"><code>4378a6e</code></a>\n[Fix] <code>quote</code>: validate object-token shapes</li>\n<li><a\nhref=\"https://github.com/ljharb/shell-quote/commit/22ebec04349065a45ad8afc8cc8d53c4624634a6\"><code>22ebec0</code></a>\n[Dev Deps] update <code>@ljharb/eslint-config</code>,\n<code>auto-changelog</code>, <code>eslint</code>, `npmig...</li>\n<li><a\nhref=\"https://github.com/ljharb/shell-quote/commit/9f3caa31900cc6ee64858b31134144c648ce206d\"><code>9f3caa3</code></a>\n[Tests] increase coverage</li>\n<li><a\nhref=\"https://github.com/ljharb/shell-quote/commit/3344a047dd1e95f71c4ca27522cbfd05c56277e0\"><code>3344a04</code></a>\n[readme] replace runkit CI badge with shields.io check-runs badge</li>\n<li><a\nhref=\"https://github.com/ljharb/shell-quote/commit/699c5113d135f4d4591574bebf173334ffa453d4\"><code>699c511</code></a>\n[Dev Deps] update <code>@ljharb/eslint-config</code></li>\n<li>See full diff in <a\nhref=\"https://github.com/ljharb/shell-quote/compare/v1.8.3...v1.8.4\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility\nscore](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=shell-quote&package-manager=npm_and_yarn&previous-version=1.8.3&new-version=1.8.4)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't\nalter it yourself. You can also trigger a rebase manually by commenting\n`@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits\nthat have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all\nof the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop\nDependabot creating any more for this major version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop\nDependabot creating any more for this minor version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop\nDependabot creating any more for this dependency (unless you reopen the\nPR or upgrade to it yourself)\nYou can disable automated security fix PRs for this repo from the\n[Security Alerts page](https://github.com/apache/texera/network/alerts).\n\n</details>\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>",
          "timestamp": "2026-06-13T20:59:04Z",
          "tree_id": "58c4b12a2ccef3dd30abd0a0da746c79facf6ca1",
          "url": "https://github.com/apache/texera/commit/4fd395b8f9875d82d8aaf3cc4884ab2366381ae0"
        },
        "date": 1781385343559,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 24155.835,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 30242.894,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 30242.894,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 103271.159,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 144101.88,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 144101.88,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 892725.179,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 943636.18,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 943636.18,
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
          "id": "a0442876321ede19fe7f33e57c1f7b277c09e3ba",
          "message": "chore(frontend): refresh UDF operator icons (#5686)\n\n### What changes were proposed in this PR?\n\nReplaces the eight UDF operator icons under\n`frontend/src/assets/operator_images/` with a clean, consistent\ntypographic set. The previous icons were mismatched — the three Python\nUDF icons carried a leftover red \"New\" overlay, and the\nLambda/Reducer/Java/R icons varied in size and visual style (logo art,\nplain text, stock marks).\n\nEach new icon is a 1024×1024 transparent PNG with a large,\ntightly-cropped hero glyph and, where needed, a small descriptor stacked\nbeneath it: `Py` (Python UDF), `Py` + `src` (Python UDF source), `Py` +\n`2-in` (dual-input Python UDF), `λ` + `py` (Python lambda), `Σ` + `py`\n(Python table reducer), `Java` (Java UDF), `R` (R UDF), `R` + `src` (R\nUDF source). Every Python wordmark — both the hero `Py` and the `py`\ndescriptor — uses the same Python blue/gold two-color scheme; the\n`src`/`2-in` qualifiers are gray. Java uses its orange/blue, R its blue.\nFilenames are unchanged, so they continue to map to the\n`assets/operator_images/<operatorType>.png` lookup in\n`joint-ui.service.ts`.\n\nBefore / after comparison of all eight icons:\n\n![Before and after comparison of the eight UDF operator\nicons](https://raw.githubusercontent.com/Yicong-Huang/texera/media/udf-icons-preview/udf-icons-preview/_before_after.png)\n\n### Any related issues, documentation, discussions?\n\nCloses #5685\n\n### How was this PR tested?\n\nThis is a static asset swap with no code changes, so no automated tests\nwere added. Verified manually by running the Angular frontend (`ng\nserve`) against a local backend and confirming all eight icons render\ncorrectly in the operator panel and on the workflow canvas. Each PNG is\na 1024×1024 transparent square and the eight filenames exactly match the\nexisting files, so the operator-type icon lookup is unaffected.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Opus 4.8 (Cowork)\n\nCo-authored-by: Yicong Huang <yiconghuang@cs.umass.edu>",
          "timestamp": "2026-06-13T22:03:39Z",
          "tree_id": "d6b17b997a64c4c258a8e93652ed019f2f77f126",
          "url": "https://github.com/apache/texera/commit/a0442876321ede19fe7f33e57c1f7b277c09e3ba"
        },
        "date": 1781389202070,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 24518.596,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 34934.639,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 34934.639,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 120315.388,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 145016.611,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 145016.611,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1052107.943,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1094010.381,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1094010.381,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "eugenegujing@outlook.com",
            "name": "Eugene Gu",
            "username": "eugenegujing"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "bdde6b946d79fbe668d211b77e663e2ed263e28f",
          "message": "feat(workflow-operator): add not-blank validation messages (#5640)\n\n<!-- PR title: feat(workflow-operator): add not-blank validation\nmessages -->\n\n### What changes were proposed in this PR?\n\nExtends the not-blank validation pattern introduced for basic chart\noperators in #4006 to **all visualization operators**, and gives every\nbackend `assert` a human-readable message.\n\n**Why this matters.** The frontend (AJV + `required` + the autofill\nattribute enum) already blocks most empty required fields in normal UI\nusage, but three gaps remained where users or API callers hit a bare\n`assertion failed` with no explanation:\n\n| Gap | Example | Fix |\n|---|---|---|\n| Bare asserts are the only backend check for execution paths that\nbypass frontend validation (direct API calls, agent-generated workflows)\n| `assert(value.nonEmpty)` in `PieChartOpDesc` | Every assert now\ncarries a message, e.g. `assert(value.nonEmpty, \"Value Column cannot be\nempty\")` |\n| Empty lists pass AJV's `required` check (it only verifies key\npresence) | `FigureFactoryTableOpDesc.columns = []` ran and crashed |\n`@NotEmpty(message = ...)` on required list attributes → schema gains\n`minItems: 1` |\n| Numeric constraints existed only in backend asserts, invisible to the\nfrontend | `rowHeight = 10` passed the form, then `assert(rowHeight >=\n30)` crashed | `@DecimalMin` on `FigureFactoryTable`\n`fontSize`/`rowHeight` → schema gains `minimum` |\n\n**Changes in detail:**\n\n- Added messages to all 45 previously bare asserts across 23\nvisualization operators, splitting compound asserts (e.g.\n`assert(x.nonEmpty && y.nonEmpty)`) per field so the error names the\nexact missing attribute.\n- Added `@NotNull(message = ...)` to required string attributes lacking\nit; with the generator's `useMinLengthForNotNull`, the schema gains\n`minLength: 1` so the frontend rejects empty strings as well. Annotation\nmessages are identical to their assert messages (same convention as\n#4006). An annotation-only sweep over 20 more operator/config files\nbrings every required visualization attribute under a constraint.\n- Fixed two null defaults (`ImageVisualizerOpDesc.binaryContent`,\n`ScatterMatrixChartOpDesc.selectedAttributes`) that threw a\n`NullPointerException` before the assert could produce its message.\n- Added two missing guards for fields that were interpolated unchecked:\n`IcicleChartOpDesc.manipulateTable()` (`value`) and\n`RadarChartOpDesc.createPlotlyFigure()` (`valueColumns`).\n- No optional field gained a new required-ness constraint, so existing\nsaved workflows are unaffected.\n\n**Notes for reviewers:**\n\n- Messages quote each field's `@JsonSchemaTitle` verbatim; a few titles\nare terse (`x`, `r`, `theta` in contourPlot/quiverPlot/polarChart),\nproducing messages like `x cannot be empty`. Improving those titles is\nleft out of scope — happy to adjust if preferred.\n- `LineChartOpDesc.lines` reuses its pre-existing assert message (`At\nleast one line must be configured`) instead of inventing a second\nphrasing.\n- Non-visualization operators also have required attributes without\nconstraint annotations (~55 files); those fields need per-field semantic\nreview, so they are left for a follow-up issue.\n\n### Any related issues, documentation, discussions?\n\nCloses #4053. Follows the approach of #4006 (and #3692).\n\n### How was this PR tested?\n\nAdded 14 new spec files and extended 9 existing ones, covering every\ntouched operator with positive (all fields set → template renders the\nconfigured columns), negative (empty field → `AssertionError` whose\nmessage names the field and contains \"cannot be empty\"), and boundary\ncases (`rowHeight = 10` → \"at least 30\", `fontSize = -1` →\n\"non-negative\", exact boundary values pass).\n\n```\nsbt \"WorkflowOperator/testOnly org.apache.texera.amber.operator.visualization.*\"\n```\n\n168 tests, 31 suites, all passed. Also ran `sbt scalafixAll` and `sbt\nscalafmtAll` (clean).\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nCo-authored-by: Claude Code (Claude Fable 5)",
          "timestamp": "2026-06-13T23:36:46Z",
          "tree_id": "a2d36008c4931c4b2cd36a093654aa09320810a5",
          "url": "https://github.com/apache/texera/commit/bdde6b946d79fbe668d211b77e663e2ed263e28f"
        },
        "date": 1781394719624,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 23391.386,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 29322.897,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 29322.897,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 123999.934,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 145810.861,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 145810.861,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1073040.496,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1136638.618,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1136638.618,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "1c580e59eb69bc45205298606c3980c67a05803f",
          "message": "fix(execution-service): surface init-time fatal errors to the websocket (#5781)\n\n### What changes were proposed in this PR?\n\nWhen workflow execution initialization fails, the error was recorded\ninto the execution metadata store but never pushed to the websocket, so\nconnected frontend clients saw nothing — particularly for failures\nduring `WorkflowExecutionService` construction, which happens *before*\nthe execution is published to subscribers.\n\n`WorkflowService.initExecutionService`'s catch arm now, after\n`errorHandler(e)` records the fatal error, pushes a `WorkflowErrorEvent`\n(carrying the recorded fatal errors) to `errorSubject` — the\nworkflow-level channel that `connect()` subscribers listen on — so\ninit-time failures surface in the UI.\n\n| init failure | before | after |\n|---|---|---|\n| during `WorkflowExecutionService` construction (pre-publish) | logged\n+ stored, invisible to the UI | `WorkflowErrorEvent` delivered to the\nfrontend |\n| during `executeWorkflow()` | recorded; UI delivery depended on\nsubscription timing | `WorkflowErrorEvent` delivered to the frontend |\n\nThe push is extracted into a small `reportFatalErrorsToSubscribers`\nmethod so it can be unit-tested without a database (the init path itself\nis DB-bound).\n\n### Any related issues, documentation, discussions?\n\nResolves #5782. Discovered while splitting #5700 (loop operators) into\nsmaller PRs; this fix is independent of that feature and applies to\n`main` on its own.\n\n### How was this PR tested?\n\nNew `WorkflowServiceSpec` (TDD, red → green): pins that\n`reportFatalErrorsToSubscribers` delivers a `WorkflowErrorEvent` to a\n`connect()` subscriber carrying exactly the fatal errors recorded in the\nexecution state store (single error, and all errors when several are\npresent). `sbt \"WorkflowExecutionService/testOnly *WorkflowServiceSpec\"`\npasses (2/2); scalafmt + scalafix clean.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nCo-authored with Claude Opus 4.8 in compliance with ASF.",
          "timestamp": "2026-06-24T02:45:12Z",
          "url": "https://github.com/apache/texera/commit/1c580e59eb69bc45205298606c3980c67a05803f"
        },
        "date": 1782308017539,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 14794.777,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 19822.715,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 25644.441,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 77068.658,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 85851.514,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 94712.603,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 696478.645,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 734189.815,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 785541.695,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 11786.065,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 13476.038,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 17148.236,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 72597.332,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 79701.662,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 89026.193,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 695868.176,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 739525.356,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 764680.035,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 11043.996,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 14915.911,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 16277.087,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 72820.452,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 79253.945,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 88643.753,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 706633.98,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 752221.912,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 806491.784,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 13425.336,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 17815.584,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 19253.975,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 93088.091,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 101872.288,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 115444.063,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 884642.829,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 932707.828,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 965676.788,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 12124.959,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 14958.576,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 18502.927,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 92709.422,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 100226.788,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 109877.114,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 899196.751,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 945162.077,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 986231.35,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 12501.645,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 14604.518,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 17642.639,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 95864.83,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 103032.187,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 113148.895,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 903988.987,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 956949.976,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1019518.628,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 20471.888,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 22053.311,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 34144.039,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 167882.884,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 178528.918,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 188524.161,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1679033.73,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1743099.965,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1770511.47,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 21043.503,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 21887.763,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 25947.64,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 172050.288,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 180048.365,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 187957.555,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1700171.765,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1760438.767,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1816359.664,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 21467.688,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 23897.142,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 27496.28,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 177869.017,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 188793.535,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 207547.165,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1769169.734,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 1820510.423,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 1850503.036,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "abbe7a30244d6e50bc5ce7a89befcbc7a9d31c28",
          "message": "test(workflow-operator): add unit test coverage for Sklearn tree-based classifier descriptors (#5939)\n\n### What changes were proposed in this PR?\n\nPin behavior of four previously-untested Sklearn tree-based classifier\ndescriptors in `common/workflow-operator`. No production-code changes.\n\n| Spec | Source class | Tests |\n| --- | --- | --- |\n| `SklearnDecisionTreeOpDescSpec` | `SklearnDecisionTreeOpDesc` | 5 |\n| `SklearnExtraTreeOpDescSpec` | `SklearnExtraTreeOpDesc` | 5 |\n| `SklearnExtraTreesOpDescSpec` | `SklearnExtraTreesOpDesc` | 5 |\n| `SklearnRandomForestOpDescSpec` | `SklearnRandomForestOpDesc` | 5 |\n\n**Behavior pinned**\n\n| Surface | Contract |\n| --- | --- |\n| `operatorInfo` | exact model name + `Sklearn <name> Operator`\ndescription; Sklearn group; training/testing input ports + one blocking\noutput |\n| field defaults | `countVectorizer`/`tfidfTransformer` `false`;\n`target`/`text` `null` |\n| `getOutputSchemas` | `model_name` (STRING) + `model` (BINARY) keyed by\nthe declared output port |\n| `generatePythonCode` | imports the matching sklearn estimator and\nbuilds the `make_pipeline` model |\n| Round-trip | config fields preserved through the polymorphic\n`LogicalOp` base, with the correct `operatorType` discriminator |\n\n### Any related issues, documentation, discussions?\n\nPart of the ongoing `workflow-operator` unit-test coverage effort\n(follow-up to the Sklearn Naive Bayes coverage in #5925).\n\n### How was this PR tested?\n\n- `sbt \"WorkflowOperator/testOnly *SklearnDecisionTreeOpDescSpec\n*SklearnExtraTreeOpDescSpec *SklearnExtraTreesOpDescSpec\n*SklearnRandomForestOpDescSpec\"` — 20 tests, all green\n- `sbt \"WorkflowOperator/Test/scalafmtCheck\"` and `sbt\n\"WorkflowOperator/scalafixAll --check\"` — clean\n- CI to confirm\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-06-25T07:19:56Z",
          "url": "https://github.com/apache/texera/commit/abbe7a30244d6e50bc5ce7a89befcbc7a9d31c28"
        },
        "date": 1782394910507,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 14319.239,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 17670.798,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 21738.346,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 86211.465,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 95556.931,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 112481.601,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 836383.488,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 874065.436,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 884759.902,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 11311.583,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 13156.046,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 18408.946,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 86647.07,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 92190.325,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 95944.942,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 837430.393,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 865767.222,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 880170.572,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 11024.343,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 13947.908,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 17213.896,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 87367.505,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 92617.537,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 97269.423,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 825630.751,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 867786.299,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 878883.095,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 13406.532,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 16766.788,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 18699.192,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 105337.717,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 115742.097,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 125157.552,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 1047063.261,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 1081506.045,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 1092193.218,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 12958.395,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 14813.977,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 16657.972,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 106663.851,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 112315.74,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 121013.879,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1042153.586,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1078694.783,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1103858.828,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 12744.563,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 14494.628,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 18067.277,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 107280.982,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 113473.756,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 120291.796,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 1055414.921,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 1096235.201,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1114249.17,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 21828.787,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 24210.454,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 28095.251,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 188332.26,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 198827.316,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 224384.957,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1852456.592,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1895322.994,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1933200.448,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 21680.968,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 25242.823,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 37140.363,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 189269.256,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 194544.936,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 212266.1,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1858144.901,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1895198.653,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1916935.262,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 22442.775,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 26712.96,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 32851.386,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 199965.785,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 204521.529,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 219816.932,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1943846.462,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 1977876.124,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 2033012.523,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "7a38b6cf4c476e6f7ae0b0555fd711b50f4e85ec",
          "message": "test(workflow-operator): add unit test coverage for Sklearn Naive Bayes descriptors (#5925)\n\n### What changes were proposed in this PR?\n\nPin behavior of the four previously-untested Sklearn Naive Bayes\nclassifier descriptors in `common/workflow-operator`. No production-code\nchanges.\n\n| Spec | Source class | Tests |\n| --- | --- | --- |\n| `SklearnBernoulliNaiveBayesOpDescSpec` |\n`SklearnBernoulliNaiveBayesOpDesc` | 5 |\n| `SklearnComplementNaiveBayesOpDescSpec` |\n`SklearnComplementNaiveBayesOpDesc` | 5 |\n| `SklearnGaussianNaiveBayesOpDescSpec` |\n`SklearnGaussianNaiveBayesOpDesc` | 5 |\n| `SklearnMultinomialNaiveBayesOpDescSpec` |\n`SklearnMultinomialNaiveBayesOpDesc` | 5 |\n\n**Behavior pinned**\n\n| Surface | Contract |\n| --- | --- |\n| `operatorInfo` | exact model name + `Sklearn <name> Operator`\ndescription; Sklearn group; training/testing input ports + one blocking\noutput |\n| field defaults | `countVectorizer`/`tfidfTransformer` `false`;\n`target`/`text` `null` |\n| `getOutputSchemas` | `model_name` (STRING) + `model` (BINARY) keyed by\nthe declared output port |\n| `generatePythonCode` | imports and instantiates the matching sklearn\nestimator (e.g. `BernoulliNB`) via `make_pipeline` |\n| Round-trip | config fields preserved through the polymorphic\n`LogicalOp` base, with the correct `operatorType` discriminator |\n\n### Any related issues, documentation, discussions?\n\nPart of the ongoing `workflow-operator` unit-test coverage effort.\n\n### How was this PR tested?\n\n- `sbt \"WorkflowOperator/testOnly *SklearnBernoulliNaiveBayesOpDescSpec\n*SklearnComplementNaiveBayesOpDescSpec\n*SklearnGaussianNaiveBayesOpDescSpec\n*SklearnMultinomialNaiveBayesOpDescSpec\"` — 20 tests, all green\n- `sbt \"WorkflowOperator/Test/scalafmtCheck\"` and `sbt\n\"WorkflowOperator/scalafixAll --check\"` — clean\n- CI to confirm\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-06-26T08:35:47Z",
          "url": "https://github.com/apache/texera/commit/7a38b6cf4c476e6f7ae0b0555fd711b50f4e85ec"
        },
        "date": 1782481152077,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 16421.706,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 22465.822,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 26691.163,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 93460.841,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 103596.809,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 112816.552,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 880083.927,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 909594.645,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 927252.627,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 12381.748,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 14601.296,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 17477.389,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 88670.292,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 96131.465,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 103439.418,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 863821.642,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 899191.865,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 919331.081,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 12377.385,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 16784.414,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 20082.706,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 89725.518,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 95923.394,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 106526.735,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 870462.044,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 909583.921,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 930924.336,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 13888.784,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 17708.74,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 22110.986,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 110446.215,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 117638.612,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 126047.761,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 1098218.873,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 1141180.829,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 1166141.546,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 13811.663,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 16960.503,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 19972.98,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 108929.159,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 116343.967,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 123496.171,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1086567.232,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1129405.641,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1157514.789,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 14597.3,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 17340.89,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 19961.216,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 112782.541,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 121274.931,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 132210.966,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 1095014.283,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 1140250.024,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1179104.959,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 21756.228,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 28673.443,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 32507.206,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 191123.23,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 200079.241,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 215459.261,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1903613.313,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1949900.16,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 2011754.961,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 23040.068,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 23922.015,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 28592.385,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 194298.049,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 202825.73,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 222656.189,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1908777.672,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1948166.343,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1987382.215,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 23720.787,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 28937.221,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 41388.131,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 201114.809,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 210526.012,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 241242.808,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 2014030.125,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 2067571.784,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 2087671.933,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "7944c0979b0d79a44ae3d4641deb80a87a426682",
          "message": "test(workflow-operator): add unit test coverage for Sklearn training SVM, neighbor & misc descriptors (#5956)\n\n### What changes were proposed in this PR?\n\nPin behavior of nine previously-untested Sklearn **training** operator\ndescriptors in `common/workflow-operator`. No production-code changes.\n\n| Spec | Source class |\n| --- | --- |\n| `SklearnTrainingSVMOpDescSpec` | `SklearnTrainingSVMOpDesc` |\n| `SklearnTrainingLinearSVMOpDescSpec` |\n`SklearnTrainingLinearSVMOpDesc` |\n| `SklearnTrainingKNNOpDescSpec` | `SklearnTrainingKNNOpDesc` |\n| `SklearnTrainingNearestCentroidOpDescSpec` |\n`SklearnTrainingNearestCentroidOpDesc` |\n| `SklearnTrainingRidgeOpDescSpec` | `SklearnTrainingRidgeOpDesc` |\n| `SklearnTrainingRidgeCVOpDescSpec` | `SklearnTrainingRidgeCVOpDesc` |\n| `SklearnTrainingDummyClassifierOpDescSpec` |\n`SklearnTrainingDummyClassifierOpDesc` |\n| `SklearnTrainingMultiLayerPerceptronOpDescSpec` |\n`SklearnTrainingMultiLayerPerceptronOpDesc` |\n| `SklearnTrainingProbabilityCalibrationOpDescSpec` |\n`SklearnTrainingProbabilityCalibrationOpDesc` |\n\n**Behavior pinned** (shared `SklearnTrainingOpDesc` contract)\n\n| Surface | Contract |\n| --- | --- |\n| `operatorInfo` | exact model name (`Training: <model>`) + `Sklearn\n<name> Operator` description; **Sklearn Training** group; single\n`training` input port; one blocking output |\n| field defaults | `countVectorizer`/`tfidfTransformer` `false`;\n`target`/`text` `null` |\n| `getOutputSchemas` | `model_name` (STRING) + `model` (BINARY) keyed by\nthe declared output port |\n| `generatePythonCode` | imports the matching sklearn estimator and\nbuilds the `make_pipeline(...).fit(X, Y)` training model |\n| Round-trip | config fields preserved through the polymorphic\n`LogicalOp` base, with the correct `operatorType` discriminator |\n\n### Any related issues, documentation, discussions?\n\nPart of the ongoing `workflow-operator` unit-test coverage effort (the\ntraining-side counterpart to the Sklearn classifier coverage in\n#5925/#5939/#5940/#5941/#5945/#5946/#5951).\n\n### How was this PR tested?\n\n- `sbt \"WorkflowOperator/testOnly\norg.apache.texera.amber.operator.sklearn.training.*\"` — 45 tests, all\ngreen\n- `sbt \"WorkflowOperator/Test/scalafmtCheck\"` and `sbt\n\"WorkflowOperator/scalafixAll --check\"` — clean\n- CI to confirm\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-06-27T00:24:53Z",
          "url": "https://github.com/apache/texera/commit/7944c0979b0d79a44ae3d4641deb80a87a426682"
        },
        "date": 1782566451129,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 14637.467,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 20315.413,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 25470.081,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 88765.697,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 97228.742,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 106455.722,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 825730.161,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 862168.614,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 876822.181,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 11679.882,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 13373.054,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 16287.457,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 85579.08,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 91567.331,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 99258.102,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 821893.911,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 863873.607,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 884371.059,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 11183.344,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 13189.383,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 16541.867,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 84274.717,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 91279.295,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 94270.438,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 837100.631,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 868538.192,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 915514.385,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 13682.627,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 17548.155,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 20281.536,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 104496.317,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 112392.648,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 126589.348,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 1035096.998,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 1083417.393,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 1115667.325,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 13139.943,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 15117.787,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 19111.766,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 104978.46,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 111346.759,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 119783.619,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1034477.682,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1070514.334,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1099731.428,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 12931.864,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 14417.901,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 17427.517,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 106709.888,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 114558.2,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 125075.923,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 1048067.404,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 1089225.191,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1108001.51,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 21807.238,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 22907.464,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 32729.578,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 185545.107,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 193070.484,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 229106.549,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1847053.964,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1892867.905,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1956275.091,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 21904.955,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 22739.113,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 26511.82,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 186806.509,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 193511.273,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 225955.489,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1838872.929,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1887525.289,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1909763.633,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 22065.426,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 25787.095,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 29556.649,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 192652.838,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 202533.543,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 232744.033,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1917426.14,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 1969618.917,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 2016603.209,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Jiadong Bai",
            "username": "bobbai00",
            "email": "43344272+bobbai00@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "a24d1d1701dfffc46279ecda1db34268e2dca44c",
          "message": "refactor(agent-service): extract WebSocket message types into types/ws (#5751)\n\n### What changes were proposed in this PR?\n\nThis PR refactors the agent-service ↔ frontend WebSocket messaging into\na dedicated **`types/ws`** folder. Each frame is a small class whose\n`type` discriminator equals its class name (matching the main Texera WS\nconvention, e.g. `RegionUpdateEvent`). Building a frame with `new\nWsServerStatusEvent(...)` sets the wire `type` for you, so no `type:\n\"...\"` literal is hand-written at call sites; receivers `switch` on\n`event.type`.\n\n- `types/ws/client.ts` — frames the frontend sends, unioned as\n`WsClientCommand`:\n  - `WsClientPromptCommand` — a user prompt for the agent to run.\n  - `WsClientStopCommand` — stop the in-flight run (payload-less).\n- `types/ws/server.ts` — frames agent-service sends back, unioned as\n`WsServerEvent`:\n- `WsServerSnapshotEvent` — full state, sent once when a client\nconnects.\n  - `WsServerStepEvent` — one step, streamed live as the agent runs.\n- `WsServerStatusEvent` — a lifecycle change (GENERATING / AVAILABLE /\nSTOPPING).\n  - `WsServerErrorEvent` — an error to surface to the user.\n- `WsServerHeadChangeEvent` — unused by the frontend/UI (still reachable\nvia `/agents/:id/checkout`); slated for removal in #5930.\n- Operator result summaries are no longer pushed over WebSocket — they\nare pulled on demand via `GET /agents/:id/operator-results`. The\ncorresponding REST DTO `OperatorResultSummary` lives in\n`types/execution.ts` (not under `ws/`).\n\nThis PR also adds unit tests and renames the `agent-service` test files\nfrom `*.test.ts` to `*.spec.ts` to align with the frontend's naming\nconvention.\n\n### Any related issues, documentation, discussions?\n\nCloses #5749\n\n### How was this PR tested?\n\nAdded/updated unit tests in both `agent-service` and `frontend` all\npass; a local end-to-end run was also verified.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Opus 4.8 (1M context)\n\n---------\n\nCo-authored-by: Claude Opus 4.8 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-06-28T09:46:50Z",
          "url": "https://github.com/apache/texera/commit/a24d1d1701dfffc46279ecda1db34268e2dca44c"
        },
        "date": 1782652966099,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 14951.883,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 23157.339,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 26445.617,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 88540.694,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 97173.089,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 108251.686,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 849353.993,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 885129.355,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 905125.448,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 11498.358,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 13927.553,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 14813.713,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 87701.952,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 94422.387,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 100478.078,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 844318.26,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 879304.266,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 895708.521,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 11249.431,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 14909.229,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 17278.481,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 87747.645,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 93005.135,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 96296.944,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 851955.107,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 890088.248,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 904196.37,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 13382.292,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 16591.284,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 20138.87,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 109035.299,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 114699.565,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 126307.697,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 1054258.884,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 1096296.845,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 1107552.683,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 12821.671,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 14815.869,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 19415.899,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 106628.908,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 112680.059,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 117673.47,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1060368.731,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1095169.108,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1133859.598,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 12974.138,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 15883.48,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 18852.273,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 108259.026,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 115745.747,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 124059.465,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 1076323.411,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 1121389.362,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1138806.717,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 21274.141,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 22466.626,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 28413.627,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 188188.31,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 199896.688,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 234118.764,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1853933.233,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1904792.734,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1935162.906,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 21399.569,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 22433.713,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 27649.389,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 188139.629,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 199796.211,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 234073.417,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1860215.265,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1909681.328,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1928698.265,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 23157.736,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 26168.538,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 31330.142,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 197609.588,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 206872.188,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 232529.113,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1965556.955,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 2005349.654,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 2062845.422,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Yicong Huang",
            "username": "Yicong-Huang",
            "email": "17627829+Yicong-Huang@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "613b76e467713efffdeef353b4c831e0f7adba29",
          "message": "ci: require 60% patch coverage in Codecov (#6022)\n\n### What changes were proposed in this PR?\n\nThe Codecov patch status was `informational: true`, so it never gated\nPRs. This makes it a real requirement:\n\n- set `target: 60%` on `coverage.status.patch.default`;\n- add `threshold: 10%` so the patch passes when coverage is at least\n`target - threshold` = 50%, softening the quantization on tiny diffs\n(e.g. a 2-coverable-line change passes at 1 line / 50%);\n- drop the `informational` flag so the patch check can fail.\n\nPRs must now cover at least 50% of the lines they change (60% target,\n10% slack). Project status (auto target, 1% threshold), flag\ncarryforward, and ignore rules are unchanged. The explanatory comment in\n`codecov.yml` is updated to match.\n\n### Any related issues, documentation, discussions?\n\nCloses #6021\n\n### How was this PR tested?\n\nConfig-only change; `codecov.yml` validated as well-formed YAML. Codecov\napplies the new policy on the next PR upload.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Claude Opus 4.8)",
          "timestamp": "2026-06-29T06:18:20Z",
          "url": "https://github.com/apache/texera/commit/613b76e467713efffdeef353b4c831e0f7adba29"
        },
        "date": 1782742627500,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 13083.007,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 16807.624,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 21928.483,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 76221.623,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 85722.442,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 109592.184,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 703355.449,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 743891.071,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 772058.654,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 10583.183,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 13439.503,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 15207.915,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 71619.608,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 78708.114,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 83351.675,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 697762.484,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 737237.273,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 756823.442,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 10103.966,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 13890.348,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 20408.955,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 73608.061,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 79078.771,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 89156.15,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 691056.358,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 730112.347,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 754836.431,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 12230.419,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 16569.849,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 17813.202,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 91105.915,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 99408.871,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 110782.29,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 882458.151,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 930883.477,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 960145.81,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 11482.79,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 13750.705,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 16911.905,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 95203.142,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 100217.723,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 114883.806,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 888813.324,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 938591.139,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 968163.005,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 12127.199,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 13844.415,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 19432.058,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 92801.443,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 100106.09,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 108445.897,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 914207.924,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 971678.418,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 984551.387,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 20001.976,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 21657.033,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 24757.096,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 172203.878,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 180841.22,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 190415.554,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1691454.217,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1742301.889,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1773409.749,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 20982.684,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 22048.199,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 25923.316,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 172701.669,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 181158.241,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 197631.717,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1696296.739,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1755062.751,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1778269.367,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 20473.588,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 24940.244,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 27069.01,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 178592.525,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 189210.061,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 199683.899,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1778499.136,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 1833417.507,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 1877928.829,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Yicong Huang",
            "username": "Yicong-Huang",
            "email": "17627829+Yicong-Huang@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "1a584332af50eb21e715df91a108a3d7c81736cc",
          "message": "test(amber): make PveResourceSpec hermetic via process-runner seam (#6024)\n\n### What changes were proposed in this PR?\n\n`PveResourceSpec` runs in the `amber` unit CI job\n(`AMBER_TEST_FILTER=skip-integration`) but is not tagged\n`@IntegrationTest`, so every run shelled out to **real `pip` over the\nnetwork**:\n\n- each `createNewPve` ran `pip install -r requirements.txt` (the full\namber dependency set), and\n- the lazy `resolveSystemPackages()` installed `requirements.txt` into a\nthrowaway venv, then `pip freeze`\n\n— roughly 6 full installs per spec run. That made a PR-blocking unit\nspec **network-dependent (flaky)** and **slow**.\n\nThis PR funnels every child process in `PveManager` (venv creation, pip\ninstall / uninstall / freeze) through a single injectable seam:\n\n```scala\nprivate[pythonvirtualenvironment] var runProcess: ProcessRunner =\n  (command, env, logger) => Process(command, None, env: _*).!(logger)\n```\n\nProduction wiring is unchanged (the default runner executes the command\nfor real). `PveResourceSpec` swaps in a **ScalaMock `mockFunction`**\nwhose handler fabricates the `<venv>/bin/{python,pip}` layout, emits the\nresolved system set on `freeze`, and returns a configurable exit code —\nso the spec is **fully hermetic: no venv, no pip, no network**.\n`PveManager` still owns the metadata files (`user-packages.txt`) and\nqueue messages, so that logic stays under test.\n\nBecause failures are now cheap to trigger, this also adds negative\ncoverage that was previously impractical: venv-create failure,\nrequirements-install failure, user-package install failure, and\nsystem-package rejection (using `pyarrow`, a hard amber dependency).\n\n| Before | After |\n| --- | --- |\n| ~6 real `pip install` per run, hits PyPI | 0 network calls |\n| flaky on network hiccups, ~minutes | deterministic, ~8s |\n| only happy-path assertions | + 4 negative/failure cases |\n\n### Any related issues, documentation, discussions?\n\nCloses #6023\n\n### How was this PR tested?\n\n`PveManager` is an `object`, so the test points the shared `runProcess`\nat the ScalaMock `mockFunction` in `beforeAll` and restores the real\nrunner in `afterAll`. ScalaMock expectations are per-test, so\n`expectProcessCalls()` (an `anyNumberOfTimes` handler) is invoked at the\ntop of each test that exercises a process. Run with JDK 17:\n\n```bash\nSTORAGE_JDBC_USERNAME=texera STORAGE_JDBC_PASSWORD=password \\\n  sbt \"WorkflowExecutionService/testOnly org.apache.texera.web.resource.pythonvirtualenvironment.PveResourceSpec\"\n# -> Tests: succeeded 25, failed 0, in ~8s\n```\n\nAlso green:\n\n```bash\nsbt scalafmtCheckAll\nsbt \"WorkflowExecutionService/scalafixAll --check\"\n```\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Claude Opus 4.8)",
          "timestamp": "2026-06-30T04:15:16Z",
          "url": "https://github.com/apache/texera/commit/1a584332af50eb21e715df91a108a3d7c81736cc"
        },
        "date": 1782826183951,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 14317.914,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 19154.334,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 20886.991,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 75651.987,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 85839.19,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 90615.563,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 700789.025,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 743197.073,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 782372.152,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 10664.228,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 12464.18,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 13965.611,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 72548.181,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 78529.025,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 83429.333,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 701587.747,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 741221.591,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 759315.751,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 10040.462,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 12574.257,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 15250.401,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 72921.638,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 79810.305,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 82520.47,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 697503.839,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 737711.305,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 757704.568,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 12459.986,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 16561.975,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 18951.047,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 93071.74,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 104465.23,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 112108.075,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 888259.512,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 948171.56,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 965255.687,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 11944.768,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 15074.108,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 17947.282,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 92346.14,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 100024.196,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 104081.815,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 897299.862,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 941898.291,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 960093.767,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 12086.255,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 14831.099,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 17601.971,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 91910.074,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 101839.869,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 112054.042,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 913843.402,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 968650.349,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 996492.507,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 21428.94,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 24920.153,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 31586.455,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 178324.878,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 188331.758,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 207982.228,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1705910.191,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1759944.361,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1792678.428,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 20538.55,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 21774.647,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 23939.315,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 172667.592,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 181907.343,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 188925.313,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1715880.475,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1780087.212,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1831922.351,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 21950.173,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 25927.741,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 29150.349,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 180146.707,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 190517.431,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 203001.179,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1803497.897,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 1861860.881,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 1870220.912,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xiaozhen Liu",
            "username": "Xiao-zhen-Liu",
            "email": "xiaozl3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "104bcc40cbda04621b37bcc5fb6f3ca58cb99f46",
          "message": "feat(dao): add operator_port_cache table (#5967)\n\n### What changes were proposed in this PR?\n\nAdds the `operator_port_cache` table that records a materialized output\nport\nresult so it can be reused across executions. It is keyed by\n`(workflow_id, global_port_id, cache_key)` and stores the JSON the cache\nkey was\ncomputed from, the result location, an optional tuple count and source\nexecution\nid, and a database-managed `updated_at`. The foreign key to\n`workflow(wid)` is\n`ON DELETE CASCADE`. The stored JSON (`cache_key_json`) lets a lookup\nconfirm a\nhash match by comparing the full JSON, so a hash collision never reuses\nthe wrong\nresult.\n\nThe change is additive: a new table in `sql/texera_ddl.sql` (fresh\ninstalls) plus\na Liquibase migration `sql/updates/26.sql` registered in\n`sql/changelog.xml`\n(existing deployments). No code reads or writes the table yet; the cache\nread/write\nlogic and its tests land with the cache service that uses it, following\nthe\nconvention of testing a table through its consumer (as `feedback` is\ntested via\n`FeedbackResourceSpec`).\n\n### Any related issues, documentation, discussions?\n\nCloses #5969. Part of the storage foundation #5882 (umbrella #5881).\nDesign discussion: #5880.\n\n### How was this PR tested?\n\nVerified the schema directly against Postgres: the migration applies\ncleanly, the\ncolumns and primary key `(workflow_id, global_port_id, cache_key)` are\ncorrect,\nthe foreign key's delete rule is `CASCADE`, the schema file and the\nmigration\ndefine identical columns/keys, and `changelog.xml` is well-formed and\nregisters\n`26.sql`. The generated jOOQ classes build from the table. The table's\nruntime\nbehavior is exercised by the cache service tests in the follow-up PR.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Opus 4.8 (Claude Code)",
          "timestamp": "2026-07-01T06:34:40Z",
          "url": "https://github.com/apache/texera/commit/104bcc40cbda04621b37bcc5fb6f3ca58cb99f46"
        },
        "date": 1782913462116,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 13128.205,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 17706.128,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 23363.492,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 72644.444,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 80248.391,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 88021.445,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 686240.704,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 707849.144,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 774231.7,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 10070.194,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 12790.537,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 17124.791,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 72930.668,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 76855.875,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 84063.474,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 689838.433,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 719099.118,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 754670.576,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 9793.48,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 15025.247,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 18811.638,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 72342.267,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 76913.581,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 88264.997,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 690338.785,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 707388.566,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 729040.441,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 11934.532,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 17910.414,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 18763.989,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 90757.445,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 102068.638,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 124334.226,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 870041.825,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 903982.945,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 941839.967,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 11304.464,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 12952.089,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 19257.307,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 91753.647,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 95332.374,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 97661.97,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 877167.714,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 912165.443,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 953961.298,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 11459.946,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 15141.719,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 17311.393,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 91483.251,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 96905.562,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 104036.982,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 893469.988,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 917504.205,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 949872.826,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 19464.859,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 22067.325,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 26233.461,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 166541.614,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 174278.867,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 219092.914,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1639919.041,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1691450.544,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1722798.061,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 19685.298,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 20675.063,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 26612.491,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 168070.593,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 174209.559,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 209511.931,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1669655.702,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1732848.459,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1789694.399,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 20280.414,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 25367.335,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 30623.391,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 177328.94,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 191875.407,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 242155.631,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1750698.607,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 1800591.419,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 1823175.704,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "24b587fcbf30bce1a4614a22bbf1c5a6ad1e158e",
          "message": "test(workflow-core): fill uncovered branches in tuple and type utilities (#6055)\n\n### What changes were proposed in this PR?\n\nFill uncovered code paths in five `workflow-core` tuple/type utility\nclasses, selected from the Codecov report (not spec-name gaps). No\nproduction-code changes.\n\n| File | Codecov before | Missed lines targeted |\n| --- | --- | --- |\n| `Tuple.scala` | 54.1% | 24 — `getField` miss/Attribute overload,\n`enforceSchema`, equals branches (incl. binary contents / non-Tuple),\n`getPartialTuple`, `toString`, size/type-mismatch throws, 3-arg builder\n`add` + null guards, case-class synthetics |\n| `TupleLike.scala` | 12.8% | 32 — `SeqTupleLike`/`MapTupleLike`\n`enforceSchema` (positional vs name-based, null fill, extras dropped),\n`inMemSize` `???`, all seven `TupleLike.apply` overloads, the\n`NotAnIterable` guard implicit |\n| `AttributeType.java` | 47.2% | 18 — `getName` ANY branch (`\"\"`), the\nfull `getAttributeType(Class)` mapping incl. primitive/unknown fallback\nto ANY |\n| `AttributeTypeUtils.scala` | 71.7% | 20 (+29 partials) —\n`SchemaCasting`, `tupleCasting`, both `parseFields` overloads,\nnull-inference early-returns, BINARY/ANY inference fallback, LONG/DOUBLE\n`compare`, unsupported-type throws, forced-parse failures |\n| `ArrowUtils.scala` | 67.8% | 15 —\n`appendTexeraTuple`/`setTexeraTuple`/`getTexeraTuple` round-trip (all 7\ntypes + nulls + row indices), parse-failure → null field,\n`fromAttributeType(null)` throw, unrecognized `texera_type` metadata\nfallback |\n\nExtends the existing `TupleSpec`, `AttributeTypeUtilsSpec`, and\n`ArrowUtilsSpec` (the operator-module `ArrowUtilsSpec` doesn't attribute\ncoverage to `workflow-core` sources), and adds `TupleLikeSpec` /\n`AttributeTypeSpec`.\n\nBehavior notes pinned along the way: `$attrType` error messages render\nthe lowercase wire name; the `NotAnIterable` iterable-guard implicit is\nnot summoned for multi-iterable varargs under Scala 2.13 inference\n(pinned by direct invocation).\n\n### Any related issues, documentation, discussions?\n\nFollow-up to the review feedback on #6043: prioritize tests that fill\nuncovered code paths.\n\n### How was this PR tested?\n\n- `sbt \"WorkflowCore/testOnly *TupleSpec *TupleLikeSpec\n*AttributeTypeSpec *AttributeTypeUtilsSpec\norg.apache.texera.amber.util.ArrowUtilsSpec\"` — 105 tests, all green\n- `sbt \"WorkflowCore/Test/scalafmtCheck\"` and `sbt\n\"WorkflowCore/scalafixAll --check\"` — clean\n- CI + Codecov delta to confirm\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-02T07:11:04Z",
          "url": "https://github.com/apache/texera/commit/24b587fcbf30bce1a4614a22bbf1c5a6ad1e158e"
        },
        "date": 1782999508727,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 16587.86,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 21516.318,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 25126.366,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 92798.177,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 102871.065,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 106786.06,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 874718.147,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 904498.415,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 947779.166,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 12351.497,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 14277.196,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 18541.251,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 90707.942,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 97312.256,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 108261.906,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 877413.238,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 909536.682,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 915683.429,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 12059.942,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 15205.809,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 17898.883,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 89745.176,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 95892.226,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 97594.001,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 885073.542,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 919534.43,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 928803.776,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 14877.496,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 19370.613,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 21497.477,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 113089.598,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 119345.414,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 129025.111,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 1096991.407,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 1130013.709,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 1148818.825,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 14738.374,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 18801.791,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 21085.57,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 113950.905,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 118804.254,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 123397.768,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1094712.572,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1127409.723,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1177487.177,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 14514.435,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 16932.101,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 20338.863,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 115776.662,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 121464.27,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 134336.171,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 1113174.714,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 1146631.287,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1182231.54,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 23484.942,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 25643.197,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 30739.781,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 197671.94,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 207019.201,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 248938.188,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1935043.557,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1971917.626,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 2014332.781,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 23484.831,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 25080.92,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 27701.019,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 199126.574,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 204935.517,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 220311.081,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1963464.932,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1994768.978,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 2039044.213,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 23906.638,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 25879.682,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 29602.401,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 204943.998,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 212542.524,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 232857.343,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 2020534.585,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 2058540.448,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 2077812.349,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "48ab6ca5d627e6d8c9c23e5edfa6556a3681fb07",
          "message": "test(workflow-operator): add unit test coverage for scan source executors (#6076)\n\n### What changes were proposed in this PR?\n\nAdd unit test coverage for the four file-reading scan source\n**executors**, selected from the Codecov report. No production-code\nchanges. Every test is driven by a local temp file (`file:` URI →\n`ReadonlyLocalFileDocument`); no live DB/network/S3/Iceberg.\n\n| File | Codecov before | Missed lines targeted |\n| --- | --- | --- |\n| `ParallelCSVScanSourceOpExec.scala` | 0% | 43 — construct + schema\ninit, `open` byte-range partitioning (both `endOffset` branches +\nmid-file partial-line skip + header skip), `produceTuple` happy path,\nshort-row null padding, all-null (blank) line discard, `close` |\n| `JSONLScanSourceOpExec.scala` | 0% | 23 — construct, `produceTuple`,\n`open` line counting + worker partitioning (both ternary branches), row\nlimit, `close` |\n| `CSVOldScanSourceOpExec.scala` | 0% | 21 — construct + header-based\nschema, `open` (header vs no-header start offset), `produceTuple` +\nlimit, `close` null-guard before open |\n| `ArrowSourceOpExec.scala` | 0% | 30 — construct, `open` success +\nerror-wrapping path, `produceTuple` batch iteration + offset/limit,\n`close` no-op before open |\n\nNew specs live in each executor's own package so their `private[...]`\nconstructors are reachable, mirroring the existing\n`FileScanSourceOpExecSpec` temp-file/URI pattern.\n\n### Any related issues, documentation, discussions?\n\nFollow-up to the review feedback on #6043: prioritize tests that fill\nuncovered code paths.\n\n### How was this PR tested?\n\n- `sbt \"WorkflowOperator/testOnly *ParallelCSVScanSourceOpExecSpec\n*JSONLScanSourceOpExecSpec *CSVOldScanSourceOpExecSpec\n*ArrowSourceOpExecSpec\"` — 17 tests, all green\n- `sbt \"WorkflowOperator/Test/scalafmtCheck\"` and `sbt\n\"WorkflowOperator/scalafixAll --check\"` — clean\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-03T09:24:10Z",
          "url": "https://github.com/apache/texera/commit/48ab6ca5d627e6d8c9c23e5edfa6556a3681fb07"
        },
        "date": 1783085359385,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 14003.026,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 18285.856,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 22711.892,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 84704.473,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 92349.207,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 101175.766,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 818873.309,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 853289.838,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 880376.735,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 10804.955,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 13747.418,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 16012.906,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 82041.946,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 89244.085,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 94803.302,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 810674.507,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 849091.98,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 867029.929,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 10747.848,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 13447.697,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 15549.155,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 82171.825,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 89165.89,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 94233.979,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 807582.262,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 848084.012,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 863177.396,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 12878.38,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 16034.083,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 18512.555,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 102865.751,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 110284.653,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 118014.289,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 1012358.007,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 1055104.413,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 1072973.292,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 12717.806,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 15163.591,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 18730.784,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 101900.015,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 111927.96,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 114052.762,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1009175.113,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1044760.925,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1073398.315,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 12327.551,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 14463.523,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 18589.125,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 104591.737,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 112533.619,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 117596.276,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 1015011.252,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 1051082.4,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1082828.452,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 20422.746,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 23806.248,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 29299.818,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 179257.204,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 192503.281,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 203396.525,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1791574.588,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1840792.239,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1853944.485,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 20386.16,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 21801.944,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 27715.19,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 182472.802,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 192656.066,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 195270.863,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1769973.809,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1822096.454,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1838511.947,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 20859.549,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 24397.341,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 32248.487,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 188771.582,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 196808.985,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 209022.738,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1925703.7,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 1993597.404,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 2007701.053,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "4c9d30a132672b9a0e8ecfe1d480a473dae16e4d",
          "message": "test(config): add unit test coverage for the remaining config objects (#6094)\n\n### What changes were proposed in this PR?\n\nAdd unit test coverage for the remaining `common/config` objects,\nselected from the Codecov report (all 0%). No production-code changes.\n\n| File | Codecov before | What the tests pin |\n| --- | --- | --- |\n| `UdfConfig.scala` | 0% | python/R path + log handler defaults from\nudf.conf |\n| `DefaultsConfig.scala` | 0% | the `reinit` flag and the flattened\n`allDefaults` short-key/value map from default.conf |\n| `ComputingUnitConfig.scala` | 0% | local/sharing enabled flags |\n| `LLMConfig.scala` | 0% | LiteLLM base URL + master key |\n| `PekkoConfig.scala` | 0% | actor/serialization, remote/artery, and\ncluster/failure-detector settings from cluster.conf (no ActorSystem\nstarted) |\n| `PythonUtils.scala` | 0% | `getPythonExecutable` blank→`python3`\nfallback and trimmed-path branch |\n\nReading each value forces resolution from the backing `.conf`;\nenv/system-property-overridable values are guarded on the override being\nunset (mirroring `StorageConfigSpec`).\n\n### Any related issues, documentation, discussions?\n\nFollow-up to the review feedback on #6043: prioritize tests that fill\nuncovered code paths.\n\n### How was this PR tested?\n\n- `sbt \"Config/testOnly *UdfConfigSpec *DefaultsConfigSpec\n*ComputingUnitConfigSpec *LLMConfigSpec *PekkoConfigSpec\n*PythonUtilsSpec\"` — 13 tests, all green\n- `sbt \"Config/Test/scalafmtCheck\"` and `sbt \"Config/scalafixAll\n--check\"` — clean\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-04T08:27:46Z",
          "url": "https://github.com/apache/texera/commit/4c9d30a132672b9a0e8ecfe1d480a473dae16e4d"
        },
        "date": 1783171118479,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 15495.434,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 19263.106,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 23144.151,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 90027.108,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 98362.43,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 115518.155,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 860366.509,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 898260.273,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 919019.587,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 11836.501,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 13854.77,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 17231.579,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 86023.221,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 93915.876,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 100731.481,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 857757.842,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 897285.899,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 909256.852,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 11272.626,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 14946.385,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 16584.745,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 86679.176,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 93341.691,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 104209.763,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 857033.515,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 891820.871,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 902727.264,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 13621.639,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 21444.811,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 24751.525,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 110087.02,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 117026.384,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 138374.72,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 1052026.401,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 1089631.013,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 1116633.029,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 12934.38,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 15468.842,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 19172.461,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 106603.347,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 113174.951,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 118851.561,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1055529.26,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1096108.724,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1113057.18,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 13110.841,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 15830.141,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 21548.762,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 109352.505,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 116805.746,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 129901.13,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 1082865.946,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 1121534.072,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1134178.034,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 21790.662,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 23880.204,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 28397.753,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 185309.286,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 205431.601,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 216376.912,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1850040.581,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1892832.573,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1910726.098,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 21494.743,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 23292.662,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 26431.385,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 187776.482,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 195882.416,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 203804.19,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1852771.059,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1898580.448,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1926436.147,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 22180.33,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 26784.952,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 29285.078,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 194940.507,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 201653.243,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 219260.43,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1941250.519,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 1982733.239,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 1996957.833,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "PJ Fanning",
            "username": "pjfanning",
            "email": "pjfanning@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "6fdfe19106d64cfb33fb431ac82390966afc3e52",
          "message": "chore(deps): Upgrade to Pekko 1.6.0 (#6151)\n\n### What changes were proposed in this PR?\n\nBump Apache Pekko from 1.2.1 to 1.6.0: `pekkoVersion` in\n`amber/build.sbt`, plus the 11 corresponding `org.apache.pekko.*` jar\nentries in `amber/LICENSE-binary-java`.\n\nPekko is amber's actor runtime (actor, remote, cluster, persistence,\nstream, …); all modules move together on the shared version. The jump\nspans the 1.3.x → 1.6.0 maintenance lines — bug-fix and performance\nreleases that stay binary compatible within the 1.x series ([1.6.x\nrelease\nnotes](https://pekko.apache.org/docs/pekko/current/release-notes/releases-1.6.html)).\nNo other bundled jars change (transitive dependencies are unaffected),\nso the LICENSE-binary diff is limited to the Pekko version strings.\n\n### Any related issues, documentation, discussions?\n\nPekko release notes:\n[1.6.x](https://pekko.apache.org/docs/pekko/current/release-notes/releases-1.6.html)\n(see the linked notes for 1.3.x–1.5.x covered by this jump).\n\n### How was this PR tested?\n\nExisting test cases via the label-gated `engine` CI stack (amber unit\ntests + amber-integration on Linux/macOS); dependency bump only, no code\nchange. The binary licensing check verifies the bundled jars against\n`amber/LICENSE-binary-java`.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nThe dependency bump was authored by @pjfanning (no AI declaration). This\nPR description was written with generative AI tooling: Generated-by:\nClaude Code (Claude Fable 5).",
          "timestamp": "2026-07-05T09:52:42Z",
          "url": "https://github.com/apache/texera/commit/6fdfe19106d64cfb33fb431ac82390966afc3e52"
        },
        "date": 1783257791823,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 15415.873,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 19933.556,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 25213.874,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 91291.381,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 100304.975,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 106693.475,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 883505.809,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 918519.66,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 931514.395,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 11748.178,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 13874.228,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 16121.003,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 89110.784,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 95045.221,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 103735.397,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 876142.7,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 918231.608,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 935944.775,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 11431.247,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 13178.309,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 15087.303,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 89044.945,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 96453.727,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 104258.403,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 865652.849,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 905097.032,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 923863.208,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 14111.4,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 16719.599,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 21698.369,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 109815.359,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 116641.566,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 126089.097,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 1080624.925,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 1123664.575,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 1140008.777,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 13097.827,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 15290.476,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 17214.277,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 108240.898,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 116231.34,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 130510.366,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1081845.568,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1125225.852,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1135751.667,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 13728.896,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 15483.191,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 19147.894,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 111885.674,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 117671.199,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 125189.775,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 1094367.101,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 1143480.989,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1160484.075,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 22416.538,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 23661.994,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 29428.462,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 192389.554,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 202108.39,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 210734.776,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1906910.237,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1957852.442,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1982770.147,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 22554.085,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 25025.898,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 36041.153,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 191795.772,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 200438.399,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 211295.323,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1920098.227,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1971367.179,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 2011129.672,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 22991.886,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 27069.096,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 34252.335,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 200676.604,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 208223.391,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 218874.175,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1971793.507,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 2013774.932,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 2033830.649,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "233c4deb2bd646a5e52eea042384caa28400dbf6",
          "message": "test(workflow-core): add unit tests for PhysicalFileNode, Schema, and VFSURIFactory (#6127)\n\n### What changes were proposed in this PR?\n\nAdds unit-test coverage for three value/utility classes in\n`common/workflow-core` that Codecov reports as having uncovered lines:\n\n- **`PhysicalFileNodeSpec`** (new) — covers the `PhysicalFileNode` value\nclass: constructor/getters, `isFile`/`isDirectory`, `addChildNode`\nsuccess and the direct-subpath guard failure, `equals`/`hashCode` (which\ndeliberately excludes `size`), and the recursive\n`getAllFileRelativePaths` traversal.\n- **`SchemaSpec`** (extended) — adds `hashCode`/`equals` cases\n(including non-`Schema` operands), both duplicate-name rejection paths\n(`add(Iterable)` and `add(Attribute)`), and a Jackson JSON round-trip.\n- **`VFSURIFactorySpec`** (extended) — adds the missing-value guard\nwhere a required key is the final path segment.\n\nNo production code is changed; this is test-only.\n\n### Any related issues, documentation, discussions?\n\nCoverage gaps identified from the Codecov report for `apache/texera`.\n\n### How was this PR tested?\n\nNew/extended ScalaTest specs, run locally:\n\n```\nsbt \"WorkflowCore/testOnly *PhysicalFileNodeSpec *SchemaSpec *VFSURIFactorySpec\"\n```\n\nAll pass. `scalafmt` and `scalafixAll --check` are clean.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-06T07:35:15Z",
          "url": "https://github.com/apache/texera/commit/233c4deb2bd646a5e52eea042384caa28400dbf6"
        },
        "date": 1783346673680,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 14372.821,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 17729.499,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 24183.253,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 79624.38,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 87883.418,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 92903.11,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 740455.931,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 767706.555,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 814593.762,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 11142.63,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 13226.887,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 16857.661,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 77780.067,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 84867.402,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 92238.335,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 725355.701,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 747935.183,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 780680.928,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 10460.057,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 12794.476,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 16319.16,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 75636.186,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 79066.375,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 86862.985,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 729106.522,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 756748.708,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 787674.671,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 12696.644,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 18373.016,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 20127.232,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 96521.32,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 103269.157,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 123936.626,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 932082.896,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 961863.373,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 991954.905,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 12567.609,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 16460.251,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 19466.27,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 96032.452,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 99473.491,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 102241.479,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 933969.626,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 971350.306,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1017339.243,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 12655.782,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 14381.288,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 19479.383,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 97429.414,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 103487.771,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 116158.441,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 957362.338,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 1004201.888,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1041279.746,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 21856.667,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 23454.556,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 32641.136,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 178467.073,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 189626.357,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 198678.407,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1769947.536,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1841021.167,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1868695.744,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 21553.922,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 22691.026,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 27129.338,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 181089.776,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 194053.691,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 200844.029,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1764316.011,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1826375.515,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1872430.746,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 22574.904,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 26452.328,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 34709.924,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 186611.535,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 197976.919,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 215539.918,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1823711.027,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 1881708.234,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 1907132.229,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "40526b8e1783e7782ce6043e7ca72840b7fbe04d",
          "message": "test(amber): add NetworkOutputGateway SELF-rewrite and FIFO-state tests (#6159)\n\n### What changes were proposed in this PR?\n\nAdds `NetworkOutputGatewaySpec` (new) for the amber messaging layer's\n`NetworkOutputGateway`, which had no spec. Using a hand-rolled capture\nhandler (no actor system), it covers the previously-untested paths:\n\n- `sendTo(ChannelIdentity, payload)` rewriting a `SELF` receiver to the\nactor id, and passing a non-`SELF` channel through verbatim;\n- `sendTo(SELF, DataPayload)` folding `SELF` to the actor id;\n- per-channel sequence-number increment and `getFIFOState`.\n\nNo production code changed.\n\n### Any related issues, documentation, discussions?\n\nCoverage gaps identified from the Codecov report for `apache/texera`\n(amber module coverage push toward 80%).\n\n### How was this PR tested?\n\n```\nsbt \"WorkflowExecutionService/testOnly *NetworkOutputGatewaySpec\"\n```\n\nAll 5 pass. `scalafmt` and `scalafixAll --check` are clean.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-07T08:18:50Z",
          "url": "https://github.com/apache/texera/commit/40526b8e1783e7782ce6043e7ca72840b7fbe04d"
        },
        "date": 1783431817345,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 14943.396,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 18403.681,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 21919.827,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 91442.749,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 99079.095,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 117563.436,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 878386.118,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 908962.992,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 942561.716,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 12281.845,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 18547.735,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 42537.557,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 87220.306,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 94397.106,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 100133.302,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 867236.389,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 906589.422,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 926454.903,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 11599.078,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 14703.084,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 16673.004,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 89151.896,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 95387.664,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 107485.263,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 875849.281,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 915508.317,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 928737.106,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 13905.451,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 19403.803,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 20907.019,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 110252.202,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 118808.293,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 127605.932,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 1077304.5,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 1121749.877,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 1146869.722,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 14138.181,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 16427.782,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 21357.627,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 109977.67,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 118336.487,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 129210.964,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1085436.103,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1130613.242,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1145744.701,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 13612.739,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 15174.344,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 19344.403,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 109423.252,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 118288.675,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 124368.672,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 1090960.747,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 1132074.697,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1163266.408,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 22125.598,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 23977.307,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 33830.574,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 190319.742,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 198198.736,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 213792.478,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1923743.208,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1969062.518,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 2009166.301,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 22110.241,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 23145.019,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 25274.81,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 191502.106,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 201359.479,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 213770.504,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1914218.976,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1961843.94,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1991767.947,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 22854.375,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 26100.704,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 32643.56,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 202074.278,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 212588.898,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 230296.684,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 2020402.795,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 2061338.063,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 2079941.122,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "18ccda990415e70fc835b1728585cea4d211888f",
          "message": "chore(deps, frontend): bump the frontend-patch group in /frontend with 4 updates (#6233)\n\nBumps the frontend-patch group in /frontend with 4 updates:\n[@vitest/browser](https://github.com/vitest-dev/vitest/tree/HEAD/packages/browser),\n[@vitest/browser-playwright](https://github.com/vitest-dev/vitest/tree/HEAD/packages/browser-playwright),\n[@vitest/coverage-v8](https://github.com/vitest-dev/vitest/tree/HEAD/packages/coverage-v8)\nand\n[vitest](https://github.com/vitest-dev/vitest/tree/HEAD/packages/vitest).\n\nUpdates `@vitest/browser` from 4.1.9 to 4.1.10\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/vitest-dev/vitest/releases\">@​vitest/browser's\nreleases</a>.</em></p>\n<blockquote>\n<h2>v4.1.10</h2>\n<h3>   🐞 Bug Fixes</h3>\n<ul>\n<li><strong>browser</strong>: Check fs access in builtin commands\n[backport to v4]  -  by <a\nhref=\"https://github.com/hi-ogawa\"><code>@​hi-ogawa</code></a>,\n<strong>Hiroshi Ogawa</strong> and <strong>OpenCode\n(claude-opus-4-8)</strong> in <a\nhref=\"https://redirect.github.com/vitest-dev/vitest/issues/10680\">vitest-dev/vitest#10680</a>\n<a href=\"https://github.com/vitest-dev/vitest/commit/5c18dd267\"><!-- raw\nHTML omitted -->(5c18d)<!-- raw HTML omitted --></a></li>\n<li><strong>vm</strong>: Fix external module resolve error with deps\noptimizer query for encoded URI [backport to v4]  -  by <a\nhref=\"https://github.com/SveLil\"><code>@​SveLil</code></a> and <a\nhref=\"https://github.com/hi-ogawa\"><code>@​hi-ogawa</code></a> in <a\nhref=\"https://redirect.github.com/vitest-dev/vitest/issues/10661\">vitest-dev/vitest#10661</a>\n<a href=\"https://github.com/vitest-dev/vitest/commit/bae52b511\"><!-- raw\nHTML omitted -->(bae52)<!-- raw HTML omitted --></a></li>\n</ul>\n<h5>    <a\nhref=\"https://github.com/vitest-dev/vitest/compare/v4.1.9...v4.1.10\">View\nchanges on GitHub</a></h5>\n</blockquote>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a\nhref=\"https://github.com/vitest-dev/vitest/commit/db616d227b6e0cb07a94f5d1bba262ee95db7e46\"><code>db616d2</code></a>\nchore: release v4.1.10 (<a\nhref=\"https://github.com/vitest-dev/vitest/tree/HEAD/packages/browser/issues/10718\">#10718</a>)</li>\n<li><a\nhref=\"https://github.com/vitest-dev/vitest/commit/5c18dd267ff7f47f24cab2f615a16b37d90feb7f\"><code>5c18dd2</code></a>\nfix(browser): check fs access in builtin commands [backport to v4] (<a\nhref=\"https://github.com/vitest-dev/vitest/tree/HEAD/packages/browser/issues/10680\">#10680</a>)</li>\n<li>See full diff in <a\nhref=\"https://github.com/vitest-dev/vitest/commits/v4.1.10/packages/browser\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\nUpdates `@vitest/browser-playwright` from 4.1.9 to 4.1.10\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/vitest-dev/vitest/releases\">@​vitest/browser-playwright's\nreleases</a>.</em></p>\n<blockquote>\n<h2>v4.1.10</h2>\n<h3>   🐞 Bug Fixes</h3>\n<ul>\n<li><strong>browser</strong>: Check fs access in builtin commands\n[backport to v4]  -  by <a\nhref=\"https://github.com/hi-ogawa\"><code>@​hi-ogawa</code></a>,\n<strong>Hiroshi Ogawa</strong> and <strong>OpenCode\n(claude-opus-4-8)</strong> in <a\nhref=\"https://redirect.github.com/vitest-dev/vitest/issues/10680\">vitest-dev/vitest#10680</a>\n<a href=\"https://github.com/vitest-dev/vitest/commit/5c18dd267\"><!-- raw\nHTML omitted -->(5c18d)<!-- raw HTML omitted --></a></li>\n<li><strong>vm</strong>: Fix external module resolve error with deps\noptimizer query for encoded URI [backport to v4]  -  by <a\nhref=\"https://github.com/SveLil\"><code>@​SveLil</code></a> and <a\nhref=\"https://github.com/hi-ogawa\"><code>@​hi-ogawa</code></a> in <a\nhref=\"https://redirect.github.com/vitest-dev/vitest/issues/10661\">vitest-dev/vitest#10661</a>\n<a href=\"https://github.com/vitest-dev/vitest/commit/bae52b511\"><!-- raw\nHTML omitted -->(bae52)<!-- raw HTML omitted --></a></li>\n</ul>\n<h5>    <a\nhref=\"https://github.com/vitest-dev/vitest/compare/v4.1.9...v4.1.10\">View\nchanges on GitHub</a></h5>\n</blockquote>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a\nhref=\"https://github.com/vitest-dev/vitest/commit/db616d227b6e0cb07a94f5d1bba262ee95db7e46\"><code>db616d2</code></a>\nchore: release v4.1.10 (<a\nhref=\"https://github.com/vitest-dev/vitest/tree/HEAD/packages/browser-playwright/issues/10718\">#10718</a>)</li>\n<li><a\nhref=\"https://github.com/vitest-dev/vitest/commit/5c18dd267ff7f47f24cab2f615a16b37d90feb7f\"><code>5c18dd2</code></a>\nfix(browser): check fs access in builtin commands [backport to v4] (<a\nhref=\"https://github.com/vitest-dev/vitest/tree/HEAD/packages/browser-playwright/issues/10680\">#10680</a>)</li>\n<li>See full diff in <a\nhref=\"https://github.com/vitest-dev/vitest/commits/v4.1.10/packages/browser-playwright\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\nUpdates `@vitest/coverage-v8` from 4.1.9 to 4.1.10\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/vitest-dev/vitest/releases\">@​vitest/coverage-v8's\nreleases</a>.</em></p>\n<blockquote>\n<h2>v4.1.10</h2>\n<h3>   🐞 Bug Fixes</h3>\n<ul>\n<li><strong>browser</strong>: Check fs access in builtin commands\n[backport to v4]  -  by <a\nhref=\"https://github.com/hi-ogawa\"><code>@​hi-ogawa</code></a>,\n<strong>Hiroshi Ogawa</strong> and <strong>OpenCode\n(claude-opus-4-8)</strong> in <a\nhref=\"https://redirect.github.com/vitest-dev/vitest/issues/10680\">vitest-dev/vitest#10680</a>\n<a href=\"https://github.com/vitest-dev/vitest/commit/5c18dd267\"><!-- raw\nHTML omitted -->(5c18d)<!-- raw HTML omitted --></a></li>\n<li><strong>vm</strong>: Fix external module resolve error with deps\noptimizer query for encoded URI [backport to v4]  -  by <a\nhref=\"https://github.com/SveLil\"><code>@​SveLil</code></a> and <a\nhref=\"https://github.com/hi-ogawa\"><code>@​hi-ogawa</code></a> in <a\nhref=\"https://redirect.github.com/vitest-dev/vitest/issues/10661\">vitest-dev/vitest#10661</a>\n<a href=\"https://github.com/vitest-dev/vitest/commit/bae52b511\"><!-- raw\nHTML omitted -->(bae52)<!-- raw HTML omitted --></a></li>\n</ul>\n<h5>    <a\nhref=\"https://github.com/vitest-dev/vitest/compare/v4.1.9...v4.1.10\">View\nchanges on GitHub</a></h5>\n</blockquote>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a\nhref=\"https://github.com/vitest-dev/vitest/commit/db616d227b6e0cb07a94f5d1bba262ee95db7e46\"><code>db616d2</code></a>\nchore: release v4.1.10 (<a\nhref=\"https://github.com/vitest-dev/vitest/tree/HEAD/packages/coverage-v8/issues/10718\">#10718</a>)</li>\n<li>See full diff in <a\nhref=\"https://github.com/vitest-dev/vitest/commits/v4.1.10/packages/coverage-v8\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\nUpdates `vitest` from 4.1.9 to 4.1.10\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/vitest-dev/vitest/releases\">vitest's\nreleases</a>.</em></p>\n<blockquote>\n<h2>v4.1.10</h2>\n<h3>   🐞 Bug Fixes</h3>\n<ul>\n<li><strong>browser</strong>: Check fs access in builtin commands\n[backport to v4]  -  by <a\nhref=\"https://github.com/hi-ogawa\"><code>@​hi-ogawa</code></a>,\n<strong>Hiroshi Ogawa</strong> and <strong>OpenCode\n(claude-opus-4-8)</strong> in <a\nhref=\"https://redirect.github.com/vitest-dev/vitest/issues/10680\">vitest-dev/vitest#10680</a>\n<a href=\"https://github.com/vitest-dev/vitest/commit/5c18dd267\"><!-- raw\nHTML omitted -->(5c18d)<!-- raw HTML omitted --></a></li>\n<li><strong>vm</strong>: Fix external module resolve error with deps\noptimizer query for encoded URI [backport to v4]  -  by <a\nhref=\"https://github.com/SveLil\"><code>@​SveLil</code></a> and <a\nhref=\"https://github.com/hi-ogawa\"><code>@​hi-ogawa</code></a> in <a\nhref=\"https://redirect.github.com/vitest-dev/vitest/issues/10661\">vitest-dev/vitest#10661</a>\n<a href=\"https://github.com/vitest-dev/vitest/commit/bae52b511\"><!-- raw\nHTML omitted -->(bae52)<!-- raw HTML omitted --></a></li>\n</ul>\n<h5>    <a\nhref=\"https://github.com/vitest-dev/vitest/compare/v4.1.9...v4.1.10\">View\nchanges on GitHub</a></h5>\n</blockquote>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a\nhref=\"https://github.com/vitest-dev/vitest/commit/db616d227b6e0cb07a94f5d1bba262ee95db7e46\"><code>db616d2</code></a>\nchore: release v4.1.10 (<a\nhref=\"https://github.com/vitest-dev/vitest/tree/HEAD/packages/vitest/issues/10718\">#10718</a>)</li>\n<li><a\nhref=\"https://github.com/vitest-dev/vitest/commit/bae52b5112a6fd8200101b88bf8af9685d077295\"><code>bae52b5</code></a>\nfix(vm): fix external module resolve error with deps optimizer query for\nenco...</li>\n<li>See full diff in <a\nhref=\"https://github.com/vitest-dev/vitest/commits/v4.1.10/packages/vitest\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\n\nDependabot will resolve any conflicts with this PR as long as you don't\nalter it yourself. You can also trigger a rebase manually by commenting\n`@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits\nthat have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all\nof the ignore conditions of the specified dependency\n- `@dependabot ignore <dependency name> major version` will close this\ngroup update PR and stop Dependabot creating any more for the specific\ndependency's major version (unless you unignore this specific\ndependency's major version or upgrade to it yourself)\n- `@dependabot ignore <dependency name> minor version` will close this\ngroup update PR and stop Dependabot creating any more for the specific\ndependency's minor version (unless you unignore this specific\ndependency's minor version or upgrade to it yourself)\n- `@dependabot ignore <dependency name>` will close this group update PR\nand stop Dependabot creating any more for the specific dependency\n(unless you unignore this specific dependency or upgrade to it yourself)\n- `@dependabot unignore <dependency name>` will remove all of the ignore\nconditions of the specified dependency\n- `@dependabot unignore <dependency name> <ignore condition>` will\nremove the ignore condition of the specified dependency and ignore\nconditions\n\n\n</details>\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>\nCo-authored-by: Xinyuan Lin <xinyual3@uci.edu>",
          "timestamp": "2026-07-08T06:26:15Z",
          "url": "https://github.com/apache/texera/commit/18ccda990415e70fc835b1728585cea4d211888f"
        },
        "date": 1783516938177,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 13187.552,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 16184.659,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 19052.257,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 75473.03,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 83631.185,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 94117.686,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 691895.016,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 731680.647,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 750824.401,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 10843.816,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 12594.345,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 15605.528,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 72722.371,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 79887.065,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 87315.899,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 691878.599,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 737447.444,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 765329.779,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 10148.026,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 13389.359,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 15145.969,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 72823.743,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 79843.076,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 84439.27,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 692108.086,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 746119.682,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 773059.455,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 12004.224,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 14995.338,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 18532.131,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 92187.211,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 102795.942,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 107695.703,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 904533.483,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 950334.324,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 992982.494,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 11486.731,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 14615.676,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 18419.813,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 91303.242,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 99873.399,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 108313.543,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 906523.151,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 960112.359,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 997660.562,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 11864.081,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 13972.853,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 18056.075,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 94652.149,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 101342.696,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 106572.219,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 918620.193,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 974214.253,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 988607.22,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 19972.476,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 23626.06,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 25605.529,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 175566.414,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 184530.571,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 196271.525,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1749183.981,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1820668.427,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1898767.269,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 20898.972,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 24553.47,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 26846.048,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 176784.234,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 187213.585,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 199316.467,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1750271.09,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1801262.229,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1822744.063,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 21133.152,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 25753.903,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 29425.887,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 191192.645,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 212747.803,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 244551.446,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1824293.236,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 1875150.752,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 1896029.895,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "29233cc86ed8687f5eafd6b5b7dd4e51b7000c84",
          "message": "fix(deps, pyamber): bump aiobotocore to 3.7.0 with s3fs/botocore/boto3 in /amber (#6228)\n\nBumps [aiobotocore](https://github.com/aio-libs/aiobotocore) from 2.26.0\nto 3.7.0.\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/aio-libs/aiobotocore/releases\">aiobotocore's\nreleases</a>.</em></p>\n<blockquote>\n<h2>3.7.0</h2>\n<ul>\n<li>replace per-PR <code>CHANGES.rst</code> / <code>__init__.py</code>\nceremony with an AI-drafted\nrelease flow: contributors no longer touch version or changelog files; a\nworkflow-triggered agent synthesizes merged PRs into a release PR at\nrelease\ntime (closes <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1167\">#1167</a>)\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1592\">#1592</a>)</li>\n<li>add <code>AioSession.warm_up_loader_caches()</code> and\n<code>warm_up_loader_caches</code>\noption in <code>AioConfig</code> to pre-populate botocore loader caches\noff the event\nloop, avoiding blocking file I/O on first client/waiter/paginator use\n(closes <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1199\">#1199</a>)\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1462\">#1462</a>)</li>\n<li>fix race condition in\n<code>AioAssumeRoleProvider._visited_profiles</code> causing\nfalse <code>InfiniteLoopConfigError</code> under concurrent async usage\n(closes <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1455\">#1455</a>)\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1537\">#1537</a>)</li>\n<li>fall back to synchronous <code>subprocess.run</code> (via\n<code>asyncio.to_thread</code>) for\n<code>credential_process</code> when the running event loop does not\nimplement\nsubprocess transports — notably <code>asyncio.SelectorEventLoop</code>\non Windows\n(closes <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1415\">#1415</a>)\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1588\">#1588</a>)</li>\n</ul>\n<h2>3.6.0</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>Relax <code>botocore</code> dependency specification by <a\nhref=\"https://github.com/jakob-keller\"><code>@​jakob-keller</code></a>\nin <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1578\">aio-libs/aiobotocore#1578</a></li>\n<li>Relax <code>botocore</code> dependency specification by <a\nhref=\"https://github.com/jakob-keller\"><code>@​jakob-keller</code></a>\nin <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1579\">aio-libs/aiobotocore#1579</a></li>\n</ul>\n<!-- raw HTML omitted -->\n<ul>\n<li>Bump actions/cache from 5.0.4 to 5.0.5 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1575\">aio-libs/aiobotocore#1575</a></li>\n<li>Bump packaging from 26.0 to 26.1 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1576\">aio-libs/aiobotocore#1576</a></li>\n<li>Bump anthropics/claude-code-action from 1.0.101 to 1.0.105 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1580\">aio-libs/aiobotocore#1580</a></li>\n<li>Bump anthropic from 0.96.0 to 0.97.0 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1581\">aio-libs/aiobotocore#1581</a></li>\n<li>ci(claude): skip dependabot and fork PRs in pull_request trigger by\n<a href=\"https://github.com/thehesiod\"><code>@​thehesiod</code></a> in\n<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1582\">aio-libs/aiobotocore#1582</a></li>\n<li>Revert pull_request → pull_request_target switch (App token 401) by\n<a href=\"https://github.com/thehesiod\"><code>@​thehesiod</code></a> in\n<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1583\">aio-libs/aiobotocore#1583</a></li>\n</ul>\n<!-- raw HTML omitted -->\n<p><strong>Full Changelog</strong>: <a\nhref=\"https://github.com/aio-libs/aiobotocore/compare/3.5.0...3.6.0\">https://github.com/aio-libs/aiobotocore/compare/3.5.0...3.6.0</a></p>\n<h2>3.5.0</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>Relax botocore dependency specification by <a\nhref=\"https://github.com/claude\"><code>@​claude</code></a>[bot] in <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1546\">aio-libs/aiobotocore#1546</a></li>\n<li>Bump botocore dependency specification by <a\nhref=\"https://github.com/claude\"><code>@​claude</code></a>[bot] in <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1571\">aio-libs/aiobotocore#1571</a></li>\n</ul>\n<!-- raw HTML omitted -->\n<ul>\n<li>Fix sync prompt template vars, add MCP commit fallback by <a\nhref=\"https://github.com/thehesiod\"><code>@​thehesiod</code></a> in <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1523\">aio-libs/aiobotocore#1523</a></li>\n<li>Fix Claude bot failing on issue comments by <a\nhref=\"https://github.com/thehesiod\"><code>@​thehesiod</code></a> in <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1536\">aio-libs/aiobotocore#1536</a></li>\n<li>Add dev environment to Claude CI workflow by <a\nhref=\"https://github.com/thehesiod\"><code>@​thehesiod</code></a> in <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1538\">aio-libs/aiobotocore#1538</a></li>\n<li>Bump cryptography from 46.0.6 to 46.0.7 in the uv group across 1\ndirectory by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1541\">aio-libs/aiobotocore#1541</a></li>\n<li>Bump anthropics/claude-code-action from 1.0.75 to 1.0.87 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1542\">aio-libs/aiobotocore#1542</a></li>\n<li>Bump astral-sh/setup-uv from 7.6.0 to 8.0.0 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/pull/1543\">aio-libs/aiobotocore#1543</a></li>\n</ul>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Changelog</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/aio-libs/aiobotocore/blob/main/CHANGES.rst\">aiobotocore's\nchangelog</a>.</em></p>\n<blockquote>\n<p>3.7.0 (2026-05-09)\n^^^^^^^^^^^^^^^^^^</p>\n<ul>\n<li>replace per-PR <code>CHANGES.rst</code> / <code>__init__.py</code>\nceremony with an AI-drafted\nrelease flow: contributors no longer touch version or changelog files; a\nworkflow-triggered agent synthesizes merged PRs into a release PR at\nrelease\ntime (closes <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1167\">#1167</a>)\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1592\">#1592</a>)</li>\n<li>add <code>AioSession.warm_up_loader_caches()</code> and\n<code>warm_up_loader_caches</code>\noption in <code>AioConfig</code> to pre-populate botocore loader caches\noff the event\nloop, avoiding blocking file I/O on first client/waiter/paginator use\n(closes <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1199\">#1199</a>)\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1462\">#1462</a>)</li>\n<li>fix race condition in\n<code>AioAssumeRoleProvider._visited_profiles</code> causing\nfalse <code>InfiniteLoopConfigError</code> under concurrent async usage\n(closes <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1455\">#1455</a>)\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1537\">#1537</a>)</li>\n<li>fall back to synchronous <code>subprocess.run</code> (via\n<code>asyncio.to_thread</code>) for\n<code>credential_process</code> when the running event loop does not\nimplement\nsubprocess transports — notably <code>asyncio.SelectorEventLoop</code>\non Windows\n(closes <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1415\">#1415</a>)\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1588\">#1588</a>)</li>\n</ul>\n<p>3.6.0 (2026-04-30)\n^^^^^^^^^^^^^^^^^^</p>\n<ul>\n<li>relax botocore dependency specification to support\n<code>&quot;botocore &gt;= 1.42.90, &lt; 1.43.1&quot;</code></li>\n<li>drop support for Python 3.9 (EOL)</li>\n</ul>\n<p>3.5.0 (2026-04-19)\n^^^^^^^^^^^^^^^^^^</p>\n<ul>\n<li>bump botocore dependency specification to support\n<code>&quot;botocore &gt;= 1.42.90, &lt; 1.42.92&quot;</code></li>\n</ul>\n<p>3.4.0 (2026-04-07)\n^^^^^^^^^^^^^^^^^^</p>\n<ul>\n<li>bump botocore dependency specification to support\n<code>&quot;botocore &gt;= 1.42.79, &lt; 1.42.85&quot;</code></li>\n</ul>\n<p>3.3.0 (2026-03-18)\n^^^^^^^^^^^^^^^^^^</p>\n<ul>\n<li>bump botocore dependency specification to support\n<code>&quot;botocore &gt;= 1.42.62, &lt; 1.42.71&quot;</code></li>\n</ul>\n<p>3.2.1 (2026-03-04)\n^^^^^^^^^^^^^^^^^^</p>\n<ul>\n<li>relax botocore dependency specification to support\n<code>&quot;botocore &gt;= 1.42.53, &lt; 1.42.62&quot;</code></li>\n</ul>\n<p>3.2.0 (2026-02-23)\n^^^^^^^^^^^^^^^^^^</p>\n<ul>\n<li>bump botocore dependency specification to support\n<code>&quot;botocore &gt;= 1.42.53, &lt; 1.42.56&quot;</code></li>\n</ul>\n<p>3.1.3 (2026-02-14)\n^^^^^^^^^^^^^^^^^^</p>\n<ul>\n<li>relax botocore dependency specification to support\n<code>&quot;botocore &gt;= 1.41.0, &lt; 1.42.50&quot;</code></li>\n</ul>\n<p>3.1.2 (2026-02-05)\n^^^^^^^^^^^^^^^^^^</p>\n<ul>\n<li>relax botocore dependency specification to support\n<code>&quot;botocore &gt;= 1.41.0, &lt; 1.42.43&quot;</code></li>\n</ul>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a\nhref=\"https://github.com/aio-libs/aiobotocore/commit/d232742cfc2e1098cb3a448f9e1d78eb0899203d\"><code>d232742</code></a>\nRelease v3.7.0 (<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1595\">#1595</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiobotocore/commit/0fb20e5732b50c1b8336bd952929185fe0528e25\"><code>0fb20e5</code></a>\nfix(draft-release): tighten window boundary + unique-PR count (<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1597\">#1597</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiobotocore/commit/e8bb50e26d754f5753b75a5b1b5f70be90ea1367\"><code>e8bb50e</code></a>\ndocs(CLAUDE.md): canonical branch-creation pattern for Claude workflows\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1596\">#1596</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiobotocore/commit/b8c2266f589dcbfe9f0ad04b3a4fc7491969c850\"><code>b8c2266</code></a>\nfix(draft-release): provision uv + Python in workflow (<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1594\">#1594</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiobotocore/commit/392dfcea287c2348c9924f9578855fb1df065776\"><code>392dfce</code></a>\nfeat: AI-drafted release flow (closes <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1167\">#1167</a>)\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1592\">#1592</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiobotocore/commit/0cbf43792b4a1d8b1286faa13657ea57d2f2b062\"><code>0cbf437</code></a>\nchore(release): consolidate unreleased 3.6.1/3.7.0/3.7.1 into 3.7.0 (<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1593\">#1593</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiobotocore/commit/e332cd4727756da8db07e5a8bfc7ff71a0dc9def\"><code>e332cd4</code></a>\nfix: credential_process on Windows + Selector event loop (closes <a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1415\">#1415</a>)\n(<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1588\">#1588</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiobotocore/commit/4d1dbadaa56133a75f1df0267d619ffe4b43ec3f\"><code>4d1dbad</code></a>\nBump anthropics/claude-code-action from 1.0.105 to 1.0.111 (<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1584\">#1584</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiobotocore/commit/bc3c5b5827fd8a8f3d3e89589c5abae67ef42584\"><code>bc3c5b5</code></a>\nSupport warm-up of loader caches (<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1462\">#1462</a>)</li>\n<li><a\nhref=\"https://github.com/aio-libs/aiobotocore/commit/804cf28a6f92e9c5fe66d12e02d68783e94b6f02\"><code>804cf28</code></a>\nBump pre-commit from 4.5.1 to 4.6.0 (<a\nhref=\"https://redirect.github.com/aio-libs/aiobotocore/issues/1586\">#1586</a>)</li>\n<li>Additional commits viewable in <a\nhref=\"https://github.com/aio-libs/aiobotocore/compare/2.26.0...3.7.0\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility\nscore](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=aiobotocore&package-manager=pip&previous-version=2.26.0&new-version=3.7.0)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't\nalter it yourself. You can also trigger a rebase manually by commenting\n`@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits\nthat have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all\nof the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop\nDependabot creating any more for this major version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop\nDependabot creating any more for this minor version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop\nDependabot creating any more for this dependency (unless you reopen the\nPR or upgrade to it yourself)\n\n\n</details>\n\n---------\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>\nCo-authored-by: probe <probe@x>\nCo-authored-by: Meng Wang <mengw15@uci.edu>\nCo-authored-by: Xinyuan Lin <xinyual3@uci.edu>",
          "timestamp": "2026-07-09T08:56:32Z",
          "url": "https://github.com/apache/texera/commit/29233cc86ed8687f5eafd6b5b7dd4e51b7000c84"
        },
        "date": 1783605075597,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 12173.944,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 16060.699,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 19283.196,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 67536.413,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 78705.66,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 91060.102,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 653741.552,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 679292.572,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 695162.711,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 9625.069,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 11407.134,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 14352.981,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 70372.25,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 75157.067,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 82575.348,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 647554.264,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 667074.243,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 694299.949,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 9087.451,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 10368.135,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 13233.793,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 66943.613,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 72997.199,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 96589.577,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 663410.416,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 692623.442,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 718249.667,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 11068.069,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 13612.448,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 16211.461,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 84506.297,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 92781.959,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 107545.296,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 830543.795,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 862260.584,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 873175.371,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 10907.658,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 12378.948,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 16199.592,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 85043.842,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 90496.753,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 97456.055,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 840015.159,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 873314.598,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 917003.105,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 11005.092,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 13192.758,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 15886.433,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 87752.08,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 92665.077,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 100968.665,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 844133.795,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 886383.548,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 931398.797,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 18225.248,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 19631.467,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 27122.354,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 151776.553,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 160030.182,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 172029.762,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1504708.661,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1583558.827,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1614984.685,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 17600.894,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 18530.356,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 19760.468,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 144131.568,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 154225.379,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 173119.791,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1443865.135,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1515800.136,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1539570.016,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 18416.611,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 22231.348,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 27157.435,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 159666.415,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 169837.093,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 187215.39,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1569118.483,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 1624680.313,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 1646157.572,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Kunwoo (Chris)",
            "username": "kunwp1",
            "email": "143021053+kunwp1@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "45c82a59a3efc3501e5b9edcb5c5cc7b9d6b02b1",
          "message": "fix(frontend): speed up dataset bulk upload with many files (#6306)\n\n### What changes were proposed in this PR?\n\nBulk-uploading many files to a dataset was extremely slow (~1,100 files\ntook ~12 minutes) because the Pending and Finished lists fully\nre-rendered on every change-detection pass, on the same thread driving\nthe uploads (root cause detailed in #5586).\n\nAs proposed in the issue: replace the `queuedFileNames` getter with a\ncached field refreshed only when the queue changes, virtualize the\nPending and Finished lists with `cdk-virtual-scroll` so only visible\nrows are in the DOM, and add `trackBy` to both lists.\n\n### Any related issues, documentation, discussions?\n\nCloses #5586.\n\n### How was this PR tested?\n\nScreen recording of a successful 1,100-file upload with the fix:\n\n\nhttps://github.com/user-attachments/assets/c23d7612-0317-4d18-970b-c98540005eed\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code, Claude Fable 5",
          "timestamp": "2026-07-10T05:17:26Z",
          "url": "https://github.com/apache/texera/commit/45c82a59a3efc3501e5b9edcb5c5cc7b9d6b02b1"
        },
        "date": 1783689757616,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 10600.51,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 14052.95,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 18383.618,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 57764.802,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 64099.752,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 73337.053,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 539326.448,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 577974.227,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 629582.309,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 7750.799,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 10793.816,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 11605.719,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 54736.472,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 60766.095,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 65877.431,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 538390.482,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 567407.129,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 575077.737,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 7873.705,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 10603.751,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 11152.349,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 53959.823,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 59806.343,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 68895.238,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 534875.579,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 562518.927,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 577586.992,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 9491.64,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 13077.058,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 13460.202,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 71249.918,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 76881.814,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 86577.952,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 683963.822,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 719450.664,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 736747.429,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 9105.867,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 12526.675,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 15295.128,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 70713.809,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 75875.131,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 87629.684,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 692662.595,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 733998.92,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 780026.405,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 9197.676,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 12297.891,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 17071.053,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 70610.78,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 77134.882,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 84796.869,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 700704.015,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 745528.108,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 772295.907,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 15526.236,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 18065.056,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 22606.221,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 131978.772,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 139463.365,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 146531.765,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1315626.105,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1367280.494,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1391555.16,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 15699.764,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 16553.869,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 22283.547,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 131521.146,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 138950.999,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 142440.555,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1329754.936,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1392562.365,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1438264.288,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 16203.059,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 18772.131,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 22588.66,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 140061.638,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 148868.568,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 161846.195,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1390469.465,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 1432218.899,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 1440584.102,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Meng Wang",
            "username": "mengw15",
            "email": "mengw15@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "ac39ac84aca97b72a35c17ba218c061b52ef1d18",
          "message": "test(computing-unit-managing-service): add unit test coverage for ComputingUnitHelpers (#6330)\n\n### What changes were proposed in this PR?\n\nAdd unit test coverage for `ComputingUnitHelpers.getComputingUnitStatus`\nand `getComputingUnitMetrics`, on their pure `local` and unknown-type\nbranches. Each constructs the jOOQ `WorkflowComputingUnit` pojo via\n`setType` (no DB). No production-code changes.\n\nThe `kubernetes` branch calls the static `KubernetesClient`\n(`getPodByName` / `getPodMetrics`) and needs a mock seam — out of scope\nhere, per the issue. (`WorkflowComputingUnitTypeEnum` defines only\n`local` and `kubernetes`, so an untyped unit exercises the `unknown`\nbranch.)\n\n### Any related issues, documentation, discussions?\n\nCloses #6326.\n\n### How was this PR tested?\n\n```\nsbt \"ComputingUnitManagingService/testOnly *ComputingUnitHelpersSpec\"\n```\n\nAll 4 pass. `scalafmt` and `scalafix --check` are clean. The spec\ntouches no cluster or DB — it constructs pojos and exercises only the\npure `local` / unknown branches.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-11T06:52:52Z",
          "url": "https://github.com/apache/texera/commit/ac39ac84aca97b72a35c17ba218c061b52ef1d18"
        },
        "date": 1783774892596,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 12012.912,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 14992.253,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 18379.228,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 69883.411,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 77636.927,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 119328.951,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 638343.568,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 665268.538,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 691485.264,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 9296.714,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 12139.99,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 13801.839,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 66848.351,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 71634.037,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 79584.897,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 648674.895,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 672828.675,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 700372.641,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 8923.043,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 11053.06,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 13367.818,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 68123.563,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 76504.156,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 94561.295,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 652970.572,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 680221.033,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 695540.023,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 10903.045,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 13906.902,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 16716.561,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 84279.955,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 91025.131,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 101556.355,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 803896.069,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 829410.43,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 862569.241,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 10721.078,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 12906.736,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 15775.53,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 82892.115,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 89216.115,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 98007.647,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 823611.028,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 854902.976,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 883406.925,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 10959.657,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 12516.774,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 16476.607,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 85886.117,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 92537.296,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 98736.247,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 845658.043,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 882784.763,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 926136.583,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 17647.565,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 19040.654,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 23214.907,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 147835.379,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 160051.368,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 174556.831,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1515318.74,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1614101.013,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1647530.587,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 17593.69,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 19034.418,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 21487.54,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 147145.381,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 153672.777,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 162913.342,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1517136.29,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1597679.587,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1628873.523,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 18378.114,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 22294.721,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 26262.049,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 158382.583,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 163837.567,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 185175.601,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1570882.295,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 1615514.034,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 1681596.996,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Meng Wang",
            "username": "mengw15",
            "email": "mengw15@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "afa8af22b2ac0279fbcc0c610cfe54e3ee0ef0b8",
          "message": "test(frontend): add unit test coverage for formly-utils field helpers (#6365)\n\n### What changes were proposed in this PR?\n\nAdd unit test coverage for the pure `FormlyFieldConfig` helpers in\n`frontend/src/app/common/formly/formly-utils.ts` — `getFieldByName`,\n`setHideExpression`, and the `createShouldHideFieldFunc` predicate\nfactory. A Vitest spec on plain objects (no `TestBed`). No\nproduction-code changes.\n\nThe rxjs `createOutputFormChangeEventStream` (debounce/distinct/share\nstream) is out of scope for this issue.\n\n### Any related issues, documentation, discussions?\n\nCloses #6361. See `frontend/TESTING.md`.\n\n### How was this PR tested?\n\n```\nng test --watch=false --include src/app/common/formly/formly-utils.spec.ts\n```\n\n9 pass. ESLint and Prettier on the new spec are clean. The spec is pure\n— it builds plain `FormlyFieldConfig` objects (no `TestBed`) and covers:\n`getFieldByName` (match / no-match / first-of-duplicate-keys);\n`setHideExpression` (sets the hide expression on present fields, no-op\nfor absent names); and the `createShouldHideFieldFunc` predicate\n(missing model → `false`, null target → `hideOnNull`, anchored regex\nmatch, and `equals` via `toString()`).\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])\n\n---------\n\nSigned-off-by: Meng Wang <mengw15@uci.edu>\nCo-authored-by: Copilot Autofix powered by AI <175728472+Copilot@users.noreply.github.com>",
          "timestamp": "2026-07-12T07:27:05Z",
          "url": "https://github.com/apache/texera/commit/afa8af22b2ac0279fbcc0c610cfe54e3ee0ef0b8"
        },
        "date": 1783862041427,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 15803.326,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 20450.234,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 25216.385,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 93766.02,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 101884.083,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 107997.962,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 889551.999,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 926666.052,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 947279.227,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 12937.788,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 18891.056,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 39798.306,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 92237.23,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 98099.333,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 105206.397,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 885621.576,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 922935.5,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 943920.108,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 12031.146,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 14346.069,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 16024.293,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 92038.501,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 97975.779,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 105827.861,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 883357.644,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 918725.695,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 947617.412,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 14014.316,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 19151.071,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 21546.491,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 114133.678,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 122220.588,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 138797.431,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 1089280.087,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 1127310.008,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 1145187.55,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 13997.913,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 15924.009,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 21791.224,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 111970.482,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 119381.765,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 135604.457,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1112304.76,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1151317.267,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1166376.949,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 13905.951,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 16427.269,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 20546.645,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 113516.527,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 121372.275,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 135694.906,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 1130657.854,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 1173335.256,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1204843.34,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 23172.667,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 24532.394,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 28335.224,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 198408.239,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 206174.006,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 223401.054,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1941785.402,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1999273.641,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 2052290.756,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 23088.752,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 23932.813,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 24928.467,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 202534.66,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 209970.695,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 223008.759,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1977359.855,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 2023769.823,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 2052623.196,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 24179.967,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 28435.604,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 33485.793,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 208670.973,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 217998.907,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 230700.692,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 2057959.693,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 2093082.819,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 2108937.309,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Meng Wang",
            "username": "mengw15",
            "email": "mengw15@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "0cdba7b5cd13ad9b19f0349ce402b3165e876c8c",
          "message": "test(frontend): add unit test coverage for UserProjectService (#6392)\n\n### What changes were proposed in this PR?\n\nAdds a Vitest spec for `UserProjectService`\n\n(`frontend/src/app/dashboard/service/user/project/user-project.service.ts`),\nwhich previously had no dedicated spec (codecov ~33%). The service is an\n`HttpClient` wrapper for the project dashboard, plus two static color\nhelpers.\n\n19 tests, using `HttpClientTestingModule` + `HttpTestingController` (no\nbackend):\n\n- **HTTP methods (14)** — each asserts request method / URL (built from\nthe\n  service's exported URL constants) / body, and the emitted response:\n`getProjectList`, `retrieveWorkflowsOfProject`,\n`retrieveFilesOfProject`,\n  `retrieveProject`, `createProject`, `updateProjectName`,\n  `updateProjectDescription` (raw-string body), `deleteProject`,\n`addWorkflowToProject`, `removeWorkflowFromProject`, `addFileToProject`,\n  `updateProjectColor`, `deleteProjectColor`, `removeFileFromProject`.\n- **Cached file list (3)** — `getProjectFiles` starts empty;\n`refreshFilesOfProject` fetches and caches;\n`deleteDashboardUserFileEntry`\n  deletes then refreshes the cache (two-request flow).\n- **Static helpers (2)** — `isInvalidColorFormat` and `isLightColor`\nacross\n  null / wrong-length / non-hex / 3- and 6-digit / white / black inputs.\n\n`NotificationService` (unused by this service, but a constructor\ndependency\nthat pulls in ng-zorro) is stubbed so `TestBed` stays hermetic. No\nproduction\ncode was changed.\n\n### Any related issues, documentation, discussions?\n\nCloses #6388\n\n### How was this PR tested?\n\nNew unit tests, run locally in `frontend/` (all green; the failure path\nwas\nverified by breaking a URL assertion to confirm the suite goes red):\n\n```\nng test --watch=false --include src/app/dashboard/service/user/project/user-project.service.spec.ts\n# Tests  19 passed (19)\neslint <spec>       # clean\nprettier --check    # clean\n```\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-13T07:36:40Z",
          "url": "https://github.com/apache/texera/commit/0cdba7b5cd13ad9b19f0349ce402b3165e876c8c"
        },
        "date": 1783950140494,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 15450.631,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 20818.869,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 24415.088,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 90847.399,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 97965.813,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 112287.427,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 838965.29,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 871997.952,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 915386.189,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 12002.79,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 15851.806,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 17523.129,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 86025.003,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 93210.251,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 97893.356,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 832285.542,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 871536.092,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 885238.411,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 11385.235,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 15413.988,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 17467.027,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 84978.294,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 91460.462,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 103061.177,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 841511.122,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 869851.739,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 881768.607,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 13389.848,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 16087.708,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 19945.627,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 107224.864,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 113646.199,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 121844.083,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 1040052.045,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 1071459.398,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 1087243.002,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 13034.379,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 16288.693,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 18816.144,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 105977.826,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 112240.259,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 121995.059,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1054525.528,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1088629.104,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1116103.724,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 13370.713,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 14926.118,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 18890.827,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 107491.893,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 114112.784,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 121919.367,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 1061179.02,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 1099703.852,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1123995.923,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 21623.376,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 22584.429,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 31302.259,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 185700.846,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 193980.937,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 197807.822,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1849798.112,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1891272.541,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1947397.69,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 21822.778,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 22813.038,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 25492.537,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 189726.081,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 195626.878,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 205195.261,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1849634.252,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1892260.94,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1903784.056,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 22704.761,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 24879.516,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 31276.977,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 195009.425,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 205740.213,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 216414.767,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1951053.979,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 1989038.922,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 2013298.162,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Prateek Ganigi",
            "username": "PG1204",
            "email": "91584519+PG1204@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "0485d022d9aabc818c8ed1589ec7854c66645af8",
          "message": "refactor(frontend): have both callers supply Validation to applyOperatorBorder (#6075)\n\n### What changes were proposed in this PR?\n\nFollow-up to #5702. `WorkflowEditorComponent.applyOperatorBorder`\ndecides an operator's border color and has two callers:\n\n- `handleOperatorValidation`: the validation-stream subscriber, which\nalready had the `Validation` and passed it in.\n- The operator-add stream subscriber: which passed no `Validation` and\nrelied on an optional-parameter fallback that recomputed it inside the\nhelper.\n\nThis PR unifies the two paths so both callers obtain the `Validation`\nthemselves and pass it in:\n\n- The operator-add subscriber now computes it via\n`validationWorkflowService.validateOperator(...)` and passes it,\nmirroring the validation-stream caller.\n- `applyOperatorBorder`'s `validation` parameter becomes **required**,\nand the `?? validateOperator(...)` fallback is removed. The color\ndecision no longer silently depends on a recompute hidden inside the\nhelper.\n\nThis is a non-functional cleanup and no behavioral change. In\nparticular, the operator-add subscriber still paints the border\nimmediately (and restores cached run statistics), preserving the\nnavigation-reset behavior from #3614.\n\nNote on scope: the issue's summary mentions \"painted exactly once.\"\nTruly collapsing to a single paint conflicts with the #3614 requirement\nthat borders render immediately on reload without waiting for validation\nevents, and would need fragile \"just-added\" state tracking in the\nvalidation handler. This PR implements the safe, behavior-preserving\nunification (consistent validation acquisition, fallback removed) rather\nthan suppressing either paint.\n\n### Any related issues, documentation, discussions?\n\nPart of #5726. \nRelated: #5702 (added the optional parameter; this issue arose from its\nreview), #5146 (introduced `applyOperatorBorder` and the #3614\nnavigation fix).\n\n### How was this PR tested?\n\nUpdated `workflow-editor.component.spec.ts`:\n- Renamed the \"uses the Validation passed in instead of recomputing it\"\ntest to \"relies solely on the passed-in Validation (never recomputes\ninside the helper)\": the old name implied a recompute path that no\nlonger exists.\n- Added \"supplies a computed Validation from the operator-add path\",\nasserting the operator-add subscriber calls `applyOperatorBorder` with a\n`Validation` object.\n- Existing border-outcome tests (green / gray / red /\ninvalid-over-cached priority) remain and continue to pass.\n\nVerified locally:\n- `tsc --noEmit`: clean\n- `prettier --check` / `eslint`: clean\n- `ng test` (jsdom): 30/30 pass in the editor spec\n- `ng run gui:test-browser`: 13/13 pass\n\n### Was this PR authored or co-authored using generative AI tooling?\nCo-authored with Claude Opus 4.8 in compliance with ASF",
          "timestamp": "2026-07-14T04:56:00Z",
          "url": "https://github.com/apache/texera/commit/0485d022d9aabc818c8ed1589ec7854c66645af8"
        },
        "date": 1784034906967,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 13355.207,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 17518.434,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 20489.278,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 75101.375,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 85818.34,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 97028.759,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 688096.405,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 725897.665,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 759923.529,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 10178.504,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 12026.783,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 14193.421,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 73698.896,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 84158.376,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 161845.725,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 686995.118,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 732631.775,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 759394.331,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 9880.539,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 13754.078,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 16038.123,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 71869.134,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 78538.403,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 96274.946,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 688985.796,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 730335.192,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 758038.376,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 12037.17,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 17935.078,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 20212.989,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 92856.172,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 99775.198,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 109904.749,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 891147.887,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 934699.331,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 970601.343,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 11851.363,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 14823.383,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 27788.542,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 89419.797,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 98488.908,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 112701.826,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 888594.424,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 944214.827,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 975743.787,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 12003.905,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 14458.49,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 17945.016,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 93481.702,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 101383.549,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 117924.559,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 910884.744,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 967942.244,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1005429.505,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 21233.5,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 24608.239,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 38128.125,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 178754.037,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 191057.41,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 276525.387,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1715360.271,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1789974.745,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1822495.066,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 20782.568,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 22059.232,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 27841.351,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 173326.403,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 184672.401,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 201008.827,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1727549.969,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1794109.551,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1855394.895,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 20559.358,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 22669.637,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 29357.656,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 180448.401,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 191283.365,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 227659.851,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1813639.446,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 2104916.45,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 2177255.129,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "aa0bc44a86f9b8ac5d6ccab6dd767c669057d86f",
          "message": "chore(deps, pyright-language-service): bump @types/node from 26.1.0 to 26.1.1 in /pyright-language-service in the pyright-patch group (#6404)\n\nBumps the pyright-patch group in /pyright-language-service with 1\nupdate:\n[@types/node](https://github.com/DefinitelyTyped/DefinitelyTyped/tree/HEAD/types/node).\n\nUpdates `@types/node` from 26.1.0 to 26.1.1\n<details>\n<summary>Commits</summary>\n<ul>\n<li>See full diff in <a\nhref=\"https://github.com/DefinitelyTyped/DefinitelyTyped/commits/HEAD/types/node\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility\nscore](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=@types/node&package-manager=npm_and_yarn&previous-version=26.1.0&new-version=26.1.1)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't\nalter it yourself. You can also trigger a rebase manually by commenting\n`@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits\nthat have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all\nof the ignore conditions of the specified dependency\n- `@dependabot ignore <dependency name> major version` will close this\ngroup update PR and stop Dependabot creating any more for the specific\ndependency's major version (unless you unignore this specific\ndependency's major version or upgrade to it yourself)\n- `@dependabot ignore <dependency name> minor version` will close this\ngroup update PR and stop Dependabot creating any more for the specific\ndependency's minor version (unless you unignore this specific\ndependency's minor version or upgrade to it yourself)\n- `@dependabot ignore <dependency name>` will close this group update PR\nand stop Dependabot creating any more for the specific dependency\n(unless you unignore this specific dependency or upgrade to it yourself)\n- `@dependabot unignore <dependency name>` will remove all of the ignore\nconditions of the specified dependency\n- `@dependabot unignore <dependency name> <ignore condition>` will\nremove the ignore condition of the specified dependency and ignore\nconditions\n\n\n</details>\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>\nCo-authored-by: Xinyuan Lin <xinyual3@uci.edu>",
          "timestamp": "2026-07-15T08:27:39Z",
          "url": "https://github.com/apache/texera/commit/aa0bc44a86f9b8ac5d6ccab6dd767c669057d86f"
        },
        "date": 1784121408864,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 13578.608,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 19497.062,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 21503.174,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 75872.475,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 84322.177,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 96143.43,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 721474.155,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 742533.159,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 774667.571,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 9997.042,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 12687.087,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 16073.468,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 73330.666,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 77430.727,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 94155.625,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 708867.413,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 730490.641,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 745129.028,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 9709.562,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 13055.871,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 16367.348,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 74354.312,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 77592.812,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 82289.47,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 713806.583,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 734976.246,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 763039.273,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 11707.897,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 15448.418,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 18599.403,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 92042.18,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 97968.718,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 114893.69,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 895069.294,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 915939.569,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 954973.989,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 11863.603,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 13769.589,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 18465.437,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 92911.232,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 96597.868,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 100678.343,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 902185.203,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 925435.007,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 995881.965,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 11925.792,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 13831.227,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 21862.686,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 93910.144,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 98653.762,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 112875.892,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 919434.924,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 964413.442,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1001343.128,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 20661.281,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 21584.266,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 28465.642,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 173134.455,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 178511.381,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 201460.279,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1679656.488,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1806757.223,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1841728.906,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 20163.076,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 20961.364,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 24037.879,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 174337.373,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 178561.1,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 186877.985,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1693450.672,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1731590.24,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1769022.134,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 21482.498,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 25879.527,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 28517.126,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 183962.542,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 192732.77,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 207668.709,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1804471.472,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 1845561.515,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 1856909.882,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "c7473a1d8c8bf68f5cfab4ec4968982173ac5ae2",
          "message": "fix(deps): bump sttp client core to 4.0.26 (#6408)\n\nUpdates `com.softwaremill.sttp.client4:core_2.13` from `4.0.25` to\n`4.0.26`.\n\nNotes:\n- The `org.postgresql:postgresql` update was removed from this PR.\n- `project/plugins.sbt` keeps pgjdbc pinned at `42.7.4` for jOOQ 3.19.x\ncodegen compatibility, as documented in that file.\n- `computing-unit-managing-service/LICENSE-binary` was updated to match\nthe bundled sttp `4.0.26` jar.\n\nValidation:\n- `bin/licensing/check_binary_deps.py --ignore-transitive-version jar\n--license-binary computing-unit-managing-service/LICENSE-binary ...`\npasses locally for the service jar set.\n- GitHub Actions `build / platform (computing-unit-managing-service)`\npassed on commit `986cf34a99`.\n- Required Checks passed on commit `986cf34a99`.\n\n---------\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>\nCo-authored-by: Xinyuan Lin <xinyual3@uci.edu>",
          "timestamp": "2026-07-16T00:45:12Z",
          "url": "https://github.com/apache/texera/commit/c7473a1d8c8bf68f5cfab4ec4968982173ac5ae2"
        },
        "date": 1784208084383,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 15225.059,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 21453.046,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 30156.299,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 78141.406,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 84772.999,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 104842.974,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 700833.611,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 736906.971,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 781332.434,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 10831.773,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 13625.529,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 15123.933,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 73330.324,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 80270.109,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 91636.734,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 697742.499,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 744202.946,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 757755.776,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 10119.524,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 13446.289,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 16939.254,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 73427.017,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 80713.479,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 95347.855,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 705205.371,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 749137.936,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 770467.583,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 12346.48,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 15852.254,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 18691.388,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 95053.529,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 102431.59,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 107171.842,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 908286.776,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 956254.197,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 972308.619,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 11826.031,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 13681.786,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 15198.8,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 92601.785,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 100237.272,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 113002.754,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 901610.154,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 955658.54,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 986959.152,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 11916.768,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 13811.724,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 16782.518,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 92693.055,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 101911.259,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 112527.6,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 902135.104,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 956896.528,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1005278.993,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 20781.86,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 22187.499,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 29379.433,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 173513.076,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 181521.307,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 195556.833,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1727871.771,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1803720.522,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1826856.611,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 20101.988,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 20749.264,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 24783.044,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 172663.15,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 180516.452,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 188228.21,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1733967.107,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1791279.722,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1819436.824,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 20536.338,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 24666.312,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 27500.949,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 183682.222,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 194497.587,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 202849.001,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1782940.528,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 1837566.36,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 1861785.376,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "ali risheh",
            "username": "aicam",
            "email": "ali.risheh876@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "ee062a20d3777d1409508766dba18187e3719035",
          "message": "refactor(k8s): consolidate gateway resources under the gateway folder (#6472)\n\n### What changes were proposed in this PR?\n\nA small, behavior-preserving refactor of the Helm chart layout under\n`bin/k8s`.\n\nThe agent-service **`BackendTrafficPolicy`** is a gateway-layer [Envoy\nGateway](https://gateway.envoyproxy.io/) resource — it configures\nconsistent-hash load balancing on the gateway for the agent-service\n`HTTPRoute` (so all requests for a given workflow pin to the same\nreplica). Despite being a gateway concern, it was living under\n`templates/base/agent-service/` next to the app-tier\n`Deployment`/`Service`/`Secret`.\n\nIt is the same class of resource as the `SecurityPolicy`\n(`gateway-security-policy.yaml`) that already lives in\n`templates/base/gateway/`, so this PR moves it there and renames it to\nthe folder's `gateway-*` convention:\n\n```\ntemplates/base/agent-service/agent-service-backend-traffic-policy.yaml\n  -> templates/base/gateway/gateway-agent-traffic-policy.yaml\n```\n\nAfter this move, **every** gateway-related resource (`GatewayClass`,\n`Gateway`, `HTTPRoute`, `Backend`, `SecurityPolicy`,\n`BackendTrafficPolicy`) lives under `templates/base/gateway/`, and no\ngateway resource remains scattered in a service folder (verified by\ngrepping all templates for the `gateway.envoyproxy.io` /\n`gateway.networking.k8s.io` API groups — this was the only stray one).\n\nThis is purely organizational: no values, resource names, gating\nconditions, or rendered output change. It continues the\n`base`/`aws`/`on-prem` template reorganization from #5757.\n\n### Any related issues, documentation, discussions?\n\nPart of the deployment-unification effort tracked in #5891 (housekeeping\nfollow-up to the template reorg in #5757). No functional dependency on\nthe object-storage work (#5932, #6295).\n\n### How was this PR tested?\n\nVerified the move is byte-for-byte behavior-preserving by diffing the\nrendered manifests before and after:\n\n```bash\ncd bin/k8s\nhelm dependency build   # fetch subcharts\n\n# render with the file in its OLD location\nhelm template test . > /tmp/before.txt\n\n# render with the file in its NEW location (this PR)\nhelm template test . > /tmp/after.txt\n\ndiff /tmp/before.txt /tmp/after.txt\n```\n\nThe only differences are:\n1. The `# Source:` provenance comment path (`.../agent-service/...` →\n`.../gateway/...`) — expected, cosmetic.\n2. The auto-generated `encryptionKey` value — Helm regenerates this\nrandom secret on every invocation; unrelated to this change.\n\nThe `BackendTrafficPolicy` resource itself (name\n`<release>-agent-service-traffic-policy`, its `targetRefs`, and the\n`X-Agent-Workflow-Id` consistent-hash config) renders identically. `helm\ntemplate` succeeds for the default value set.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Claude Opus 4.8)\n\nCo-authored-by: Claude Opus 4.8 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-07-17T07:19:16Z",
          "url": "https://github.com/apache/texera/commit/ee062a20d3777d1409508766dba18187e3719035"
        },
        "date": 1784294037699,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 14289.908,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 18960.041,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 21968.899,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 75363.847,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 81838.605,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 89333.13,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 685336.71,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 733510.379,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 782399.726,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 10779.108,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 12873.792,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 15384.737,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 71689.077,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 77921.513,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 95693.909,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 693331.788,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 736683.875,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 781578.372,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 10317.934,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 13712.361,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 18582.953,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 72140.424,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 77536.994,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 81118.852,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 687273.283,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 738085.756,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 831661.15,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 12929.379,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 18977.886,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 22241.815,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 93549.928,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 104251.586,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 113056.398,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 907526.346,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 959580.593,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 990162.808,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 12355.571,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 14954.261,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 17969.45,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 93538.981,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 100930.021,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 129756.483,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 904874.433,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 951568.634,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 982155.301,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 12161.547,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 14114.611,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 17226.143,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 94243.358,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 101607.72,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 119651.562,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 915974.951,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 977907.314,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1015330.902,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 19888.725,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 21320.143,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 27281.531,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 182768.466,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 197218.915,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 208064.903,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1717822.808,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1813614.986,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1841521.721,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 20472.795,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 21277.123,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 23379.248,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 171902.244,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 179436.165,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 202738.639,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1711605.517,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1775556.326,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1807646.909,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 20340.389,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 24389.789,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 27709.51,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 178612.378,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 189896.961,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 203724.731,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1804330.017,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 1864043.977,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 1907669.601,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Matthew B.",
            "username": "Ma77Ball",
            "email": "mgball@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "39db110e74b4bdcf0e4b0df961c25973e6bbb20c",
          "message": "test(frontend): add unit tests for AuthService (#6470)\n\n### What changes were proposed in this PR?\n- Adds auth.service.spec.ts covering AuthService; no production code\nchanged.\n- Verifies the static access-token helpers and the login, register and\nGoogle HTTP endpoints.\n- Covers logout and the loginWithExistingToken branches (no token,\nexpired, valid, invite-only inactive).\n### Any related issues, documentation, discussions?\nCloses: #6458\n### How was this PR tested?\n- Run `cd frontend && npx nx test gui\n--include=\"**/auth.service.spec.ts\"`, expect 10 passed.\n- Full frontend suite runs in CI via `yarn test:ci`, selected by the\nauto-applied `frontend` label.\n### Was this PR authored or co-authored using generative AI tooling?\nCo-authored with Claude Opus 4.8 in compliance with ASF",
          "timestamp": "2026-07-18T04:00:53Z",
          "url": "https://github.com/apache/texera/commit/39db110e74b4bdcf0e4b0df961c25973e6bbb20c"
        },
        "date": 1784380402888,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 16048.698,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 20080.67,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 25909.013,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 93432.181,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 103220.499,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 110916.714,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 863437.045,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 896481.257,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 902526.262,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 12483.176,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 16732.631,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 17172.595,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 90654.541,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 96874.358,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 106481.227,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 863626.614,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 894673.212,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 925192.162,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 12074.655,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 14284.999,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 16318.4,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 90362.753,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 96081.652,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 109746.164,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 867026.44,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 897560.35,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 906335.247,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 14191.911,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 16730.528,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 21542.1,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 112553.643,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 122491.866,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 130213.193,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 1081208.812,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 1121764.221,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 1149921.916,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 13875.23,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 19573.597,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 20499.733,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 111853.236,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 116534.502,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 123356.705,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1090153.415,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1135023.231,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1161873.676,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 13984.389,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 16482.343,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 20109.246,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 112694.041,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 121533.445,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 131997.825,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 1113315.453,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 1148657.818,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1198581.691,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 23552.82,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 24631.593,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 30945.251,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 194642.299,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 202983.845,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 228641.59,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1943080.088,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 2002797.632,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 2085745.581,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 23719.685,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 24552.549,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 26051.347,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 201526.731,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 209025.16,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 225897.096,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1966887.39,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 2072543.573,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 2124434.195,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 24311.487,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 28110.676,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 33233.08,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 207711.697,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 231565.273,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 245864.345,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 2050241.01,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 2094464.922,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 2122028.39,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "80c45be89dbbc64abafe0f70a1c05ef7117fbaa8",
          "message": "fix(amber): run Iceberg local storage on Windows in the Python worker (#6545)\n\n### What changes were proposed in this PR?\n\nFollow-up to #6488, which made Iceberg local storage work on Windows\nwithout\nwinutils **on the Scala/JVM side only** (`IcebergUtil` +\n`WinutilsFreeLocalFileSystem`).\nThe Python UDF worker writes its output-port Iceberg storage through its\n**own**\npyiceberg path — `core/storage/iceberg/iceberg_document.py` and\n`document_factory.py`\ncall `IcebergCatalogInstance.get_instance()` → `create_postgres_catalog`\ninside the\nPython process — so #6488 does not reach it. On a Windows dev machine\nwith a\nlocal-filesystem warehouse (`postgres` catalog type), that path fails\ntwo ways:\n\n| # | Symptom | Root cause |\n|---|---|---|\n| 1 | `ValueError: Unrecognized filesystem type in URI: c` at table\ncreation | a bare drive path `C:\\...` is parsed by pyiceberg as URI\nscheme `c` |\n| 2 | `OSError: [WinError 123] ... The filename, directory name, or\nvolume label syntax is incorrect` | even a `file:///C:/...` URI is\nrejected by pyiceberg's default **pyarrow** FileIO (`/C:/...`) |\n\nFix (in `iceberg_utils.py`, mirroring #6488's tight, self-gating\napproach):\n\n| Change | Detail |\n|---|---|\n| `_is_windows_local_warehouse(warehouse)` (new) | True only when the\nwarehouse carries a **Windows drive letter** — bare (`C:\\...`, `C:/...`)\nor `file:///C:/...`. Excludes POSIX paths, colon-stripped paths\n(`C/...`), and remote object stores (`s3://...`). |\n| `_to_file_uri(warehouse)` (new) | Normalizes a bare drive path to a\n`file:///` URI via `PureWindowsPath.as_posix()` — not `.as_uri()`, which\nwould percent-encode a space to `%20` (deterministic on every host OS);\nalready-URI values pass through. |\n| `create_postgres_catalog` | When (and only when) the warehouse is\nWindows-local, register the normalized `file:///` URI and select\n`pyiceberg.io.fsspec.FsspecFileIO`, whose `LocalFileSystem` handles\nWindows drive paths. |\n| `create_rest_catalog` | Unchanged — its `warehouse_name` is a logical\nidentifier resolved server-side via `S3FileIO`, not a local path\n(checked, exempt). |\n\nThe gate is deliberately narrow so the fix cannot alter the\ncross-runtime\nwarehouse convention that Linux, macOS, CI, and the Scala engine rely\non: PyIceberg\npersists the `warehouse` value into table metadata, so a `postgres`\ncatalog with a\nPOSIX or remote warehouse must keep the plain-path / default-FileIO\nbehavior\n(regression fixed in #4409). Only genuine Windows drive-letter\nwarehouses are\nnormalized.\n\nThe drive path is normalized with `PureWindowsPath.as_posix()` (not\n`.as_uri()`)\nso a warehouse containing a space — e.g. `C:\\Users\\John Doe\\...`,\n`C:\\Program\nFiles\\...`, OneDrive-redirected profiles — keeps a **raw** space rather\nthan\n`%20`; fsspec's `LocalFileSystem` does not URL-decode, so a `%20` would\nwrite\ninto a literally `%20`-named directory.\n\nScope note: on Windows the two runtimes register different warehouse\nstrings into\nthe shared `postgres` catalog — the Scala side its existing\ncolon-stripped path\n(`C/Users/...`), the Python worker the absolute `file:///C:/...` URI.\nThis is fine\nfor the actual data flow here (a Python UDF's output-port table is\nwritten by the\nPython worker and read back by the engine/result service via the\nabsolute metadata\npointer, verified end-to-end); reconciling the Scala side's\ncolon-stripped\nconvention is out of scope for this fix and would belong with #6488's\nfollow-ups.\n\nBefore → after:\n\n| Environment (`postgres` catalog) | Before | After |\n|---|---|---|\n| Windows, local warehouse (`C:\\...`) | worker crashes at first table\ncreate | works |\n| Linux / macOS / CI, local warehouse (`/tmp/...`) | works | unchanged\n(gate off) |\n| Any host, remote warehouse (`s3://...`) | works | unchanged (gate off)\n|\n| `rest` catalog | unaffected (`S3FileIO`) | unaffected |\n\n### Any related issues, documentation, discussions?\n\nFollow-up to #6488 (Scala/JVM side); closes the Python-worker half of\n#6487.\nPreserves the cross-runtime warehouse invariant from #4409.\n\n### How was this PR tested?\n\n- New `TestCreatePostgresCatalogWindowsLocal` in\n`test_iceberg_utils_catalog.py`\n(written first, TDD): a Windows drive-letter warehouse is normalized to\na\n`file:///` URI and `FsspecFileIO` is selected; POSIX, colon-stripped,\nand\nremote (`s3://`) warehouses are left untouched (no FileIO override). The\ntests\nassert on the computed `SqlCatalog` props and use `PureWindowsPath`, so\nthey are\n  OS-independent and pass on Linux CI.\n- The pre-existing `TestCreatePostgresCatalog` tests (the #4409\nplain-path\n  invariant) still pass unchanged.\n- Verified end-to-end on a real Windows filesystem: a bare `C:\\...`\nwarehouse\nfed through the productionized helpers into a real catalog creates a\ntable and\nround-trips rows (both failure modes above reproduced on stock `main`\nfirst).\n- `amber/` `ruff check` and `ruff format --check` pass on the changed\nfiles.\n(The `test_iceberg_document.py` / REST integration tests require a live\npostgres / REST catalog server and are environment-gated locally; they\nare\nunaffected by this change — confirmed identical pass/fail with the diff\nstashed.)\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-19T06:02:30Z",
          "url": "https://github.com/apache/texera/commit/80c45be89dbbc64abafe0f70a1c05ef7117fbaa8"
        },
        "date": 1784466376998,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 15164.347,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 19060.507,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 22461.841,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 74537.731,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 83165.586,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 87906.374,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 707035.795,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 749909.235,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 765496.905,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 10439.818,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 12416.877,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 14731.124,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 73692.485,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 80692.711,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 87505.296,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 703567.479,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 748843.381,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 760252.236,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 10929.004,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 12795.505,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 15587.598,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 73700.322,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 81092.488,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 87589.82,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 718082.982,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 754323.972,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 801066.67,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 12733.218,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 15760.783,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 18403.935,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 94653.622,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 104851.113,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 143983.367,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 889946.376,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 939988.411,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 971948.59,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 12700.113,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 13905.639,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 16827.561,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 96972.402,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 103503.139,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 117884.186,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 930049.054,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 975247.847,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 992551.225,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 12334.702,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 16374.919,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 18483.826,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 95791.796,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 102715.877,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 148498.169,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 915614.376,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 976667.071,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1026294.015,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 21566.521,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 24239.795,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 30689.08,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 173105.419,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 183082.409,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 205423.01,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1745626.069,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1825476.659,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1863315.618,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 21534.708,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 23543.745,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 29687.31,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 177086.571,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 190871.228,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 220246.116,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1738777.577,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1793437.736,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1828107.559,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 21229.204,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 24450.438,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 26239.472,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 183444.524,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 198650.862,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 217205.158,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1794003.815,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 1889328.695,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 1918459.813,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "c543d7796a027382828648826266a0e45232934c",
          "message": "test(frontend): extend WorkflowGraph unit test coverage (#6586)\n\n### What changes were proposed in this PR?\n\nExtends the existing `workflow-graph.spec.ts` with 44 new tests (24 ->\n68) for the `TexeraGraph` in-memory model, targeting\npreviously-uncovered behavior:\n- operator/link/port add-remove-get and their guard/throw branches;\n- disabled / view-result / reuse-cache state toggles and their getters\n(incl. sink-ignore and idempotent no-ops);\n- comment-box create/edit/delete and the `assert*` existence helpers;\n- operator debug-state lifecycle;\n- `getSubDAG` (full DAG from terminals, targeted root,\ndisabled-operator/link exclusion, visited dedup on a diamond);\n- `setPortProperty`; and every event-stream getter.\n\nReuses the existing direct-construction setup; adds an `afterEach` that\ntears down the shared model. Statement coverage for the file went from\n262 uncovered lines to 18 (the remainder are unreachable defensive\nbranches). No existing tests modified.\n\n### Any related issues, documentation, discussions?\n\nCloses #6583.\n\n### How was this PR tested?\n\n`npx ng test --watch=false --include='**/workflow-graph.spec.ts'` ->\n68/68 passing. `yarn format:ci` passes.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-20T07:38:03Z",
          "url": "https://github.com/apache/texera/commit/c543d7796a027382828648826266a0e45232934c"
        },
        "date": 1784554754803,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 14806.214,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 17307.901,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 20698.902,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 94293.249,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 104118.409,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 111289.537,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 884671.08,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 924498.593,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 933913.935,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 12376.734,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 16706.616,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 22490.233,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 91470.26,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 98596.182,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 105347.736,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 873710.916,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 905886.028,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 921213.596,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 11473.986,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 14126.464,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 18071.227,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 91947.535,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 101127.711,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 116873.002,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 892105.464,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 929267.508,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 936227.941,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 14167.186,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 19379.323,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 21296.902,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 111988.393,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 121672.139,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 134869.263,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 1137545.214,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 1167136.657,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 1180231.102,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 14133.402,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 18069.721,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 26149.985,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 116313.95,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 119437.215,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 124617.213,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1124575.117,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1155958.001,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1179112.226,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 14228.671,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 16139.44,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 20599.278,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 117746.429,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 122146.417,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 132339.23,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 1117737.678,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 1161283.875,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1188031.978,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 23025.782,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 25999.762,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 30288.436,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 196052.176,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 207765.84,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 224808.603,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1969077.871,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 2020202.419,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 2056124.858,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 22840.642,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 23779,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 24496.486,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 198797.789,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 203918.836,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 215798.503,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1959724.547,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 2020078.523,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 2056232.832,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 24980.09,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 28956.199,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 33881.27,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 210564.848,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 222501.121,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 254103.369,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 2083995.882,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 2133069.707,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 2151193.851,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Ryan Zhang",
            "username": "zyratlo",
            "email": "97552093+zyratlo@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "0467c76529981b3951bceb33eedf6bcc776f16a3",
          "message": "feat(python-notebook-migration): add notebook migration orchestration service (#5262)\n\n### What changes were proposed in this PR?\nIntroduces `NotebookMigrationService`, the frontend orchestration\nservice that sits between the migration-tool UI and the lower layers:\nthe LLM client (`migration-tool-llm-client`) and the backend\nnotebook-migration microservice\n(`migration-tool-backend-notebook-migration-service`).\n\n**`notebook-migration.service.ts`**\n- `getAvailableModels()` — `GET /api/models` against the existing\nLiteLLM proxy, returns the model dropdown options.\n- `sendToAIGenerateWorkflow(notebook, modelType)` — drives the full\n`NotebookMigrationLLM` lifecycle (initialize → verify connection →\nconvert → close in `finally`) and returns `{ workflowContent,\nmappingContent }`.\n- `sendNotebookToJupyter(notebookData)` — `POST\n/api/notebook-migration/set-notebook`; surfaces a `NotificationService`\ntoast on success and failure; returns `1` / `0`.\n- `getJupyterURL()`, `getJupyterIframeURL()` — calls the matching\nmicroservice endpoints to retrieve URLs to embed.\n- `storeNotebookAndMapping(wid, vid, mappingContent, notebookContent)` —\n`POST /api/notebook-migration/store-notebook-and-mapping`; returns the\n`HttpClient` observable directly so callers can compose with\n`switchMap`.\n- Mapping cache — small in-memory dictionary `{ [key: string]:\nMappingContent }` keyed by `mapping_wid_<workflowId>`, with\n`hasMapping`, `getMapping`, `setMapping`, `deleteMapping`.\n\n  **`notebook-migration.service.spec.ts`**\n- `getAvailableModels`: maps the LiteLLM `data[].id` array correctly;\nfalls back to an empty array on HTTP error.\n- `sendNotebookToJupyter`: success → returns `1`; error → returns `0`\nand toasts.\n- `getJupyterURL` / `getJupyterIframeURL`: success → returns the URL;\nnon-OK response or thrown error → returns `null`.\n- Mapping cache: `setMapping` then `getMapping` round-trips;\n`deleteMapping` removes the entry.\n- `storeNotebookAndMapping`: makes the expected `POST` to the\npersistence endpoint.\n\n\n### Any related issues, documentation, discussions?\nCloses #5261 \nParent issue #4301 \n\n\n### How was this PR tested?\nThe new `notebook-migration.service.spec.ts` adds\n`HttpClientTestingModule`-driven test cases\n\n\n\n### Was this PR authored or co-authored using generative AI tooling?\nGenerated-by: Claude Code (Claude Opus 4.7)",
          "timestamp": "2026-07-20T23:11:48Z",
          "url": "https://github.com/apache/texera/commit/0467c76529981b3951bceb33eedf6bcc776f16a3"
        },
        "date": 1784640034790,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 14161.101,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 17439.393,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 19607.678,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 74592.355,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 82809.507,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 93575.282,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 696282.766,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 734662.367,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 752868.07,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 11410.254,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 14754.491,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 16041.681,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 74846.796,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 80445.792,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 83262.282,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 697868.128,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 733868.87,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 745749.008,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 10248.617,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 14027.199,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 15675.378,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 71966.184,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 78698.602,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 90891.421,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 700425.1,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 742251.981,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 779785.346,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 12256.384,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 16489.025,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 17801.83,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 92742.035,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 99087.532,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 106861.044,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 893835.237,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 937116.382,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 973008.397,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 11883.621,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 16736.52,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 17893.13,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 91918.701,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 97844.991,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 105973.452,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 894787.521,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 953589.582,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 988019.202,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 12311.348,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 14462.026,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 18008.706,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 94408.702,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 102946.091,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 110984.066,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 926141.531,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 988227.246,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1012416.22,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 20840.093,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 21931.574,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 27052.654,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 171729.395,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 181622.45,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 197825.972,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1735073.032,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1792936.483,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1829384.677,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 20612.521,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 21346.317,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 23292.493,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 172517.427,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 181212.518,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 186628.074,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1733762.155,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1783495.121,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1812128.704,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 21844.107,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 25520.676,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 30724.488,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 179841.801,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 190477.913,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 197724.076,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1844258.467,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 2058862.401,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 2252341.274,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "a9f384eab44c8e1595792e02b32c2c1bfc16e77e",
          "message": "test(frontend): extend computing-unit util unit test coverage (#6738)\n\n### What changes were proposed in this PR?\n\nExtends `computing-unit.util.spec.ts` with 15 tests (34 -> 49) covering\nthe ComputingUnitMetadataComponent render branches (owner vs\naccess-privilege, present/empty gpuLimit), the cpu/memory\nresource-conversion unknown-unit and unitless fallbacks, the\n`memoryPercentage` non-positive-limit guard, and the JVM memory-slider /\n`memoryToGb` Mi/floor/fallback paths. Coverage -> ~100%. No existing\ntests modified.\n\n### Any related issues, documentation, discussions?\n\nCloses #6731.\n\n### How was this PR tested?\n\n`ng test --include='**/computing-unit.util.spec.ts'` -> 49/49 passing.\n`yarn format:ci` passes.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-22T05:39:27Z",
          "url": "https://github.com/apache/texera/commit/a9f384eab44c8e1595792e02b32c2c1bfc16e77e"
        },
        "date": 1784729839516,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 15275.001,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 20416.495,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 24835.052,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 89211.104,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 97664.007,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 107575.273,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 838389.256,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 874552.319,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 906749.611,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 11552.092,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 13159.807,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 15166.183,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 84784.293,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 91254.074,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 104672.92,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 836075.374,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 872014.595,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 890921.376,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 11810.382,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 13579.286,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 15725.679,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 86088.632,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 93466.085,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 102850.24,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 828037.533,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 872701.264,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 892885.584,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 13379.661,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 18556.543,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 19826.564,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 107027.021,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 114343.47,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 123673.758,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 1047984.018,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 1084607.334,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 1102865.07,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 12690.668,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 15906.65,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 18976.266,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 106933.734,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 113381.576,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 127216.361,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1041164.34,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1083656.506,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1104923.557,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 12913.108,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 14878.023,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 19215.583,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 105695.722,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 112683.363,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 125085.402,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 1053953.248,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 1100175.216,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1116588.915,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 21472.724,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 23278.027,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 34192.642,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 186832.853,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 194992.91,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 209237.5,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1849486.55,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1901579.976,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1968153.53,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 21669.263,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 22548.942,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 24396.515,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 187994.038,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 195606.458,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 216187.847,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1851820.14,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1898021.764,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1925470.372,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 21862.179,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 25381.908,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 29453.114,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 194075.821,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 202774.321,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 217095.552,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1936299.495,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 1985071.177,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 2002761.296,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Xinyuan Lin",
            "username": "aglinxinyuan",
            "email": "xinyual3@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "dda9e830f54e0fa74dcf0fd5d41a3c2e1a0c989c",
          "message": "ci: discover staged Codecov flags instead of a hardcoded matrix (#6824)\n\n### What changes were proposed in this PR?\n\nFollow-up to #6730. The deferred `Codecov Upload` workflow hardcoded all\n11 flags in its matrix and tried to download each from the source run.\nOn a label-gated PR — e.g. a frontend-only PR that only stages\n`codecov-frontend` — the other 10 legs hit `##[error] Artifact not found\nfor name: codecov-<flag>`, which looks like a name mismatch; and because\nthe download used `continue-on-error: true`, a genuinely failed download\nwas swallowed, the leg went green, and `notify-failure` never fired.\n\n**Fix:** add a `discover` job that lists the source run's *actual*\n`codecov-*` artifacts and drives the upload matrix from that\n(`fromJSON`), and drop `continue-on-error` on the download.\n\n- **No more spurious errors** — only the flags the source run actually\nbuilt are processed; a frontend-only PR runs one clean leg.\n- **Failures surface** — a listed artifact that then fails to download\nor upload fails the leg, so `notify-failure` fires (previously it went\nsilently green).\n- Also removes the hardcoded flag list, so adding a `platform` service\nno longer needs a matching matrix row here.\n\nObserved on the first live run after #6730 merged ([run\n29984668372](https://github.com/apache/texera/actions/runs/29984668372),\ntriggered by a frontend-only PR's checks): `frontend` uploaded fine, but\n10 legs logged `Artifact not found` for the label-gated flags and the\nrun still went green.\n\n### Any related issues, documentation, discussions?\n\nFollow-up to #6730 (reported on #6685).\n\n### How was this PR tested?\n\n- `workflow_run` only activates from the copy of the file on the default\nbranch, so this can't run on its own PR. Validated by YAML lint and an\nadversarial review of the discover → upload → notify flow: empty-flags\ncleanly skips (no failure, no notify), the dynamic `fromJSON` matrix\nhandles one or many flags, a download/upload failure now fails the leg\nand fires `notify-failure`, `amber-integration` (results-only) skips the\ncoverage step via `hashFiles`, and the `workflow_dispatch` path resolves\nthe run id.\n- The `workflow_dispatch` (`run_id`) entry remains for manual post-merge\nvalidation against a finished *Required Checks* run.\n\n### Was this PR authored or co-authored using generative AI tooling?\n\nYes.\n\nGenerated-by: Claude Code (Opus 4.8 [1M context])",
          "timestamp": "2026-07-23T11:58:13Z",
          "url": "https://github.com/apache/texera/commit/dda9e830f54e0fa74dcf0fd5d41a3c2e1a0c989c"
        },
        "date": 1784812820678,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 12917.541,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 17405.778,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 21867.72,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 74839.517,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 83910.82,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 91256.333,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 698548.277,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 743240.633,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 760054.654,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 10242.143,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 14579.752,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 16667.747,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 74066.513,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 80108.621,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 91529.576,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 696558.881,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 740126.81,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 754175.913,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 9737.184,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 13139.7,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 13914.705,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 71998.95,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 78805.121,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 92960.708,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 710534.417,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 755029.195,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 788701.324,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 12164.905,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 17318.65,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 21297.58,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 91333.581,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 98559.699,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 110406.805,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 888634.172,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 937134.277,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 957886.097,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 11517.609,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 15255.982,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 17330.528,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 93573.359,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 99332.465,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 99885.232,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 899140.712,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 943000.374,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1003572.097,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 11766.577,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 16620.229,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 17526.688,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 95105.051,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 100549.607,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 110696.601,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 903790.756,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 962352.36,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 993165.299,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 19727.311,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 20905.512,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 28211.693,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 169533.647,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 179606.692,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 203161.749,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1700887.93,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1761253.319,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1785393.033,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 20758.162,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 21898.257,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 24829.398,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 174004.939,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 181609.962,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 192810.883,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1733796.2,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1799940.619,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1834129.358,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 21034.478,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 24828.663,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 29939.484,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 180865.487,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 195641.995,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 209959.092,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1802408.086,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 1848264.419,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 1868597.841,
            "unit": "us"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Matthew B.",
            "username": "Ma77Ball",
            "email": "mgball@uci.edu"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "a351f44fbf08920d7d9dfe806614ffc4c8ed5a83",
          "message": "test(frontend): cover WorkflowUtilService untested helpers (#6689)\n\n### What changes were proposed in this PR?\n- Add unit tests for updateOperatorVersion covering port rebuild, port\npreservation, and the unknown-type error.\n- Add unit tests for the static parseWorkflowInfo string parsing and\nobject passthrough, and for getNewCommentBox default position and unique\nids.\n### Any related issues, documentation, discussions?\nCloses: #6688\n### How was this PR tested?\n- Run: `cd frontend && node --max-old-space-size=8192\n./node_modules/nx/dist/bin/nx.js test gui --watch=false\n--include=src/app/workspace/service/workflow-graph/util/workflow-util.service.spec.ts`,\nexpect the suite passing.\n- Test-only change; no production code is modified.\n### Was this PR authored or co-authored using generative AI tooling?\nCo-authored with Claude Opus 4.8 in compliance with ASF\n\n---------\n\nCo-authored-by: Claude Opus 4.8 <noreply@anthropic.com>\nCo-authored-by: Xinyuan Lin <xinyual3@uci.edu>",
          "timestamp": "2026-07-24T07:20:06Z",
          "url": "https://github.com/apache/texera/commit/a351f44fbf08920d7d9dfe806614ffc4c8ed5a83"
        },
        "date": 1784903055592,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "latency p50 / bs=10 sw=1 sl=8",
            "value": 14286.86,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=8",
            "value": 17937.452,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=8",
            "value": 22315.477,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=8",
            "value": 88393.781,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=8",
            "value": 95473.791,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=8",
            "value": 101386.794,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=8",
            "value": 829372.694,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=8",
            "value": 862364.884,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=8",
            "value": 912206.798,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=64",
            "value": 11188.834,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=64",
            "value": 13710.253,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=64",
            "value": 15888.617,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=64",
            "value": 84611.759,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=64",
            "value": 91138.563,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=64",
            "value": 103563.84,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=64",
            "value": 836051.499,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=64",
            "value": 871849.357,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=64",
            "value": 883642.616,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=1 sl=512",
            "value": 11144.043,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=1 sl=512",
            "value": 14634.31,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=1 sl=512",
            "value": 15763.317,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=1 sl=512",
            "value": 86226.718,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=1 sl=512",
            "value": 91018.454,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=1 sl=512",
            "value": 94085.924,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=1 sl=512",
            "value": 837418.097,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=1 sl=512",
            "value": 874327.158,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=1 sl=512",
            "value": 885896.44,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=8",
            "value": 12918.883,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=8",
            "value": 18014.367,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=8",
            "value": 20390.308,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=8",
            "value": 105088.283,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=8",
            "value": 112677.184,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=8",
            "value": 122548.229,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=8",
            "value": 1046236.631,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=8",
            "value": 1085029.195,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=8",
            "value": 1100919.695,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=64",
            "value": 12909.978,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=64",
            "value": 16711.447,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=64",
            "value": 20966.36,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=64",
            "value": 107524.55,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=64",
            "value": 113645.738,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=64",
            "value": 122195.975,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=64",
            "value": 1036628.668,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=64",
            "value": 1078763.912,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=64",
            "value": 1085577.883,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=10 sl=512",
            "value": 13319.224,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=10 sl=512",
            "value": 16634.894,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=10 sl=512",
            "value": 23774.472,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=10 sl=512",
            "value": 109019.204,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=10 sl=512",
            "value": 116866.113,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=10 sl=512",
            "value": 128788.997,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=10 sl=512",
            "value": 1064595.671,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=10 sl=512",
            "value": 1110374.408,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=10 sl=512",
            "value": 1152268.132,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=8",
            "value": 21308.104,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=8",
            "value": 22279.046,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=8",
            "value": 24021.11,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=8",
            "value": 186772.067,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=8",
            "value": 195343.683,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=8",
            "value": 211507.508,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=8",
            "value": 1855905.05,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=8",
            "value": 1906138.528,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=8",
            "value": 1955280.811,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=64",
            "value": 21339.916,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=64",
            "value": 22191.714,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=64",
            "value": 24133.199,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=64",
            "value": 185442.662,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=64",
            "value": 196682.518,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=64",
            "value": 237995.817,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=64",
            "value": 1864684.466,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=64",
            "value": 1923015.789,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=64",
            "value": 1977317.233,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=10 sw=50 sl=512",
            "value": 21928.292,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=10 sw=50 sl=512",
            "value": 25079.602,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=10 sw=50 sl=512",
            "value": 29762.047,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=100 sw=50 sl=512",
            "value": 194502.822,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=100 sw=50 sl=512",
            "value": 204311.861,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=100 sw=50 sl=512",
            "value": 233254.241,
            "unit": "us"
          },
          {
            "name": "latency p50 / bs=1000 sw=50 sl=512",
            "value": 1948542.903,
            "unit": "us"
          },
          {
            "name": "latency p95 / bs=1000 sw=50 sl=512",
            "value": 1987892.135,
            "unit": "us"
          },
          {
            "name": "latency p99 / bs=1000 sw=50 sl=512",
            "value": 2005088.132,
            "unit": "us"
          }
        ]
      }
    ]
  }
}
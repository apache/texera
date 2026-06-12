window.BENCHMARK_DATA = {
  "lastUpdate": 1781284964936,
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
      }
    ]
  }
}
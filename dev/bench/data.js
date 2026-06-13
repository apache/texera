window.BENCHMARK_DATA = {
  "lastUpdate": 1781389202550,
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
      }
    ]
  }
}
window.BENCHMARK_DATA = {
  "lastUpdate": 1782999506573,
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
      }
    ]
  }
}
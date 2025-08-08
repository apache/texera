# Contributing to Texera

Thank you for your interest in contributing to Texera! Please follow the steps below to submit your contributions effectively. We follow a **fork-based development workflow** and adopt the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification for commit messages and pull request titles.

---
## Different roles in the project

| Role    | Key Permissions | How to Join
| -------- | ------- | ------- |
| Contributor  | Submit issues & PRs, join discussions    | Start contributing — no formal process |
| Committer | Merge PRs, push code, vote on code changes     | Voted by PPMC based on quality contributions |
| PPMC Member | Governance, vote on releases & new committers/PPMC     | Voted  by PPMC members |
| Mentor | Guide project, oversee releases, ensure Apache policies followed     | Appointed by Incubator PMC — must be an experienced Apache member |

## 🛠 Contribution Steps

### 1. Fork the Repo
- Fork the [Texera repository](https://github.com/Texera/texera) to your own GitHub account.

### 2. Find an Existing Issue or Open an Issue
- Find an existing issue that you want to work on, or create one issue for new proposal/bug description.
- Have a discussion on the issue with Texera Committers (@Committer).
- Reach a consensus before you work on the development related to the issue.

### 3. Open a Pull Request (PR)
- Create a new branch in your fork for your contribution.
- Once you are done with the development, submit a PR from your fork to the original Texera repository.
- **Check** the option **"Allow edits from maintainers"** so that Texera Committers can make minor edits to your PR if needed.
  
#### PR Title and Commit Messages
- We require all PR titles and commit messages to follow the [Conventional Commits spec](https://www.conventionalcommits.org/en/v1.0.0/).
- All PR titles will be used as the **squashed commit message** when merged into the `master` branch.
- Example PR titles:
  - `feat: add a new join operator`
  - `fix(ui): prevent racing of requests`
  - `chore(deps): bump numpy to version 2.0.0`

> 💡 You can use the [Conventional Commits plugin](https://plugins.jetbrains.com/plugin/13389-conventional-commit) in IntelliJ to help format commit messages correctly.

#### PR Description
Your pull request description should include:

- **Purpose** of the PR:
  - If your PR addresses an issue, use `Closes #1234` to automatically close it.
  - If it relates to an issue or another PR, reference it with `#<issue_number>` or `#<PR_number>`.
- **Summary** of changes.
- Optional **design proposal** created based on the [template](https://docs.google.com/document/d/1ih6jLni4GgKETxOAlTOPjarlbeY5ccB2g9y1vK-Xhck/edit?usp=sharing).
- Optional **technical design diagram** or description.
- Optional **GIFs or screenshots** for UI-related changes.

#### Avoid Including Sensitive Information
Do not include any of the following in your PR:

- Local configuration files (e.g., `python_udf.conf`)
- Secrets or credentials (e.g., passwords, tokens)
- Build artifacts or binary files

### Final Steps Before Review
#### Your PR should pass scalafix check (lint) and scalafmt check. 
- To check lint, under `core` run command `sbt "scalafixAll --check"`; to fix lint issues, run `sbt scalafixAll`.
- To check format, under `core` run command `sbt scalafmtCheckAll`; to fix format, run `sbt scalafmtAll`. 
- When you need to execute both, scalafmt is supposed to be executed after scalafix.
#### Testing the backend
1. The test framework is `scalatest`, for the amber engine, tests are located under `core/amber/src/test`; for `WorkflowCompilingService`, tests are located under `core/workflow-compiling-service`. You can find unit tests and e2e tests.
2. To execute it, navigate to `core` directory in the command line and execute `sbt test`.
3. If using IntelliJ to execute the test cases please make sure to be at the correct working directory.
* For the amber engine's tests, the working directory should be `core/amber`
* For the other services' tests, the working directory should be `core`
#### Testing the frontend 
Before merging your code to the master branch, you need to pass the existing unit tests first.
1. Open a command line. Navigate to the `core/gui` directory.
2. Start the test:
```
ng test --watch=false
```
3. Wait for some time and the test will get started.
You should also write some unit tests to cover your code. When others need to change your code, they will have to pass these unit tests so that you can keep your features safe.
The unit tests should be written inside `.spec.ts` file.
4. Run the following command to fix the formatting of the frontend code.
```
yarn format:fix
```

### 4. PR Review
- [ ] Ask a Texera Committer (by commenting on the PR) to triage your PR, i.e., request a reviewer, and assign the PR to you.
- [ ] Add appropriate labels such as `fix`, `enhancement`, `docs`, etc.
- [ ] Ensure that all CI checks pass (see [GitHub Actions](https://github.com/Texera/texera/actions)).
- [ ] Fully test your changes locally.

> ℹ️ If your PR is not ready for review, please mark it as a draft. You can change it to “Ready for review” when it is complete.

### 5. After PR Approval
- [ ] Wait for a Texera Committer, usually the reviewer, to merge the PR once it is approved.
- [ ] Close the related issue once the PR is merged (if it is not automatically closed).

---

## 📝 Apache License Header

All new files must include the Apache License header.

If you are modifying existing files, you may skip this step. For new files, you can automate this in IntelliJ by setting up a Copyright profile.

### Steps in IntelliJ:

1. Go to **Settings → Editor → Copyright → Copyright Profiles**.
2. Create a new profile and name it **Apache**.
3. Use the following license text:
   ```
   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at
   
       http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and 
   limitations under the License.
   ```
4. Go to "Editor" → "Copyright" and choose the "Apache" profile as the default profile for this
   project.
5. Click "Apply".

# Contributing to Texera

Thank you for your interest in contributing to Texera! Please follow the steps below to submit your contributions effectively. We follow a **fork-based development workflow** and adopt the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification for commit messages and pull request titles.

---

## 🛠 Contribution Workflow

### 1. Fork and Branch
- Fork the [Texera repository](https://github.com/Texera/texera) to your own GitHub account.
- Create a new branch in your fork for your contribution.

### 2. Open a Pull Request (PR)
- Submit a PR from your fork to the original Texera repository.
- **Check** the option **"Allow edits from maintainers"** so that Texera committers can make minor edits to your PR if needed.
  
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

### 3. Avoid Including Sensitive Information
Do not include any of the following in your PR:

- Local configuration files (e.g., `python_udf.conf`)
- Secrets or credentials (e.g., passwords, tokens)
- Build artifacts or binary files

### 4. Final Steps Before Review
- [ ] Assign yourself to the PR.
- [ ] Add appropriate labels such as `fix`, `enhancement`, `docs`, etc.
- [ ] Request at least one reviewer.
- [ ] Ensure that all CI checks pass (see [GitHub Actions](https://github.com/Texera/texera/actions)).
- [ ] Fully test your changes locally.
- [ ] Wait for a Texera committer to merge the PR once it is approved.

> ℹ️ If your PR is not ready for review, please mark it as a draft. You can change it to “Ready for review” when it is complete.

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

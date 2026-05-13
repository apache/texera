---
title: "Contribution Guidelines"
description: "How to contribute to Texera code and documentation."
weight: 60
categories: [Texera, Contributing]
tags: [contributing, development, documentation, github, workflow]
---

{{% pageinfo %}}
Thank you for your interest in contributing to Texera! This guide explains how to contribute to both **Texera’s codebase** and **documentation**.  
We follow a fork-based workflow and adopt the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) standard for commit messages.
{{% /pageinfo %}}

# Contributing to Texera

Texera welcomes contributions from everyone — whether you’re fixing a small bug, improving documentation, or adding new features.

---

## 👥 Roles in the Project

| Role | Key Permissions | How to Join |
|------|-----------------|--------------|
| **Contributor** | Submit issues & PRs, join discussions | Start contributing — no formal process |
| **Committer** | Merge PRs, push code, vote on code changes | Nominated by PPMC based on quality contributions |
| **PPMC Member** | Governance, release voting, new committer approvals | Voted by existing PPMC members |
| **Mentor** | Guide project and ensure Apache compliance | Appointed by the Incubator PMC |

---

## 🛠 How to Contribute Code

### 1. Fork the Repository
Fork the [Texera repository](https://github.com/Texera/texera) on GitHub and clone it locally.

### 2. Find or Open an Issue
- Pick an existing issue or create a new one describing your proposal or bug.
- Discuss your approach with committers before coding to reach consensus.

### 3. Create and Submit a Pull Request
- Develop in a new branch of your fork.

  > **Modifying the SQL schema?**  
  > Be sure to update `sql/changelog.xml` by adding a new `<changeSet>` element.  
- When ready, submit a PR to the main Texera repository.
- **Allow edits from maintainers** to let committers make small fixes if needed.

#### PR Title and Commit Format
We use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/):
- Example PR titles:
  - `feat: add new join operator`
  - `fix(ui): resolve workflow panel crash`
  - `chore(deps): bump dependency versions`
- The PR title becomes the final squashed commit message upon merge.

#### PR Description Should Include:
- **Purpose:** use `Closes #1234` to auto-close an issue.
- **Summary:** short overview of your changes.
- Optional: **design document**, **technical diagram**, or **screenshots**.

Avoid including:
- Local config files (e.g., `python_udf.conf`)
- Secrets or credentials
- Binary or build artifacts

---

## 🧪 Testing and Quality Checks

### Backend (Scala)
1. Run lint:
   ```bash
   sbt "scalafixAll --check"
   ```
   Fix with:
   ```bash
   sbt scalafixAll
   ```
2. Run formatter:
   ```bash
   sbt scalafmtCheckAll
   ```
   Fix with:
   ```bash
   sbt scalafmtAll
   ```
3. Execute tests:
   ```bash
   cd core
   sbt test
   ```

> For IntelliJ users: ensure the working directory matches the module (`amber` for engine tests, `core` for services).

### Frontend (Angular)
1. Run unit tests:
   ```bash
   cd core/gui
   ng test --watch=false
   ```
2. Format code:
   ```bash
   yarn format:fix
   ```

Write `.spec.ts` tests for new functionality to ensure future safety.

---

## 🔍 Pull Request Review Process
1. Request a committer to review your PR.
2. Add labels (e.g., `fix`, `enhancement`, `docs`).
3. Wait for CI to pass ([GitHub Actions](https://github.com/Texera/texera/actions)).
4. Mark your PR as **draft** if it’s not ready.
5. Once approved, a committer will merge your PR.

---

## 📝 Apache License Header
All new files must include the Apache License header.  
To automate this in IntelliJ:

1. Go to **Settings → Editor → Copyright → Copyright Profiles**.  
2. Create a profile named **Apache** and add:
   ```
   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements. See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership...
   ```
3. Set this as the default profile for the project.

---

## ✍️ Contributing to Documentation

Texera uses [Hugo](https://gohugo.io/) and the [Docsy](https://github.com/google/docsy) theme to build its website.  
All documentation is stored in the [Texera GitHub repository](https://github.com/Texera/texera).

### Quick Steps
1. Click **Edit this page** at the top of any doc page to edit directly on GitHub.
2. Make your edits and open a Pull Request.
3. The site auto-deploys a preview for review via Netlify.
4. Wait for approval and merge.

### Preview Locally
To preview locally:
```bash
hugo server
```
Visit `http://localhost:1313` to view the site as you edit.

---

## 📚 Resources
- [Texera GitHub Repository](https://github.com/Texera/texera)
- [Conventional Commits Spec](https://www.conventionalcommits.org/en/v1.0.0/)
- [Hugo Documentation](https://gohugo.io/documentation/)
- [Docsy Guide](https://www.docsy.dev/docs/)
- [GitHub Pull Request Docs](https://help.github.com/articles/about-pull-requests/)

#!/usr/bin/env node
// Spawn ng serve from the chore/monaco-lsp-v10 worktree.
// preview_start scopes cwd to the project root, so this trampoline lives in
// .claude/ here and execs the dev server in the sibling worktree.
const { spawn } = require("node:child_process");
const path = require("node:path");

const cwd = path.resolve(__dirname, "..", "..", "texera-worktrees", "chore-monaco-lsp-v10", "frontend");
const args = ["vite", "--port", "4321"];

const child = spawn("yarn", args, {
  cwd,
  stdio: "inherit",
  shell: process.platform === "win32",
});

child.on("exit", code => process.exit(code ?? 0));
process.on("SIGINT", () => child.kill("SIGINT"));
process.on("SIGTERM", () => child.kill("SIGTERM"));

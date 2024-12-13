//The source file can be referred to: https://github.com/TypeFox/monaco-languageclient/blob/main/packages/examples/src/python/server/main.ts

import { dirname, resolve } from "node:path";
import { runLanguageServer } from "./language-server-runner.ts";
import { getLocalDirectory, LanguageName } from "./server-commons.ts";
import fs from "fs";
import hoconParser from "hocon-parser";
import { fileURLToPath } from "url";

const runPythonServer = (
  baseDir: string,
  relativeDir: string,
  serverPort: number,
) => {
  const processRunPath = resolve(baseDir, relativeDir);
  runLanguageServer({
    serverName: "PYRIGHT",
    pathName: clientPathName,
    serverPort: serverPort,
    runCommand: LanguageName.node,
    runCommandArgs: [processRunPath, "--stdio"],
    wsServerOptions: {
      noServer: true,
      perMessageDeflate: false,
      clientTracking: true,
    },
  });
};

const baseDir = getLocalDirectory(import.meta.url);
const relativeDir = "./node_modules/pyright/dist/pyright-langserver.js";

const configFilePath = resolve(baseDir, "config.json");
const configContent = fs.readFileSync(configFilePath, "utf-8");
const config = JSON.parse(configContent) as Record<string, any>;

const languageServerDir = resolve(baseDir, config.languageServerDir);
const clientPathName = config.clientPathName;

const parseArgs = (): Record<string, string> => {
  const args = process.argv.slice(2);
  const options: Record<string, string> = {};
  args.forEach((arg) => {
    if (arg.startsWith("--") && arg.includes("=")) {
      const [key, value] = arg.substring(2).split("=");
      options[key] = value;
    }
  });
  return options;
};

const args = parseArgs();
const pythonLanguageServerPort = args["port"] ? parseInt(args["port"]) : 3000;

const runDir = resolve(dirname(fileURLToPath(import.meta.url)), "..");
runPythonServer(runDir, relativeDir, pythonLanguageServerPort);

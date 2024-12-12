//The source file can be referred to: https://github.com/TypeFox/monaco-languageclient/blob/main/packages/examples/src/python/server/main.ts

import { dirname, resolve } from "node:path";
import { runLanguageServer } from "./language-server-runner.ts";
import { getLocalDirectory, LanguageName } from "./server-commons.ts";
import fs from "fs";
import hoconParser from "hocon-parser";
import { fileURLToPath } from "url";
import { exec } from "child_process";
import { promisify } from "util";

const runPythonServer = async (
  baseDir: string,
  relativeDir: string,
  provider: string,
  serverPort: number,
): Promise<void> => {
  const processRunPath = resolve(baseDir, relativeDir);
  await runLanguageServer({
    serverName: provider,
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

const runPythonServerWithRetry = async (
  baseDir: string,
  relativeDir: string,
  provider: string,
  serverPort: number,
  maxRetries: number,
  waitTimeMs: number,
): Promise<void> => {
  let tryCount = 0;
  let started = false;
  while (tryCount < maxRetries && !started) {
    console.log(
      `Starting ${provider}... Attempt ${tryCount + 1} of ${maxRetries}`,
    );

    try {
      if (provider === "pyright") {
        await runPythonServer(baseDir, relativeDir, provider, serverPort);
      } else if (provider === "pylsp") {
        const pylspCommand = `pylsp --port ${serverPort} --ws`;
        await promisify(exec)(pylspCommand);
      }
      started = true; // Mark as started if no error occurs
    } catch (err) {
      console.error(
        `Failed to start ${provider} (Attempt ${tryCount + 1} of ${maxRetries}): ${err}`,
      );

      if (tryCount < maxRetries - 1) {
        console.log(`Retrying in ${waitTimeMs} ms...`);
        await new Promise((resolve) => setTimeout(resolve, waitTimeMs));
      }
    }

    tryCount++;
  }
  if (started)
    console.log(`${provider} language server is running on port ${serverPort}`);
  else
    console.log(
      `Failed to start ${provider} on port ${serverPort} after ${maxRetries} attempts`,
    );
};

const baseDir = getLocalDirectory(import.meta.url);
const relativeDir = "./node_modules/pyright/dist/pyright-langserver.js";

const configFilePath = resolve(baseDir, "pythonLanguageServerConfig.json");
const configContent = fs.readFileSync(configFilePath, "utf-8");
const config = JSON.parse(configContent) as Record<string, any>;

const pathConfig = config.Path;
const pythonLanguageServerConfig = config.pythonLanguageServer;
const languageServerDir = resolve(baseDir, pathConfig.languageServerDir);
const clientPathName = pathConfig.clientPathName;
const languageServerProvider = pythonLanguageServerConfig.provider;
const pythonLanguageServerPort = pythonLanguageServerConfig.port;
const languageServerRetryCounts = pythonLanguageServerConfig.retryCounts;
const languageServerWaitTimeMs = pythonLanguageServerConfig.waitTimeMs;

const supportedProviders = ["pyright", "pylsp"];
if (!supportedProviders.includes(languageServerProvider.toLowerCase())) {
  throw new Error(
    `Unknown language server: ${languageServerProvider}. Supported language servers are: ${supportedProviders.join(", ")}.`,
  );
}

const runDir = resolve(dirname(fileURLToPath(import.meta.url)), "..");
runPythonServerWithRetry(
  runDir,
  relativeDir,
  languageServerProvider,
  pythonLanguageServerPort,
  languageServerRetryCounts,
  languageServerWaitTimeMs,
);

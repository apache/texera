//The source file can be referred to: https://github.com/TypeFox/monaco-languageclient/blob/main/packages/examples/src/python/server/main.ts

import { resolve } from "node:path";
import { runLanguageServer } from "./language-server-runner.ts";
import { getLocalDirectory, LanguageName } from "./server-commons.ts";
import fs from "fs";
import hoconParser from "hocon-parser";

const runPythonServer = (baseDir: string, relativeDir: string, serverPort: number) => {
  const processRunPath = resolve(baseDir, relativeDir);
  runLanguageServer({
    serverName: "PYRIGHT",
    pathName: clientPathName,
    serverPort: serverPort,
    runCommand: LanguageName.node,
    runCommandArgs: [
      processRunPath,
      "--stdio",
    ],
    wsServerOptions: {
      noServer: true,
      perMessageDeflate: false,
      clientTracking: true,
    },
  });
};


const baseDir = getLocalDirectory(import.meta.url);
const relativeDir = "./node_modules/pyright/dist/pyright-langserver.js";

const configFilePath = resolve(baseDir, "pythonLanguageServerConfig.json");
const config = JSON.parse(fs.readFileSync(configFilePath, "utf-8"));

const amberConfigFilePath = resolve(baseDir, config.amberConfigFilePath);
const amberConfigContent = fs.readFileSync(amberConfigFilePath, "utf-8");
const applicationConfig = hoconParser(amberConfigContent) as Record<string, any>;

const pythonLanguageServerPort = applicationConfig["python-language-server"].port;
const clientPathName = config.clientPathName

runPythonServer(baseDir, relativeDir, pythonLanguageServerPort);
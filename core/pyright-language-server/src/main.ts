/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { dirname, resolve } from "node:path";
import { runLanguageServer } from "./language-server-runner.ts";
import { getLocalDirectory, LanguageName } from "./server-commons.ts";
import fs from "fs";
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

/* --------------------------------------------------------------------------------------------
 * Copyright (c) 2024 TypeFox and others.
 * Licensed under the MIT License. See LICENSE in the package root for license information.
 * ------------------------------------------------------------------------------------------ */
// The source file can be referred to: https://github.com/TypeFox/monaco-languageclient/blob/main/packages/examples/src/common/node/language-server-runner.ts

import { WebSocketServer } from "ws";
import { Server } from "node:http";
import express from "express";
import {
  getLocalDirectory,
  LanguageServerRunConfig,
  upgradeWsServer,
} from "./server-commons.ts";

/** LSP server runner */
export const runLanguageServer = async (
  languageServerRunConfig: LanguageServerRunConfig,
): Promise<void> => {
  return new Promise((resolve, reject) => {
    process.on("uncaughtException", (err) => {
      reject(err);
    });

    // create the express application
    const app = express();
    // serve the static content, i.e. index.html
    const dir = getLocalDirectory(import.meta.url);
    app.use(express.static(dir));
    // start the HTTP server
    const httpServer: Server = app.listen(
      languageServerRunConfig.serverPort,
      () => {
        resolve();
      },
    );

    const wss = new WebSocketServer(languageServerRunConfig.wsServerOptions);
    // create the WebSocket
    upgradeWsServer(languageServerRunConfig, {
      server: httpServer,
      wss,
    });
  });
};

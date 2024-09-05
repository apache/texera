import express from 'express';
import { WebSocketServer } from 'ws';
import { fileURLToPath } from 'url';
import { dirname, resolve} from 'path';
import { WebSocketMessageReader, WebSocketMessageWriter } from 'vscode-ws-jsonrpc';
import { createConnection, createServerProcess, forward } from 'vscode-ws-jsonrpc/server';
import hocon from 'hocon-parser';
import fs from 'fs';
import * as path from "node:path";

// To get the absolute path of this file
const absolutePath = fileURLToPath(import.meta.url);
// To get the absolute path of this file except for the file name
const dir = dirname(absolutePath);

// To get the config to avoid hard code path
const configFilePath = path.resolve(dir, 'pythonLanguageServerConfig.json');
const config = JSON.parse(fs.readFileSync(configFilePath, 'utf-8'));

// To get the backend application.conf for the port
const amberConfigFilePath = path.resolve(dir, config.amberConfigFilePath);
const amberConfigContent = fs.readFileSync(amberConfigFilePath, 'utf-8');
const applicationConfig = hocon(amberConfigContent);
// The port is decided by the configuration in the backend "python-language-server" flag
const pythonLanguageServerPort = applicationConfig['python-language-server'].port;

// To get the file to start the pyright
const pyrightPath = path.resolve(dir, config.languageServerDir, config.languageServerFile);

const app = express();
app.use(express.static(dir));
// Listening on a port which is configurable
const server = app.listen(pythonLanguageServerPort);
console.log(pythonLanguageServerPort)

const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (request, socket, head) => {
    wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit('connection', ws, request);
    });
});

const startPyrightServer = (remainingRetries = 3) => {
    try{
        return createServerProcess('pyright', 'node', [pyrightPath, '--stdio']);
    }catch (error) {
        console.error(`Failed to start Pyright language server: ${error.message}`);
        if (remainingRetries > 0) {
            console.log(`Retrying... (${remainingRetries} attempts left)`);
            return startPyrightServer(remainingRetries - 1);
        } else {
            throw new Error('Exceeded maximum retry attempts to start Pyright language server');
        }
    }
};

wss.on('connection', (ws) => {
    console.log('New WebSocket connection established.');

    //start a new server each time the websocket is on
    const serverConnection = startPyrightServer();

    const socket = {
        send: (content) => ws.send(content),
        onMessage: (message) => ws.on('message', message),
        onError: (error) => ws.on('error', error),
        onClose: () => {
            ws.on('close', () => {
                console.log('WebSocket connection closed.');
            });
        },
        dispose: () => {
            if (ws.readyState === ws.OPEN) {
                ws.close();
            }
        }
    };
    const reader = new WebSocketMessageReader(socket);
    const writer = new WebSocketMessageWriter(socket);
    const socketConnection = createConnection(reader, writer, () => socket.dispose());

    forward(socketConnection, serverConnection, message => {
        return message;
    });

});

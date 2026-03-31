# Texera Agent Service

AI-powered agents for workflow manipulation in Texera.

## Overview

This service provides a TypeScript implementation of AI agents that can manipulate Texera workflows. It uses the Vercel AI SDK for LLM integration and maintains workflow state that mirrors the frontend's workflow graph.

## Installation

### Using Bun (Recommended)

```bash
cd agent-service
bun install
```

### Using npm/pnpm/yarn

```bash
cd agent-service
npm install
# or
pnpm install
# or
yarn install
```

## Running the Service

### Environment Variables

Set the following environment variables:

```bash
export LLM_API_KEY="your-api-key"  # optional, defaults to "dummy" (for LiteLLM proxy)
export MODEL="gpt-4-turbo"         # optional, defaults to gpt-4-turbo
export PORT="3001"                 # optional, defaults to 3001
```

### Start HTTP Server

```bash
# Using Bun (recommended)
bun run dev          # Development mode with hot reload
bun run start        # Production mode

# Using Node.js (via tsx)
npm run dev:node     # Development mode with hot reload
npm run start:node   # Production mode
```

### Start Terminal GUI

```bash
# Using Bun
bun run gui

# Using Node.js
npm run gui:node
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/api/agents` | List all agents |
| GET | `/api/agents/:id` | Get/create agent |
| POST | `/api/agents/:id/message` | Send message (sync) |
| POST | `/api/agents/:id/stream` | Send message (SSE streaming) |
| GET | `/api/agents/:id/workflow` | Get workflow state |
| GET | `/api/agents/:id/messages` | Get conversation history |
| POST | `/api/agents/:id/stop` | Stop processing |
| POST | `/api/agents/:id/reset` | Reset agent |
| DELETE | `/api/agents/:id` | Delete agent |

## Project Structure

```
agent-service/
├── src/
│   ├── types/           # Core type definitions
│   │   ├── workflow.ts  # Workflow types (operators, links, schemas)
│   │   ├── execution.ts # Execution & compilation state types
│   │   └── agent.ts     # Agent-related types
│   ├── workflow/        # Workflow state management
│   │   └── workflow-state.ts
│   ├── tools/           # Agent tools
│   │   ├── workflow-tools.ts   # Workflow manipulation tools
│   │   ├── metadata-tools.ts   # Operator metadata tools
│   │   ├── execution-tools.ts  # Execution state tools
│   │   └── tools-utility.ts    # Shared utilities
│   ├── api/             # Backend API client
│   │   └── backend-api.ts      # API functions for backend services
│   ├── agent/           # Agent implementation
│   │   ├── texera-agent.ts     # Core agent class
│   │   └── prompts.ts          # System prompts
│   ├── gui/             # Terminal GUI (Ink-based)
│   │   └── index.tsx    # React terminal UI
│   ├── server.ts        # HTTP server
│   └── index.ts         # Library entry point
├── config/
│   └── backend.config.json  # Backend services configuration
├── package.json
├── tsconfig.json
└── README.md
```

## Usage

### Basic Example

```typescript
import { createOpenAI } from "@ai-sdk/openai";
import { TexeraAgent } from "./src";

// Create OpenAI client
const openai = createOpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

// Create agent
const agent = new TexeraAgent({
  model: openai("gpt-4-turbo"),
  agentId: "agent-1",
  agentName: "My Workflow Agent",
});

// Initialize operator schemas from backend (requires backend services running)
await agent.getMetadataStore().initializeFromBackend();

// Send a message
const result = await agent.sendMessage(
  "Add a Python UDF that filters rows where value > 10"
);

console.log("Response:", result.response);
console.log("Steps:", result.steps.length);
console.log("Tokens used:", result.usage.totalTokens);

// Get the resulting workflow
const workflow = agent.getWorkflowState().getWorkflowContent();
console.log("Operators:", workflow.operators);
console.log("Links:", workflow.links);
```

### Available Tools

The agent has access to the following tools:

1. **getCurrentWorkflow** - Get current workflow structure (operators and links)
2. **addOperator** - Add a new operator to the workflow
3. **addLink** - Connect two operators with a link
4. **modifyOperator** - Modify properties of an existing operator
5. **deleteFromWorkflow** - Delete operators and/or links
6. **listAllAvailableOperatorTypes** - List available operator types
7. **getOperatorSchema** - Get the schema for a specific operator type

### Workflow State

The `WorkflowState` class maintains:
- Operators (OperatorPredicate[])
- Links (OperatorLink[])
- Execution state (ExecutionState)
- Compilation state (CompilationState)
- Operator schemas (input/output)
- Results and console logs

## Architecture

This service mirrors the frontend copilot architecture but is designed for server-side use:

| Frontend (Angular) | Agent Service (Bun/Node) |
|-------------------|-------------------------|
| WorkflowActionService | WorkflowState |
| OperatorMetadataService | OperatorMetadataStore |
| TexeraCopilot | TexeraAgent |
| RxJS Observables | Async/Await |
| Angular DI | Direct instantiation |

## Development

### Type Checking

```bash
bun run typecheck
# or
npx tsc --noEmit
```

### Running Tests

```bash
bun test
```

### Building

The service is designed to be used as a library. Import from `src/index.ts`.

## Backend Configuration

The agent service connects to various Texera backend services. Configuration is read from:

1. **Environment variables** (highest priority):
   - `API_ENDPOINT` - Main API (default: http://localhost:8080)
   - `MODELS_ENDPOINT` - LiteLLM models service (default: http://localhost:9096)
   - `COMPILE_ENDPOINT` - Workflow compile service (default: http://localhost:9090)
   - `WS_ENDPOINT` - WebSocket for execution (default: ws://localhost:8085)
   - `DATASET_ENDPOINT` - File/dataset service (default: http://localhost:9092)

2. **Config file** (`config/backend.config.json`):
   ```json
   {
     "services": {
       "main": { "target": "http://localhost:8080" },
       "models": { "target": "http://localhost:9096" },
       "compile": { "target": "http://localhost:9090" }
     }
   }
   ```

3. **Default values** (lowest priority): localhost with standard ports

## Integration with Texera Backend

To integrate with the Texera backend for workflow execution:

1. **Operator Metadata**: Fetched automatically from backend API via `initializeFromBackend()`
2. **Model Listing**: Available models fetched from LiteLLM service
3. **WebSocket Connection**: Establish WebSocket connection for real-time execution
4. **Workflow Execution**: Send LogicalPlan to backend for execution
5. **Results**: Receive execution results via WebSocket

## License

Apache License 2.0

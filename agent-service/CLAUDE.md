# CLAUDE.md - Agent Service

This file provides guidance for working with the Texera Agent Service codebase.

## Overview

The Agent Service is a standalone microservice that provides AI-powered workflow manipulation capabilities for the Texera platform. It acts as a **user delegate** - performing workflow operations on behalf of authenticated users.

## Architecture

### Core Design Principles

1. **User Delegation Model**: The agent service acts on behalf of users, using their JWT tokens to authenticate with the Texera backend. When creating an agent, the frontend passes the user's token and workflow ID, enabling the agent to perform operations as that user.

2. **Backend as Source of Truth**: The Texera backend database is the authoritative source for workflow content. The agent service:
   - Loads workflows from the backend when created
   - Automatically persists changes to the backend (debounced)
   - Frontend polls workflow content from the backend, NOT from the agent service

3. **Stateless HTTP API**: The agent service exposes REST APIs via Elysia.js. Each agent is stored in-memory with its conversation history and workflow state.

4. **Tool-based Interaction**: Uses the Vercel AI SDK with custom tools for workflow manipulation. The agent reasons about user requests and executes appropriate tools.

### Directory Structure

```
agent-service/
├── src/
│   ├── server.ts           # Elysia.js HTTP server with REST endpoints
│   ├── index.ts            # Main exports
│   ├── agent/
│   │   ├── texera-agent.ts # Core agent implementation using Vercel AI SDK
│   │   ├── prompts.ts      # System prompts for the agent
│   │   └── index.ts
│   ├── api/
│   │   ├── backend-api.ts  # Texera backend API client (operators, models)
│   │   ├── auth-api.ts     # JWT token validation and user extraction
│   │   ├── workflow-api.ts # Workflow CRUD operations against backend
│   │   ├── execution-api.ts # WebSocket client for workflow execution
│   │   └── index.ts
│   ├── tools/
│   │   ├── workflow-tools.ts   # getCurrentWorkflow, addOperator, addLink, etc.
│   │   ├── metadata-tools.ts   # listOperatorTypes, getOperatorSchema
│   │   ├── execution-tools.ts  # Workflow execution tools
│   │   ├── tools-utility.ts    # Shared tool utilities
│   │   └── index.ts
│   ├── workflow/
│   │   ├── workflow-state.ts   # Workflow graph state management with change listeners
│   │   └── index.ts
│   └── types/
│       ├── agent.ts        # Agent-related types (AgentInfo, ReActStep, etc.)
│       ├── workflow.ts     # Workflow types (operators, links, logical plan)
│       ├── execution.ts    # Execution state types
│       └── index.ts
├── config/
│   └── backend.config.json # Backend service endpoints configuration
├── package.json
└── tsconfig.json
```

### Key Components

#### TexeraAgent (`src/agent/texera-agent.ts`)

The core agent class that:
- Maintains conversation history (CoreMessage[])
- Manages workflow state internally
- Processes user messages using Vercel AI SDK's `generateText`
- Tracks ReActSteps (reasoning trace) for debugging/display

Key methods:
- `sendMessage(message)` - Process a user message, returns response + steps
- `getSystemInfo()` - Returns system prompt and tools for frontend display
- `getWorkflowState()` - Access the internal workflow state
- `stop()` / `reset()` / `clearHistory()` - Control methods

#### WorkflowState (`src/workflow/workflow-state.ts`)

Manages the workflow graph with:
- Operators and links (the DAG structure)
- Execution state and results
- **Change listeners** for auto-persistence

Change listener pattern:
```typescript
workflowState.addChangeListener((event: WorkflowChangeEvent) => {
  // event.type: "add" | "modify" | "delete"
  // event.operatorIds, event.linkIds
  schedulePersist(agentId, stored);
});
```

#### Server (`src/server.ts`)

Elysia.js HTTP server with endpoints under `/api/agents`:

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/agents` | List all agents |
| POST | `/api/agents` | Create agent (with token + workflowId for delegation) |
| GET | `/api/agents/:id` | Get agent info + workflow |
| DELETE | `/api/agents/:id` | Delete agent |
| POST | `/api/agents/:id/message` | Send message to agent |
| GET | `/api/agents/:id/react-steps` | Get reasoning trace |
| GET | `/api/agents/:id/system-info` | Get system prompt and tools |
| POST | `/api/agents/:id/stop` | Stop generation |
| POST | `/api/agents/:id/reset` | Reset agent state |
| POST | `/api/agents/:id/clear` | Clear conversation history |

### Tools

Tools are created using Vercel AI SDK's `tool()` function with Zod schemas:

**Workflow Tools:**
- `getCurrentWorkflow` - Get operators and links
- `addOperator` - Add a new operator to the workflow
- `addLink` - Connect operators
- `modifyOperator` - Update operator properties
- `deleteFromWorkflow` - Remove operators/links

**Metadata Tools:**
- `listAllAvailableOperatorTypes` - Get available operator types
- `getOperatorSchema` - Get JSON schema for an operator type

**Execution Tools** (enabled when `executionConfig` is provided):
- `executeWorkflow` - Execute the current workflow and retrieve results
- `getExecutionState` - Get current execution state and operator statistics
- `killWorkflow` - Stop a running workflow execution
- `getExecutionResult` - Get results from the last execution
- `getOperatorResult` - Get paginated results for a specific operator

### Execution Architecture

The execution system uses WebSocket to communicate with the Texera backend:

```
TexeraAgent
    └── ExecutionManager
            └── ExecutionClient (WebSocket)
                    └── Texera Backend (wsapi/workflow-websocket)
```

**Key components:**

1. **ExecutionClient** (`src/api/execution-api.ts`)
   - Manages WebSocket connection to Texera backend
   - Handles authentication via JWT token in query params
   - Listens for execution events (state changes, stats, results)
   - Provides async methods: `executeWorkflow()`, `killWorkflow()`, `requestPaginatedResult()`

2. **ExecutionManager** (`src/tools/execution-tools.ts`)
   - Wraps ExecutionClient with lifecycle management
   - Tracks execution state and last result
   - Handles timeouts and cleanup

3. **Execution Tools**
   - Created when agent has `executionConfig` with user credentials
   - Tools build `LogicalPlan` from `WorkflowState` for execution

**Usage:**
```typescript
const agent = new TexeraAgent({
  model: myModel,
  agentId: "agent-1",
  executionConfig: {
    userToken: "jwt-token",
    workflowId: 123,
    userId: 1,
  },
});
// Agent now has execution tools available
```

## Development

### Commands

```bash
# Development with hot reload (requires Bun)
npm run dev

# Development with Node.js
npm run dev:node

# Production start
npm run start

# Type checking
npm run typecheck
```

### Configuration

Backend endpoints can be configured via:

1. **Environment variables** (highest priority):
   - `API_ENDPOINT` - Main API (default: http://localhost:8080)
   - `MODELS_ENDPOINT` - LLM models API (default: http://localhost:9096)
   - `WS_ENDPOINT` - WebSocket API (default: ws://localhost:8085)
   - `LLM_API_KEY` - API key for LLM calls

2. **Config file** (`config/backend.config.json`)

3. **Default values** (localhost with standard ports)

### Auto-Persistence

When an agent is created with `userToken` and `workflowId`:
1. The agent loads the workflow from the backend
2. A change listener is registered on `WorkflowState`
3. Any workflow modifications trigger debounced persistence (500ms)
4. The workflow is saved to the backend using the user's token

## Frontend Integration

The frontend (`TexeraCopilotManagerService`) communicates with this service via:

1. **Agent creation**: POST `/api/agents` with `modelType`, `userToken`, `workflowId`
2. **Message sending**: Via WebSocket at `/api/agents/:id/react`
3. **ReActSteps streaming**: Via WebSocket at `/api/agents/:id/react`
4. **Workflow content**: Polled from Texera backend, NOT from agent service

The frontend proxy routes `/api/agents` to this service (port 3001).

### Workflow Sync During Agent Activity

When the agent is actively modifying the workflow:

1. **Auto-persist disabled**: The frontend (`AgentChatComponent`) disables auto-persist when agent state is `GENERATING`
2. **Workflow polling**: Frontend polls workflow content from backend every 1 second via `getWorkflowObservable()`
3. **Workspace update**: When workflow content changes, `WorkflowActionService.reloadWorkflow()` is called with `preserveViewport=true`
4. **Auto-persist re-enabled**: When agent returns to `AVAILABLE` state, auto-persist is re-enabled

This ensures:
- Agent's changes are immediately visible in the workspace
- No conflict between agent persistence and frontend auto-save
- User's viewport is preserved during updates

## Important Notes

- **DO NOT** route auth/login to agent-service - the frontend handles auth and passes tokens
- **DO NOT** expose the user's JWT token in API responses (masked as `***`)
- Workflow changes are persisted automatically via change listeners, no manual sync needed
- The agent service maintains in-memory state only - agent data is lost on restart

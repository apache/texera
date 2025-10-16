# Texera MCP Service

Model Context Protocol (MCP) server for Apache Texera, exposing operator metadata and workflow capabilities to AI agents using the **official MCP SDK v0.14.1**.

## Overview

The MCP Service provides a standardized interface for AI assistants (like Claude) to understand and interact with Texera's workflow system through the Model Context Protocol. It exposes operator metadata, schemas, and capabilities using the official Java SDK.

## Architecture

The service uses the **official MCP Java SDK v0.14.1** for protocol implementation:

```
mcp-service/
├── src/main/
│   ├── java/org/apache/texera/mcp/
│   │   ├── server/
│   │   │   └── TexeraMcpServerImpl.java      # MCP SDK server implementation
│   │   └── tools/
│   │       └── OperatorToolProvider.java     # Java wrapper for tools
│   └── scala/org/apache/texera/mcp/
│       ├── McpService.scala                   # Main Dropwizard application
│       └── resource/
│           └── HealthCheckResource.scala     # Health check endpoint
├── common/config/
│   ├── src/main/resources/
│   │   └── mcp.conf                           # MCP configuration
│   └── src/main/scala/org/apache/texera/config/
│       └── McpConfig.scala                    # Configuration reader
```

### MCP Protocol Communication

- **Protocol**: Model Context Protocol (MCP) v1.0
- **SDK**: Official MCP Java SDK v0.14.1
- **Transport**: HTTP Streamable (Server-Sent Events)
- **Servlet**: `HttpServletStreamableServerTransportProvider` from SDK
- **Endpoint**: All MCP messages at `http://localhost:9098/api/mcp/`

## Building

### Build MCP Service Only
```bash
sbt "project McpService" compile
sbt "project McpService" dist
```

### Build All Services
```bash
bin/build-services.sh
```

## Running

### Start MCP Service
```bash
bin/mcp-service.sh
```

The service starts on port **9098** with:
- MCP server using HTTP Streamable transport at `http://localhost:9098/api/mcp/`
- Health check endpoint at `http://localhost:9098/api/healthcheck`

### Verify Service
```bash
# Health check (service monitoring only)
curl http://localhost:9098/api/healthcheck
# Expected: {"status":"ok","service":"mcp-service","version":"1.0.0"}

# MCP protocol - Initialize session
curl -X POST http://localhost:9098/api/mcp/ \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
      "protocolVersion": "2024-11-05",
      "capabilities": {},
      "clientInfo": {"name": "test", "version": "1.0"}
    }
  }'
```

## MCP Integration

### HTTP Streamable Transport

The MCP server uses HTTP Streamable transport (Server-Sent Events) for communication. MCP clients connect to:

**Base URL**: `http://localhost:9098/api/mcp`

The SDK's `HttpServletStreamableServerTransportProvider` handles:
- **GET requests**: Establish Server-Sent Events (SSE) connection for receiving server messages
- **POST requests**: Send client messages (tool calls, initialize, etc.)
- **DELETE requests**: Close the session

### Using with MCP Clients

MCP clients should use the HTTP transport to connect:

```typescript
// Example using MCP TypeScript SDK
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { HttpClientTransport } from "@modelcontextprotocol/sdk/client/http.js";

const transport = new HttpClientTransport(
  new URL("http://localhost:9098/api/mcp/")
);
const client = new Client({ name: "my-client", version: "1.0.0" }, {});
await client.connect(transport);

// List available tools
const tools = await client.listTools();

// Call a tool
const result = await client.callTool({
  name: "list_operators",
  arguments: {}
});
```

### Testing with curl

```bash
# Initialize session (POST)
curl -X POST http://localhost:9098/api/mcp/ \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
      "protocolVersion": "2024-11-05",
      "capabilities": {},
      "clientInfo": {"name": "test-client", "version": "1.0"}
    }
  }'

# List tools
curl -X POST http://localhost:9098/api/mcp/ \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 2,
    "method": "tools/list"
  }'

# Call a tool
curl -X POST http://localhost:9098/api/mcp/ \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 3,
    "method": "tools/call",
    "params": {
      "name": "list_operators",
      "arguments": {}
    }
  }'
```

## Available MCP Tools

The MCP server exposes 8 tools for querying Texera operator metadata:

### 1. `list_operators`
List all available Texera operators with metadata, groups, and schemas.

**Parameters**: None

**Returns**: Complete operator catalog with metadata

**Example usage in Claude**:
```
List all available Texera operators
```

### 2. `get_operator`
Get detailed metadata for a specific operator.

**Parameters**:
- `operatorType` (string): Operator type identifier (e.g., "CSVScanSource")

**Returns**: Full operator metadata including schema, ports, and capabilities

**Example**:
```
Get details for the CSVScanSource operator
```

### 3. `get_operator_schema`
Get JSON schema for operator configuration.

**Parameters**:
- `operatorType` (string): Operator type identifier

**Returns**: JSON schema defining configuration structure

**Example**:
```
Show me the configuration schema for CSVScanSource
```

### 4. `search_operators`
Search operators by name, description, or type.

**Parameters**:
- `query` (string): Search query

**Returns**: List of matching operators

**Example**:
```
Search for operators related to CSV
```

### 5. `get_operators_by_group`
Get all operators in a specific group.

**Parameters**:
- `groupName` (string): Group name (e.g., "Data Input", "Machine Learning")

**Returns**: List of operators in the group

**Example**:
```
Show me all operators in the Machine Learning group
```

### 6. `get_operator_groups`
Get all operator groups in hierarchical structure.

**Parameters**: None

**Returns**: Hierarchical list of operator groups

**Example**:
```
What are the operator groups in Texera?
```

### 7. `describe_operator`
Get detailed human-readable description of an operator.

**Parameters**:
- `operatorType` (string): Operator type identifier

**Returns**: Formatted description with ports, capabilities, and schema

**Example**:
```
Describe the KeywordSearch operator in detail
```

### 8. `get_operators_by_capability`
Get operators supporting specific capabilities.

**Parameters**:
- `capability` (string): One of: `reconfiguration`, `dynamic_input`, `dynamic_output`, `port_customization`

**Returns**: List of operators with the capability

**Example**:
```
Which operators support reconfiguration?
```

## Example Queries for Claude

Once integrated with Claude Desktop, you can ask:

- "List all available Texera operators"
- "Show me operators in the Machine Learning group"
- "What are the configuration options for the CSV scan operator?"
- "Find operators that support reconfiguration"
- "Describe the KeywordSearch operator in detail"
- "Search for operators that work with JSON data"
- "What visualization operators are available?"

## Monitoring Endpoints

The service provides a basic health check endpoint for service monitoring:

- `GET /api/healthcheck` - Standard health check endpoint

**Note**: All MCP functionality (server info, capabilities, tools) is accessed through the MCP protocol at `/api/mcp/` using JSON-RPC messages, not REST endpoints.

## Configuration

The MCP service uses two configuration files following Texera's standard pattern:

### MCP-Specific Configuration

Edit `common/config/src/main/resources/mcp.conf` for MCP-specific settings:

```hocon
mcp {
  server {
    port = 9098                # HTTP port for REST API
    name = "texera-mcp"        # Server name
    version = "1.0.0"          # Server version
  }

  transport = "http"           # MCP transport: http, stdio, or websocket

  auth {
    enabled = false            # Enable JWT authentication
  }

  database {
    enabled = true             # Enable database access
  }

  capabilities {
    tools = true               # Enable MCP tools
    resources = true           # Enable MCP resources (future)
    prompts = true             # Enable MCP prompts (future)
    sampling = false           # Enable sampling capability
    logging = true             # Enable logging capability
  }

  performance {
    max-concurrent-requests = 100    # Max concurrent requests
    request-timeout-ms = 30000       # Request timeout in milliseconds
  }

  enabled-tools = ["operators", "workflows", "datasets", "projects", "executions"]
  enabled-resources = ["operator-schemas", "workflow-templates", "dataset-schemas"]
  enabled-prompts = ["create-workflow", "optimize-workflow", "explain-operator"]
}
```

Configuration values can be overridden with environment variables:
- `MCP_PORT` - Override server port
- `MCP_TRANSPORT` - Override transport type
- `MCP_AUTH_ENABLED` - Enable/disable authentication
- `MCP_TOOLS_ENABLED` - Enable/disable tools capability
- etc.

### Dropwizard Configuration

Edit `mcp-service/src/main/resources/mcp-service-config.yaml` for Dropwizard-specific settings:

```yaml
server:
  applicationConnectors:
    - type: http
      port: 9098
  adminConnectors: []

logging:
  level: INFO
  loggers:
    "io.dropwizard": INFO
    "org.apache.texera": DEBUG
  appenders:
    - type: console
    - type: file
      currentLogFilename: log/mcp-service.log
```

## Dependencies

### MCP SDK Dependencies (in build.sbt)
```scala
"io.modelcontextprotocol.sdk" % "mcp" % "0.14.1",
"io.modelcontextprotocol.sdk" % "mcp-json-jackson2" % "0.14.1"
```

### Module Dependencies
- `WorkflowOperator` - Operator metadata generation
- `Auth` - Authentication (optional)
- `Config` - Configuration management
- `DAO` - Database access (optional)

## Development

### Project Structure

- **Java classes** (`src/main/java`): MCP SDK integration, uses official SDK
- **Scala classes** (`src/main/scala`): Dropwizard app, REST API, business logic

### Adding New Tools

1. Implement tool logic in `OperatorTool.scala` or create new tool class
2. Add Java wrapper in `OperatorToolProvider.java` (if needed)
3. Register tool in `TexeraMcpServerImpl.java` following SDK patterns:

```java
private void registerMyTool(McpSyncServerBuilder builder) {
    // Create input schema
    ObjectNode inputSchema = objectMapper.createObjectNode();
    // ... define schema

    // Create tool
    Tool tool = new Tool(
        "my_tool_name",
        "Tool description",
        inputSchema
    );

    // Create tool specification
    McpServerFeatures.SyncToolSpecification toolSpec =
        new McpServerFeatures.SyncToolSpecification(
            tool,
            (exchange, arguments) -> {
                // Tool implementation
                // Return CallToolResult with TextContent
            }
        );

    builder.tool(toolSpec);
}
```

### Testing

```bash
# Compile
sbt "project McpService" compile

# Run tests
sbt "project McpService" test

# Build distribution
sbt "project McpService" dist
```

## Logs

Logs are written to:
- Console output
- `log/mcp-service.log` - Current log
- `log/mcp-service-YYYY-MM-DD.log.gz` - Archived (7 days retention)

## Troubleshooting

### Service won't start
- Check port 9098 availability: `lsof -i :9098`
- Verify config YAML is valid
- Check logs: `log/mcp-service.log`

### MCP client can't connect
- Ensure service is running: `curl http://localhost:9098/api/healthcheck`
- Verify HTTP transport endpoint is accessible at `http://localhost:9098/api/mcp/`
- Check client is using HTTP Streamable transport, not STDIO
- Test with curl initialize command (see "Testing with curl" section)

### "Tool not found" errors
- Verify tools are registered in `TexeraMcpServerImpl`
- Check `enableTools: true` in configuration
- Review `enabledTools` list in config

### Compilation errors
- Ensure MCP SDK version 0.14.1 is available
- Run `sbt update` to fetch dependencies
- Check Java 17+ is installed

## Future Extensions

The architecture supports adding:

### Workflow Tools (Planned)
- `list_workflows` - List user workflows
- `get_workflow` - Get workflow details
- `validate_workflow` - Validate workflow
- `create_workflow` - Create new workflows
- `update_workflow` - Modify workflows

### Dataset Tools (Planned)
- `list_datasets` - List available datasets
- `get_dataset_schema` - Get dataset schema
- `query_dataset` - Query dataset contents

### Execution Tools (Planned)
- `get_execution_status` - Monitor execution
- `get_execution_logs` - Retrieve logs
- `get_execution_results` - Get results

### MCP Resources (Planned)
- Workflow templates
- Operator schemas
- Dataset schemas

### MCP Prompts (Planned)
- Create workflow from description
- Optimize workflow
- Explain operator functionality

## Technical Details

### MCP SDK Integration

The service uses the official MCP Java SDK (v0.14.1) which provides:
- **Protocol implementation**: Full MCP specification compliance
- **Transport providers**: STDIO, HTTP, WebSocket support
- **Type-safe APIs**: Strongly typed tool specifications
- **Async/Sync modes**: Flexible execution models

### Transport: HTTP Streamable

HTTP Streamable transport uses Server-Sent Events (SSE) for MCP communication:
- **GET** requests establish SSE connection for server→client messages
- **POST** requests send client→server JSON-RPC messages
- **DELETE** requests close the session
- JSON-RPC message format over HTTP
- Stateful sessions maintained via SSE connection

### Why Java Implementation?

While Texera is primarily Scala, the MCP server uses Java because:
1. Official MCP SDK is Java/Kotlin
2. Better SDK compatibility and stability
3. Easier to follow official documentation
4. Scala wrapper provides clean API for Texera code

## Port Information

- **9098** - MCP Service (HTTP Streamable transport at `/api/mcp/`, health check at `/api/healthcheck`)
- 8080 - Amber (Main Texera service)
- 9090 - Workflow Compiling Service
- 9092 - File Service

## References

- [Model Context Protocol Specification](https://modelcontextprotocol.io/)
- [MCP Java SDK Documentation](https://modelcontextprotocol.io/sdk/java/mcp-server)
- [Claude Desktop MCP Integration](https://www.anthropic.com/news/model-context-protocol)

## License

Licensed under the Apache License, Version 2.0. See LICENSE file for details.

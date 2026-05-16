/*
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

package org.apache.texera.amber.operator.aiagent

import com.fasterxml.jackson.annotation.{JsonFormat, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{
  InputPort,
  OutputPort,
  PhysicalOp,
  SchemaPropagationFunc
}
import org.apache.texera.amber.operator.map.MapOpDesc
import org.apache.texera.amber.operator.metadata.annotations.{AutofillAttributeNameList, UIWidget}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

class AIAgentOpDesc extends MapOpDesc {

  @JsonProperty(value = "outputMode", required = false, defaultValue = "text")
  @JsonSchemaTitle("Output Mode")
  @JsonPropertyDescription(
    "Controls whether the AI response is emitted as one text column or multiple structured columns"
  )
  @JsonSchemaInject(
    json = """{"enum": ["text", "structured"], "default": "text"}"""
  )
  var outputMode: String = AIAgentOutputMode.Text

  @JsonProperty(value = "structuredOutputFields", required = false)
  @JsonSchemaTitle("Structured Output Fields")
  @JsonPropertyDescription(
    "Define each output column and what the model should extract for it. The model returns a JSON object with one key per column."
  )
  @JsonSchemaInject(
    json =
      """{"hideTarget": "outputMode", "hideType": "equals", "hideExpectedValue": "text", "hideOnNull": true}"""
  )
  var structuredOutputFields: List[AIAgentStructuredOutputField] = List.empty

  @JsonProperty(value = "textClassificationLabels", required = false)
  @JsonSchemaTitle("Text Classification Labels")
  @JsonPropertyDescription(
    "Optional allowed labels for text output. Leave empty for free-form text; fill this to make the text response a classification."
  )
  @JsonSchemaInject(
    json =
      """{"hideTarget": "outputMode", "hideType": "equals", "hideExpectedValue": "structured", "hideOnNull": true, "widget": {"formlyConfig": {"type": "tags-input"}}}"""
  )
  var textClassificationLabels: List[String] = List.empty

  @JsonProperty(value = "classificationLabels", required = false)
  @JsonSchemaTitle("Legacy Classification Labels")
  @JsonPropertyDescription(
    "Deprecated. Use Text Classification Labels or structured classification fields."
  )
  @JsonSchemaInject(
    json = """{"hideTarget": "outputMode", "hideType": "regex", "hideExpectedValue": ".*"}"""
  )
  var classificationLabels: List[String] = List.empty

  @JsonProperty(value = "confidenceColumn", required = false)
  @JsonSchemaTitle("Legacy Confidence Column")
  @JsonPropertyDescription(
    "Deprecated. Retained so workflows saved before classification was removed still load."
  )
  @JsonSchemaInject(
    json = """{"hideTarget": "outputMode", "hideType": "regex", "hideExpectedValue": ".*"}"""
  )
  var confidenceColumn: String = ""

  @JsonProperty(value = "outputColumn", required = false, defaultValue = "ai_agent_response")
  @JsonSchemaTitle("Output Column")
  @JsonPropertyDescription("Column name for the text response")
  @JsonSchemaInject(
    json =
      """{"hideTarget": "outputMode", "hideType": "equals", "hideExpectedValue": "structured"}"""
  )
  var outputColumn: String = "ai_agent_response"

  @JsonProperty(value = "systemPrompt", required = false)
  @JsonSchemaTitle("System Prompt")
  @JsonPropertyDescription("Optional system prompt sent before each row prompt")
  @JsonSchemaInject(json = UIWidget.UIWidgetTextArea)
  var systemPrompt: String = ""

  @JsonProperty(value = "inputColumn", required = true)
  @JsonSchemaTitle("Columns Sent to AI")
  @JsonPropertyDescription("Column values sent as the user prompt for each input row")
  @JsonFormat(`with` = Array(JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY))
  @AutofillAttributeNameList
  var inputColumn: List[String] = List.empty

  @JsonProperty(value = "apiKey", required = true)
  @JsonSchemaTitle("OpenRouter API Key")
  @JsonPropertyDescription("OpenRouter API key")
  @JsonSchemaInject(json = UIWidget.UIWidgetPassword)
  var apiKey: String = _

  @JsonProperty(value = "model", required = true, defaultValue = "openai/gpt-4o-mini")
  @JsonSchemaTitle("Model")
  @JsonPropertyDescription("OpenRouter model ID")
  var model: String = "openai/gpt-4o-mini"

  @JsonProperty(value = "temperature", required = true)
  @JsonSchemaTitle("Temperature")
  @JsonPropertyDescription("Sampling temperature")
  @JsonSchemaInject(json = """{"default": 0.7}""")
  var temperature: Double = 0.7

  @JsonProperty(value = "timeoutSeconds", required = true, defaultValue = "60")
  @JsonSchemaTitle("Timeout Seconds")
  @JsonPropertyDescription("OpenRouter request timeout in seconds")
  var timeoutSeconds: Int = 60

  @JsonProperty(value = "enabledTools", required = false)
  @JsonSchemaTitle("Enabled Tools")
  @JsonPropertyDescription(
    "Optional tools the AI can call per row. read_url fetches a web page as Markdown; read_pdf extracts text from a PDF URL or path."
  )
  @JsonSchemaInject(
    json =
      """{"type": "array", "items": {"type": "string", "enum": ["read_url", "read_pdf"]}, "uniqueItems": true, "default": []}"""
  )
  var enabledTools: List[String] = List.empty

  @JsonProperty(value = "maxToolIterations", required = false)
  @JsonSchemaTitle("Max Tool Iterations")
  @JsonPropertyDescription(
    "Maximum number of model turns when tools are enabled. Each turn either calls a tool or returns the final answer."
  )
  @JsonSchemaInject(
    json = """{"default": 5}"""
  )
  var maxToolIterations: java.lang.Integer = 5

  @JsonProperty(value = "urlFetchMaxChars", required = false)
  @JsonSchemaTitle("URL Fetch Max Chars")
  @JsonPropertyDescription("Truncate read_url Markdown output to this many characters.")
  @JsonSchemaInject(json = """{"default": 50000}""")
  var urlFetchMaxChars: java.lang.Integer = 50000

  @JsonProperty(value = "pdfReadMaxChars", required = false)
  @JsonSchemaTitle("PDF Read Max Chars")
  @JsonPropertyDescription("Truncate read_pdf text output to this many characters.")
  @JsonSchemaInject(json = """{"default": 100000}""")
  var pdfReadMaxChars: java.lang.Integer = 100000

  @JsonProperty(value = "parallelism", required = false)
  @JsonSchemaTitle("Parallelism")
  @JsonPropertyDescription(
    "Number of parallel workers. Texera shards the input rows across this many actors, each running the agent loop independently. Each worker has its own response cache. Watch out for upstream API rate limits when raising this."
  )
  @JsonSchemaInject(json = """{"default": 1, "minimum": 1, "maximum": 32}""")
  var parallelism: java.lang.Integer = 1

  @JsonProperty(value = "cacheEnabled", required = false)
  @JsonSchemaTitle("Cache Responses")
  @JsonPropertyDescription(
    "If true, identical (model, prompt, tools, structured output) requests reuse the previous response without billing."
  )
  @JsonSchemaInject(json = """{"default": true}""")
  var cacheEnabled: Boolean = true

  @JsonProperty(value = "emitCostColumn", required = false)
  @JsonSchemaTitle("Emit Cost Column")
  @JsonPropertyDescription(
    "If true, append a double column with the estimated USD cost of the LLM call for the row."
  )
  @JsonSchemaInject(json = """{"default": true}""")
  var emitCostColumn: Boolean = true

  @JsonProperty(value = "costColumnName", required = false)
  @JsonSchemaTitle("Cost Column Name")
  @JsonPropertyDescription("Name of the cost column appended to the output schema.")
  @JsonSchemaInject(json = """{"default": "_cost_usd"}""")
  var costColumnName: String = "_cost_usd"

  @JsonProperty(value = "maxRowCostUsd", required = false)
  @JsonSchemaTitle("Max Row Cost (USD)")
  @JsonPropertyDescription(
    "Optional hard cap. If a row's accumulated tool-loop cost exceeds this value, the row is aborted and surfaced as an error."
  )
  var maxRowCostUsd: java.lang.Double = _

  @JsonProperty(value = "emitErrorColumn", required = false)
  @JsonSchemaTitle("Emit Error Column")
  @JsonPropertyDescription(
    "If true, append a string column containing the error message when the LLM call fails (empty on success)."
  )
  @JsonSchemaInject(json = """{"default": true}""")
  var emitErrorColumn: Boolean = true

  @JsonProperty(value = "errorColumnName", required = false)
  @JsonSchemaTitle("Error Column Name")
  @JsonPropertyDescription("Name of the error column appended to the output schema.")
  @JsonSchemaInject(json = """{"default": "_error"}""")
  var errorColumnName: String = "_error"

  @JsonProperty(value = "mcpServers", required = false)
  @JsonSchemaTitle("MCP Servers")
  @JsonPropertyDescription(
    "Connect Model Context Protocol servers to give the agent additional tools (e.g. Notion, Slack, GitHub, internal APIs). Tools discovered from each server are namespaced as `serverName__toolName`."
  )
  var mcpServers: List[AIAgentMCPServerConfig] = List.empty

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    validateConfig()
    val workers = normalizedParallelism
    val base = PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.aiagent.AIAgentOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
    val parallelized =
      if (workers > 1) base.withParallelizable(true).withSuggestedWorkerNum(workers)
      else base.withParallelizable(false)
    parallelized
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas => {
          val inputPortId = operatorInfo.inputPorts.head.id
          val outputPortId = operatorInfo.outputPorts.head.id
          val inputSchema = inputSchemas(inputPortId)
          Map(outputPortId -> outputSchema(inputSchema))
        })
      )
  }

  def normalizedParallelism: Int =
    Option(parallelism).map(_.intValue).filter(_ >= 1).map(_.min(32)).getOrElse(1)

  private def validateConfig(): Unit = {
    if (apiKey == null || apiKey.trim.isEmpty) {
      throw new IllegalArgumentException(
        "AI Agent: OpenRouter API key is missing. Open this operator's property panel and paste your key into 'OpenRouter API Key'."
      )
    }
    if (model == null || model.trim.isEmpty) {
      throw new IllegalArgumentException("AI Agent: Model is required.")
    }
    if (normalizedOutputMode == AIAgentOutputMode.Structured &&
        normalizedStructuredOutputFields.isEmpty) {
      throw new IllegalArgumentException(
        "AI Agent: Structured output mode requires at least one structured output field. Add fields or switch to text mode."
      )
    }
    if (Option(inputColumn).getOrElse(List.empty).forall(c => c == null || c.trim.isEmpty)) {
      throw new IllegalArgumentException(
        "AI Agent: 'Columns Sent to AI' is empty — pick at least one input column."
      )
    }
  }

  private def outputSchema(inputSchema: org.apache.texera.amber.core.tuple.Schema) = {
    val base = normalizedOutputMode match {
      case AIAgentOutputMode.Structured =>
        normalizedStructuredOutputColumns.foldLeft(inputSchema) { (schema, column) =>
          schema.add(new Attribute(column, AttributeType.STRING))
        }
      case AIAgentOutputMode.Classification =>
        addTextOutputColumn(inputSchema)
      case _ =>
        addTextOutputColumn(inputSchema)
    }
    val withCost =
      if (
        emitCostColumn &&
        normalizedCostColumnName.nonEmpty &&
        !base.containsAttribute(normalizedCostColumnName)
      ) {
        base.add(new Attribute(normalizedCostColumnName, AttributeType.DOUBLE))
      } else base
    if (
      emitErrorColumn &&
      normalizedErrorColumnName.nonEmpty &&
      !withCost.containsAttribute(normalizedErrorColumnName)
    ) {
      withCost.add(new Attribute(normalizedErrorColumnName, AttributeType.STRING))
    } else withCost
  }

  def normalizedErrorColumnName: String =
    Option(errorColumnName).map(_.trim).filter(_.nonEmpty).getOrElse("_error")

  def normalizedCostColumnName: String =
    Option(costColumnName).map(_.trim).filter(_.nonEmpty).getOrElse("_cost_usd")

  def normalizedMaxRowCostUsd: Option[Double] =
    Option(maxRowCostUsd).map(_.doubleValue).filter(_ > 0.0)

  private def addTextOutputColumn(inputSchema: org.apache.texera.amber.core.tuple.Schema) =
    if (outputColumn == null || outputColumn.trim.isEmpty) {
      inputSchema
    } else {
      inputSchema.add(new Attribute(outputColumn, AttributeType.STRING))
    }

  def normalizedStructuredOutputColumns: List[String] =
    normalizedStructuredOutputFields.map(_.columnName.trim)

  def normalizedStructuredOutputFields: List[AIAgentStructuredOutputField] =
    Option(structuredOutputFields)
      .getOrElse(List.empty)
      .filter(field => field != null && field.columnName != null && field.columnName.trim.nonEmpty)

  def normalizedClassificationLabels: List[String] =
    Option(classificationLabels).getOrElse(List.empty).map(_.trim).filter(_.nonEmpty)

  def normalizedTextClassificationLabels: List[String] =
    Option(textClassificationLabels).getOrElse(List.empty).map(_.trim).filter(_.nonEmpty)

  def normalizedMaxToolIterations: Int =
    Option(maxToolIterations).map(_.intValue).filter(_ > 0).getOrElse(5)

  def normalizedUrlFetchMaxChars: Int =
    Option(urlFetchMaxChars).map(_.intValue).filter(_ > 0).getOrElse(UrlFetcher.DefaultMaxChars)

  def normalizedPdfReadMaxChars: Int =
    Option(pdfReadMaxChars).map(_.intValue).filter(_ > 0).getOrElse(PdfReader.DefaultMaxChars)

  def normalizedEnabledTools: List[String] =
    Option(enabledTools).getOrElse(List.empty).map(_.trim).filter(_.nonEmpty).distinct

  def buildTools: List[AIAgentTool] = {
    val builtIn = normalizedEnabledTools.flatMap {
      case UrlFetchTool.Name => Some(new UrlFetchTool(normalizedUrlFetchMaxChars))
      case PdfReadTool.Name  => Some(new PdfReadTool(normalizedPdfReadMaxChars))
      case _                 => None
    }
    val mcp = normalizedMcpServers.flatMap { server =>
      val client = new MCPClient(
        serverName = server.normalizedName,
        url = server.url.trim,
        bearerToken = Option(server.bearerToken).map(_.trim).filter(_.nonEmpty),
        timeoutSeconds = server.normalizedTimeoutSeconds
      )
      try {
        client.initialize()
        client.listTools().map(info => new MCPToolAdapter(client, info))
      } catch {
        case t: Throwable =>
          throw new RuntimeException(
            s"AI Agent: failed to connect to MCP server '${server.normalizedName}' at ${server.url.trim}: " +
              s"${t.getClass.getSimpleName}: ${Option(t.getMessage).getOrElse("")}. " +
              s"Check the URL and Bearer Token in the operator's MCP Servers config.",
            t
          )
      }
    }
    builtIn ++ mcp
  }

  def normalizedMcpServers: List[AIAgentMCPServerConfig] =
    Option(mcpServers)
      .getOrElse(List.empty)
      .filter(s => s != null && s.url != null && s.url.trim.nonEmpty)

  def normalizedOutputMode: String =
    Option(outputMode).map(_.trim).filter(_.nonEmpty).getOrElse(AIAgentOutputMode.Text)

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "AI Agent",
      "Calls OpenRouter chat completions once per input row",
      OperatorGroupConstants.API_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort()),
      supportReconfiguration = true
    )
}

object AIAgentOutputMode {
  final val Text = "text"
  final val Structured = "structured"
  final val Classification = "classification"
}

class AIAgentStructuredOutputField {
  @JsonProperty(value = "fieldType", required = false, defaultValue = "text")
  @JsonSchemaTitle("Field Type")
  @JsonPropertyDescription("Choose free-form text or a classification label for this output column")
  @JsonSchemaInject(json = """{"enum": ["text", "classification"], "default": "text"}""")
  var fieldType: String = AIAgentStructuredFieldType.Text

  @JsonProperty(value = "columnName", required = true)
  @JsonSchemaTitle("Column Name")
  @JsonPropertyDescription("Output column name and JSON key for this extracted value")
  var columnName: String = ""

  @JsonProperty(value = "instructions", required = false)
  @JsonSchemaTitle("Instructions")
  @JsonPropertyDescription("Describe what this column should contain for each row")
  @JsonSchemaInject(json = UIWidget.UIWidgetTextArea)
  var instructions: String = ""

  @JsonProperty(value = "classificationLabels", required = false)
  @JsonSchemaTitle("Classification Labels")
  @JsonPropertyDescription("Allowed labels when this structured field is a classification")
  @JsonSchemaInject(
    json =
      """{"hideTarget": "fieldType", "hideType": "equals", "hideExpectedValue": "text", "hideOnNull": true, "widget": {"formlyConfig": {"type": "tags-input"}}}"""
  )
  var classificationLabels: List[String] = List.empty

  def normalizedFieldType: String =
    Option(fieldType).map(_.trim).filter(_.nonEmpty).getOrElse(AIAgentStructuredFieldType.Text)

  def normalizedClassificationLabels: List[String] =
    Option(classificationLabels).getOrElse(List.empty).map(_.trim).filter(_.nonEmpty)
}

object AIAgentStructuredFieldType {
  final val Text = "text"
  final val Classification = "classification"
}

class AIAgentMCPServerConfig {
  @JsonProperty(value = "name", required = false)
  @JsonSchemaTitle("Server Name")
  @JsonPropertyDescription(
    "Short identifier used to namespace this server's tools (e.g. `notion` → `notion__search`). Letters, digits, and underscores only."
  )
  var name: String = ""

  @JsonProperty(value = "url", required = true)
  @JsonSchemaTitle("Server URL")
  @JsonPropertyDescription(
    "Streamable HTTP endpoint of the MCP server, e.g. https://mcp.notion.com/mcp"
  )
  var url: String = ""

  @JsonProperty(value = "bearerToken", required = false)
  @JsonSchemaTitle("Bearer Token")
  @JsonPropertyDescription(
    "Optional auth token sent as `Authorization: Bearer ...`. Leave blank for unauthenticated servers."
  )
  @JsonSchemaInject(json = UIWidget.UIWidgetPassword)
  var bearerToken: String = ""

  @JsonProperty(value = "timeoutSeconds", required = false)
  @JsonSchemaTitle("Timeout Seconds")
  @JsonPropertyDescription("HTTP timeout for each MCP request.")
  @JsonSchemaInject(json = """{"default": 30}""")
  var timeoutSeconds: java.lang.Integer = 30

  def normalizedName: String =
    Option(name).map(_.trim).filter(_.nonEmpty).getOrElse("mcp")

  def normalizedTimeoutSeconds: Int =
    Option(timeoutSeconds).map(_.intValue).filter(_ > 0).getOrElse(30)
}

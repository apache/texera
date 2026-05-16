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

import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.operator.map.MapOpExec
import org.apache.texera.amber.util.JSONUtils.objectMapper

class AIAgentOpExec(descString: String) extends MapOpExec {
  private val desc: AIAgentOpDesc =
    objectMapper.readValue(descString, classOf[AIAgentOpDesc])
  private val client: ChatCompletionClient = new OpenRouterClient
  private val tools: List[AIAgentTool] = desc.buildTools
  private val cache: AIAgentResponseCache = new AIAgentResponseCache()

  setMapFunc(callAIAgent)

  override def close(): Unit = {
    tools.foreach { tool =>
      try tool.close()
      catch {
        case _: Throwable =>
      }
    }
  }

  private def cacheKey(userPrompt: String): String = {
    val mcpSig = desc.normalizedMcpServers
      .map { s =>
        val tokenHash = Option(s.bearerToken)
          .map(_.trim)
          .filter(_.nonEmpty)
          .map(AIAgentResponseCache.sha256)
          .getOrElse("")
        s"${s.normalizedName}|${s.url.trim}|$tokenHash"
      }
      .sorted
      .mkString(",")
    val toolSig = tools.map(_.name).sorted.mkString(",") + "||mcp=" + mcpSig
    val structSig = desc.normalizedOutputMode + ":" +
      desc.normalizedStructuredOutputFields
        .map(f => s"${f.columnName.trim}|${f.normalizedFieldType}|${f.normalizedClassificationLabels.mkString(";")}")
        .mkString(",") + ":" + desc.normalizedTextClassificationLabels.mkString(";") +
      ":" + desc.normalizedClassificationLabels.mkString(";") +
      s":operator=${desc.operatorIdentifier.id}"
    AIAgentResponseCache.key(
      desc.model,
      desc.temperature,
      AIAgentResponseCache.sha256(desc.apiKey),
      effectiveSystemPrompt,
      userPrompt,
      toolSig,
      structSig
    )
  }

  private def callAIAgent(tuple: Tuple): TupleLike = {
    val userPrompt = buildUserPrompt(tuple)
    val key = if (desc.cacheEnabled) cacheKey(userPrompt) else null
    val (fields, costUsd, errorMsg) =
      try {
        val cached = if (desc.cacheEnabled) cache.get(key) else None
        cached match {
          case Some(text) => (outputFields(text), 0.0, "")
          case None =>
            val result = completeForMode(userPrompt)
            if (desc.cacheEnabled) cache.put(key, result.text)
            (outputFields(result.text), result.usdCost, "")
        }
      } catch {
        case t: Throwable =>
          val raw = s"${t.getClass.getSimpleName}: ${Option(t.getMessage).getOrElse("")}"
          val truncated = if (raw.length > 1000) raw.substring(0, 1000) else raw
          (emptyOutputFields, 0.0, truncated)
      }
    val inputSchema = tuple.getSchema
    val emittedNames = outputAttributeNames(inputSchema)
    val withCost: Seq[Any] =
      if (
        desc.emitCostColumn &&
        desc.normalizedCostColumnName.nonEmpty &&
        !emittedNames.exists(_.equalsIgnoreCase(desc.normalizedCostColumnName))
      )
        fields :+ java.lang.Double.valueOf(costUsd)
      else fields
    val namesWithCost =
      if (withCost.length > fields.length) emittedNames :+ desc.normalizedCostColumnName else emittedNames
    val withError: Seq[Any] =
      if (
        desc.emitErrorColumn &&
        desc.normalizedErrorColumnName.nonEmpty &&
        !namesWithCost.exists(_.equalsIgnoreCase(desc.normalizedErrorColumnName))
      ) withCost :+ errorMsg
      else withCost
    TupleLike(tuple.getFields ++ withError)
  }

  private def outputAttributeNames(inputSchema: org.apache.texera.amber.core.tuple.Schema): Seq[String] = {
    val inputNames = inputSchema.getAttributeNames
    desc.normalizedOutputMode match {
      case AIAgentOutputMode.Structured =>
        inputNames ++ desc.normalizedStructuredOutputColumns
      case _ if Option(desc.outputColumn).exists(_.trim.nonEmpty) =>
        inputNames :+ desc.outputColumn.trim
      case _ =>
        inputNames
    }
  }

  private def emptyOutputFields: Seq[Any] =
    desc.normalizedOutputMode match {
      case AIAgentOutputMode.Structured =>
        desc.normalizedStructuredOutputColumns.map(_ => "")
      case _ =>
        Seq("")
    }

  private def completeForMode(userPrompt: String): ChatCompletionResult = {
    val toolSpecification = desc.normalizedOutputMode match {
      case AIAgentOutputMode.Structured =>
        Some(AIAgentFinalAnswerTools.structuredResult(desc.normalizedStructuredOutputFields))
      case AIAgentOutputMode.Classification =>
        Some(AIAgentFinalAnswerTools.textResult(desc.normalizedClassificationLabels))
      case _ if desc.normalizedTextClassificationLabels.nonEmpty =>
        Some(AIAgentFinalAnswerTools.textResult(desc.normalizedTextClassificationLabels))
      case _ =>
        None
    }

    val hasTools = tools.nonEmpty
    if (!hasTools && toolSpecification.isEmpty) {
      client.complete(
        desc.apiKey,
        desc.model,
        effectiveSystemPrompt,
        userPrompt,
        desc.temperature,
        desc.timeoutSeconds
      )
    } else {
      client.completeWithTools(
        desc.apiKey,
        desc.model,
        effectiveSystemPrompt,
        userPrompt,
        desc.temperature,
        desc.timeoutSeconds,
        tools,
        toolSpecification,
        desc.normalizedMaxToolIterations,
        desc.normalizedMaxRowCostUsd
      )
    }
  }

  private def buildUserPrompt(tuple: Tuple): String = {
    val inputColumns = Option(desc.inputColumn).getOrElse(List.empty).filter(_.trim.nonEmpty)
    require(inputColumns.nonEmpty, "At least one column must be sent to AI")
    inputColumns
      .map { column =>
        if (!tuple.getSchema.containsAttribute(column)) {
          throw new IllegalArgumentException(s"AI Agent references missing column: $column")
        }
        val value = Option(tuple.getField[Any](column)).map(_.toString).getOrElse("")
        s"$column: $value"
      }
      .mkString("\n")
  }

  private def effectiveSystemPrompt: String = {
    val basePrompt = Option(desc.systemPrompt).getOrElse("").trim
    val modePrompt = desc.normalizedOutputMode match {
      case AIAgentOutputMode.Structured =>
        val fields = desc.normalizedStructuredOutputFields
        require(fields.nonEmpty, "Structured output mode requires at least one output field")
        val fieldInstructions = fields
          .map { field =>
            val instructions = Option(field.instructions).getOrElse("").trim
            val classificationSuffix =
              if (field.normalizedFieldType == AIAgentStructuredFieldType.Classification) {
                val labels = field.normalizedClassificationLabels
                if (labels.isEmpty) {
                  " Choose a classification label."
                } else {
                  s" Choose exactly one of: ${labels.mkString(", ")}."
                }
              } else {
                ""
              }
            if (instructions.isEmpty) {
              s"- ${field.columnName.trim}: extract this value for the row.$classificationSuffix"
            } else {
              s"- ${field.columnName.trim}: $instructions$classificationSuffix"
            }
          }
          .mkString("\n")
        s"""Call the ${AIAgentFinalAnswerTools.SubmitStructuredResult} tool exactly once with the final structured result.
           |
           |Structured output fields:
           |$fieldInstructions""".stripMargin
      case AIAgentOutputMode.Classification =>
        val labels = desc.normalizedClassificationLabels
        require(labels.nonEmpty, "Classification mode requires at least one label")
        s"""Call the ${AIAgentFinalAnswerTools.SubmitTextResult} tool exactly once. The response value must exactly match one of these labels: ${labels
          .mkString(", ")}."""
      case _ if desc.normalizedTextClassificationLabels.nonEmpty =>
        s"""Call the ${AIAgentFinalAnswerTools.SubmitTextResult} tool exactly once. The response value must exactly match one of these labels: ${desc.normalizedTextClassificationLabels
          .mkString(", ")}."""
      case _ =>
        ""
    }

    List(basePrompt, modePrompt).filter(_.nonEmpty).mkString("\n\n")
  }

  private def outputFields(content: String): Seq[Any] =
    desc.normalizedOutputMode match {
      case AIAgentOutputMode.Structured =>
        AIAgentOutputParser.parseStructuredFields(content, desc.normalizedStructuredOutputFields)
      case AIAgentOutputMode.Classification =>
        Seq(AIAgentOutputParser.parseTextResult(content, desc.normalizedClassificationLabels))
      case _ if desc.normalizedTextClassificationLabels.nonEmpty =>
        Seq(AIAgentOutputParser.parseTextResult(content, desc.normalizedTextClassificationLabels))
      case _ =>
        Seq(content)
    }
}

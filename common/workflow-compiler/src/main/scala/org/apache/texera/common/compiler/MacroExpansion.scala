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
package org.apache.texera.common.compiler

import org.apache.texera.amber.core.workflow.WorkflowGraphIDs

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.common.compiler.model.{LogicalLink, LogicalPlanPojo}
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.UUID
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try

/** An unsaved body applies only to the named occurrence in a compile preview. */
case class MacroDraft(path: List[String], content: JsonNode)

case class MacroResultTarget(operatorID: String, portIndex: Int)

case class MacroInspectorInput(link: Int, input: Int, output: Int)
case class MacroInspectorPort(direction: String, portID: String, links: List[Int])
case class MacroInspector(
    ids: Map[String, String],
    inputs: Map[String, List[MacroInspectorInput]],
    terminals: List[MacroInspectorPort]
)

case class MacroBoundary(
    members: List[String],
    inputs: List[List[MacroResultTarget]],
    outputs: List[List[MacroResultTarget]],
    terminal: Boolean
)

@JsonIgnoreProperties(Array("names", "boundaries"))
case class MacroExpansion(
    content: JsonNode,
    paths: Map[String, List[String]],
    ports: Map[String, List[JsonNode]],
    names: Map[String, List[String]] = Map.empty,
    resultTargets: Map[String, MacroResultTarget] = Map.empty,
    boundaries: Map[String, MacroBoundary] = Map.empty,
    inspectors: Map[String, MacroInspector] = Map.empty
)

/** Expands immutable, self-contained workflow snapshots before ordinary logical compilation. */
object MacroExpansion {
  class InvalidMacro(val root: String, labels: List[String], cause: Throwable)
      extends IllegalArgumentException(s"${labels.mkString(" / ")}: ${cause.getMessage}", cause)
  private val namespace = UUID.fromString("33269d11-7448-5d6d-9aeb-68826068e94d")
  private def json(value: Any): JsonNode = objectMapper.valueToTree(value)
  private def text(node: JsonNode, key: String): String = node.path(key).asText("")
  private def values(node: JsonNode): List[JsonNode] = node.elements().asScala.toList
  private def array(node: JsonNode, key: String, message: String): List[JsonNode] = {
    require(node.path(key).isArray, message)
    values(node.path(key))
  }
  private def fields(node: JsonNode): List[JsonNode] = {
    val value = node.path("formBinding").path("fields")
    if (value.isMissingNode || value.isNull) Nil
    else {
      require(value.isArray, "Invalid exposed macro properties")
      values(value)
    }
  }
  private def isMacro(op: JsonNode): Boolean = text(op, "operatorType").startsWith("WorkflowMacro:")
  private def isTerminal(op: JsonNode): Boolean =
    Set("MacroInput", "MacroOutput")(text(op, "operatorType"))

  /** The public interface is derived from the saved terminal ports. */
  private def boundary(body: JsonNode, direction: String): List[JsonNode] = {
    val input = direction == "inputs"
    val terminal = values(body.path("operators")).filter(op =>
      text(op, "operatorType") == (if (input) "MacroInput" else "MacroOutput")
    )
    require(terminal.size == 1, "A macro must contain exactly one Inputs and one Outputs operator")
    values(terminal.head.path(if (input) "outputPorts" else "inputPorts")).map { port =>
      port
        .deepCopy[ObjectNode]()
        .set[JsonNode](
          "targets",
          json(
            List(
              Map(
                "operatorID" -> text(terminal.head, "operatorID"),
                "portID" -> text(port, "portID")
              )
            )
          )
        )
    }
  }

  private def portKey(id: String, direction: String, port: String): String =
    json(List(id, direction, port)).toString
  private def runtimeID(value: JsonNode): String = {
    val bytes = ByteBuffer
      .allocate(16)
      .putLong(namespace.getMostSignificantBits)
      .putLong(namespace.getLeastSignificantBits)
      .array()
    val digest = MessageDigest.getInstance("SHA-1")
    digest.update(bytes)
    val hash = digest.digest(value.toString.getBytes(UTF_8))
    hash(6) = ((hash(6) & 0x0f) | 0x50).toByte
    hash(8) = ((hash(8) & 0x3f) | 0x80).toByte
    val buffer = ByteBuffer.wrap(hash)
    new UUID(buffer.getLong(), buffer.getLong()).toString
  }
  def operatorID(chain: List[String], id: String): String =
    if (chain.isEmpty) id else runtimeID(json(chain :+ id))

  private def nativeDescriptors(expansion: MacroExpansion): Map[String, Try[LogicalOp]] =
    values(expansion.content.path("operators")).map { op =>
      val descriptor = op.path("operatorProperties").deepCopy[ObjectNode]()
      List("operatorID", "operatorType", "inputPorts", "outputPorts").foreach(k =>
        descriptor.set[JsonNode](k, op.path(k))
      )
      text(op, "operatorID") -> Try(objectMapper.treeToValue(descriptor, classOf[LogicalOp]))
    }.toMap

  def validateProperties(operator: JsonNode, dependencies: List[JsonNode]): Unit = {
    require(operator != null && isMacro(operator), "Expected a macro operator")
    val root = json(
      Map(
        "operators" -> List(operator.deepCopy[ObjectNode]().put("isDisabled", false)),
        "links" -> Nil,
        "macroDependencies" -> dependencies
      )
    )
    val expanded = expand(root, propertiesOnly = true)
    MacroValidation.validate(expanded, nativeDescriptors(expanded), propertiesOnly = true)
  }

  def logicalPlan(
      pojo: LogicalPlanPojo,
      expansion: MacroExpansion,
      errorList: Option[mutable.Buffer[(OperatorIdentity, Throwable)]] = None
  ): LogicalPlanPojo = {
    val all = values(expansion.content.path("operators"))
    val descriptors = nativeDescriptors(expansion)
    try MacroValidation.validate(expansion, descriptors)
    catch {
      case error: MacroValidation.ValidationErrors if errorList.isDefined =>
        errorList.foreach(_ ++= error.errors)
    }
    val decoded = all
      .filter(op =>
        pojo.executeOperatorIds.forall(_.contains(expansion.paths(text(op, "operatorID")).head))
      )
      .flatMap { op =>
        try Some(text(op, "operatorID") -> descriptors(text(op, "operatorID")).get)
        catch {
          case scala.util.control.NonFatal(error) if errorList.isDefined =>
            errorList.foreach(_ += OperatorIdentity(text(op, "operatorID")) -> error)
            None
        }
      }
      .toMap
    val operators = all.filter(op => decoded.contains(text(op, "operatorID")))
    val byID = operators.map(op => text(op, "operatorID") -> op).toMap
    val links = values(expansion.content.path("links"))
      .filter(link =>
        byID.contains(text(link.path("source"), "operatorID")) &&
          byID.contains(text(link.path("target"), "operatorID"))
      )
      .map { link =>
        def ordinal(end: JsonNode, direction: String): PortIdentity =
          PortIdentity(
            values(byID(text(end, "operatorID")).path(direction))
              .indexWhere(text(_, "portID") == text(end, "portID"))
          )
        LogicalLink(
          OperatorIdentity(text(link.path("source"), "operatorID")),
          ordinal(link.path("source"), "outputPorts"),
          OperatorIdentity(text(link.path("target"), "operatorID")),
          ordinal(link.path("target"), "inputPorts")
        )
      }
    val results = pojo.opsToViewResult.flatMap { id =>
      if (byID.contains(id)) List(id)
      else
        pojo.macroContent.toList
          .flatMap(c => values(c.path("operators")))
          .find(text(_, "operatorID") == id)
          .toList
          .flatMap(op => values(op.path("outputPorts")))
          .flatMap(port =>
            expansion.ports.getOrElse(portKey(id, "outputs", text(port, "portID")), Nil)
          )
          .map(text(_, "operatorID"))
    } ++ operators.filter(_.path("viewResult").asBoolean()).map(text(_, "operatorID"))
    LogicalPlanPojo(
      operators.map(op => decoded(text(op, "operatorID"))),
      links,
      results.filter(byID.contains).distinct,
      operators
        .filter(op =>
          op.path("markedForReuse").asBoolean() || pojo.opsToReuseResult
            .contains(expansion.paths(text(op, "operatorID")).head)
        )
        .map(text(_, "operatorID")),
      expandedMacros = Some(expansion)
    )
  }

  private case class Definition(id: String, name: String, content: JsonNode)

  /** Index immutable captures without requiring a runnable graph. Drafts still have usages. */
  private def indexDefinitions(
      content: JsonNode,
      additional: Iterable[JsonNode] = Nil
  ): Map[String, Definition] = {
    val definitions = mutable.Map.empty[String, Definition]
    def collect(definition: JsonNode): Unit = {
      val version = definition.path("version")
      require(
        definition.path("id").isTextual && text(definition, "id").nonEmpty &&
          definition.path("name").isTextual && version.isNumber &&
          version.decimalValue().stripTrailingZeros().scale() <= 0 &&
          version.decimalValue().signum() > 0 &&
          version.decimalValue().compareTo(java.math.BigDecimal.valueOf(9007199254740991L)) <= 0 &&
          definition.path("content").path("macroInterface").path("id") == definition.path("id"),
        "Malformed macro definition"
      )
      val id = text(definition, "id")
      val body = objectMapper
        .createObjectNode()
        .setAll[ObjectNode](definition.get("content").asInstanceOf[ObjectNode])
      body.remove("macroDependencies")
      val key = s"WorkflowMacro:$id:${version.asLong()}"
      require(definitions.get(key).forall(_.content == body), "Conflicting macro version")
      definitions(key) = Definition(id, text(definition, "name"), body)
      collectBundle(definition.path("content"))
    }
    def collectBundle(body: JsonNode): Unit = {
      if (body.hasNonNull("macroDependencies"))
        array(body, "macroDependencies", "Malformed macro definitions").foreach(collect)
    }
    collectBundle(content)
    additional.foreach(collectBundle)
    definitions.toMap
  }

  case class Reference(id: String, path: List[String], names: List[String])
  def references(content: JsonNode): List[Reference] = {
    val captures = indexDefinitions(content)
    def visit(
        body: JsonNode,
        path: List[String],
        names: List[String],
        ancestors: Set[String]
    ): List[Reference] = {
      require(path.size <= 32, "Macro nesting exceeds 32 levels")
      array(body, "operators", "Malformed workflow operators").filter(isMacro).flatMap { op =>
        val key = text(op, "operatorType")
        val definition = captures.getOrElse(
          key,
          throw new IllegalArgumentException(s"Missing referenced macro definition: $key")
        )
        val next = Reference(
          definition.id,
          path :+ text(op, "operatorID"),
          names :+ op.path("customDisplayName").asText(definition.name)
        )
        next :: (if (ancestors(key)) Nil
                 else visit(definition.content, next.path, next.names, ancestors + key))
      }
    }
    visit(content, Nil, Nil, Set.empty)
  }

  def expand(
      content: JsonNode,
      drafts: List[MacroDraft] = Nil,
      propertiesOnly: Boolean = false
  ): MacroExpansion = {
    require(content != null && content.isObject, "Malformed macro graph")
    WorkflowGraphIDs.validate(content)
    val draftBodies = drafts.map { draft =>
      require(
        draft != null && draft.path != null && draft.path.nonEmpty && draft.path.length <= 32 &&
          draft.path.forall(id => id != null && id.trim.nonEmpty),
        "Invalid macro draft path"
      )
      WorkflowGraphIDs.validate(draft.content)
      draft.path -> draft.content
    }.toMap
    require(draftBodies.size == drafts.size, "Duplicate macro draft path")
    val definitions = indexDefinitions(content, drafts.map(_.content))

    def bodyFor(op: JsonNode, path: List[String]): Option[JsonNode] =
      definitions
        .get(text(op, "operatorType"))
        .map(definition => draftBodies.getOrElse(path, definition.content))

    // Check even disabled occurrences, which the executable graph deliberately skips.
    drafts.foreach { draft =>
      var parent = content
      var path = List.empty[String]
      draft.path.foreach { id =>
        path = path :+ id
        val op = values(parent.path("operators")).find(text(_, "operatorID") == id)
        val pinned = op.flatMap(o => definitions.get(text(o, "operatorType"))).getOrElse {
          throw new IllegalArgumentException(s"Unknown macro draft: ${path.mkString(" / ")}")
        }
        parent = draftBodies.getOrElse(path, pinned.content)
        require(
          parent.at("/macroInterface/id") == pinned.content.at("/macroInterface/id"),
          "Macro draft identity differs from its pinned definition"
        )
      }
    }
    val operators = mutable.ListBuffer.empty[JsonNode]
    val links = mutable.ListBuffer.empty[JsonNode]
    val paths = mutable.LinkedHashMap.empty[String, List[String]]
    val names = mutable.LinkedHashMap.empty[String, List[String]]
    val ports = mutable.LinkedHashMap.empty[String, List[JsonNode]]
    val runtimeIDs = mutable.HashSet.empty[String]
    val terminals = mutable.Map.empty[String, String]
    val boundaryOps = mutable.LinkedHashMap.empty[String, (List[String], JsonNode)]
    val sinkTargets = mutable.Map.empty[String, MacroResultTarget]

    def visit(
        body: JsonNode,
        chain: List[String],
        stack: Set[String],
        labels: List[String],
        overrides: JsonNode = objectMapper.createObjectNode()
    ): Map[String, List[JsonNode]] = {
      val ops = array(body, "operators", "Malformed macro graph")
      val edges = if (propertiesOnly) Nil else array(body, "links", "Malformed macro graph")
      require(chain.size <= 32, "Macro nesting exceeds 32 levels")
      val byID = mutable.LinkedHashMap.empty[String, JsonNode]
      def checkPorts(group: List[JsonNode], message: String): Unit =
        require(
          group.forall(p => p.path("portID").isTextual && text(p, "portID").nonEmpty) &&
            group.map(text(_, "portID")).distinct.size == group.size,
          message
        )
      ops.foreach { op =>
        val id = text(op, "operatorID")
        require(
          op.path("operatorType").isTextual && op.path("operatorProperties").isObject,
          "Malformed operator"
        )
        List("inputPorts", "outputPorts").foreach { direction =>
          checkPorts(
            array(op, direction, "Malformed operator"),
            "Duplicate or invalid operator port"
          )
        }
        byID(id) = op
      }
      val savedTerminals = ops.filter(isTerminal)
      if (savedTerminals.nonEmpty) {
        require(body.hasNonNull("macroInterface"), "Inputs/Outputs belong in a macro definition")
        require(
          !body.path("macroInterface").has("inputs") && !body.path("macroInterface").has("outputs"),
          "The macro interface must be stored in terminal operators"
        )
        List("inputs", "outputs").foreach(boundary(body, _))
        savedTerminals.foreach { op =>
          require(!op.path("isDisabled").asBoolean(), "Macro terminals cannot be disabled")
          val input = text(op, "operatorType") == "MacroInput"
          require(
            op.path(if (input) "inputPorts" else "outputPorts").isEmpty,
            "Invalid macro terminal direction"
          )
          if (!propertiesOnly) values(op.path(if (input) "outputPorts" else "inputPorts")).foreach {
            port =>
              val matches = edges.count(link => {
                val end = link.path(if (input) "source" else "target")
                text(end, "operatorID") == text(op, "operatorID") && text(end, "portID") == text(
                  port,
                  "portID"
                )
              })
              val name = port.path("displayName").asText(text(port, "portID"))
              if (matches == 0)
                throw new IllegalArgumentException(
                  s"Connect or remove macro ${if (input) "input" else "output"} '$name' before running."
                )
              if (!input && matches != 1)
                throw new IllegalArgumentException(
                  s"Macro output '$name' must connect to exactly one operator output."
                )
          }
        }
      }
      if (body.hasNonNull("macroInterface")) {
        val boundary = body.path("macroInterface")
        require(
          boundary.path("id").isTextual && text(boundary, "id").nonEmpty,
          "Invalid macro interface ID"
        )
        List("inputs", "outputs").foreach(MacroExpansion.boundary(body, _))
        val exposed = fields(body)
        require(
          exposed.map(text(_, "id")).distinct.size == exposed.size,
          "Duplicate exposed macro property"
        )
        val targets = exposed.map { field =>
          require(
            field.path("id").isTextual && text(field, "id").nonEmpty &&
              field.path("propertyKey").isTextual && byID.contains(text(field, "operatorID")),
            "Invalid exposed macro property"
          )
          val target = byID(text(field, "operatorID"))
          require(
            !isMacro(target) || bodyFor(target, chain :+ text(target, "operatorID"))
              .exists(d => fields(d).exists(text(_, "id") == text(field, "propertyKey"))),
            s"Missing exposed macro property: ${field.path("displayName").asText(text(field, "id"))}"
          )
          text(field, "id") -> (field, target)
        }.toMap
        overrides.fields().asScala.foreach { property =>
          val (binding, target) = targets.getOrElse(
            property.getKey,
            throw new IllegalArgumentException(s"Unknown macro property: ${property.getKey}")
          )
          target
            .path("operatorProperties")
            .asInstanceOf[ObjectNode]
            .set[JsonNode](text(binding, "propertyKey"), property.getValue.deepCopy[JsonNode]())
        }
      }
      val resolved = mutable.LinkedHashMap.empty[String, List[JsonNode]]
      def endpoint(port: JsonNode, direction: String): List[JsonNode] = {
        val id = text(port, "operatorID")
        val portID = text(port, "portID")
        require(
          port.path("operatorID").isTextual && port.path("portID").isTextual &&
            byID
              .get(id)
              .exists(
                _.path(if (direction == "inputs") "inputPorts" else "outputPorts")
                  .elements()
                  .asScala
                  .exists(text(_, "portID") == portID)
              ),
          "Missing operator or port in macro graph"
        )
        if (byID(id).path("isDisabled").asBoolean()) Nil
        else
          resolved.getOrElse(
            portKey(id, direction, portID),
            throw new IllegalArgumentException("Missing exposed macro port")
          )
      }
      ops.filterNot(_.path("isDisabled").asBoolean()).foreach { op =>
        val id = text(op, "operatorID")
        val label = Option(text(op, "customDisplayName"))
          .filter(_.trim.nonEmpty)
          .getOrElse(
            definitions
              .get(text(op, "operatorType"))
              .map(_.name)
              .getOrElse(MacroValidation.operatorName(text(op, "operatorType")))
          )
        val runtime = operatorID(chain, id)
        require(runtimeIDs.add(runtime), "Duplicate runtime operator ID")
        if (isTerminal(op) || isMacro(op)) boundaryOps(runtime) = (chain :+ id, op)
        if (isTerminal(op)) {
          terminals(runtime) = chain.headOption.getOrElse(id)
          names(runtime) = labels :+ label
          values(
            op.path(if (text(op, "operatorType") == "MacroInput") "outputPorts" else "inputPorts")
          ).foreach { port =>
            val end = json(Map("operatorID" -> runtime, "portID" -> text(port, "portID")))
            List("inputs", "outputs").foreach { direction =>
              resolved(portKey(id, direction, text(port, "portID"))) = List(end)
              ports(portKey(runtime, direction, text(port, "portID"))) = List(end)
            }
          }
        } else if (!isMacro(op)) {
          operators += op.deepCopy[ObjectNode]().put("operatorID", runtime)
          paths(runtime) = chain :+ id
          if (text(op, "operatorType").toLowerCase(java.util.Locale.ROOT).contains("sink"))
            chain.inits
              .filter(_.nonEmpty)
              .foreach(path =>
                sinkTargets
                  .getOrElseUpdate(operatorID(path.init, path.last), MacroResultTarget(runtime, 0))
              )
          names(runtime) = labels :+ label
          List("inputs", "outputs").foreach { direction =>
            op.path(if (direction == "inputs") "inputPorts" else "outputPorts")
              .elements()
              .asScala
              .foreach { port =>
                resolved(portKey(id, direction, text(port, "portID"))) = List(
                  objectMapper
                    .createObjectNode()
                    .put("operatorID", runtime)
                    .put("portID", text(port, "portID"))
                )
              }
          }
        } else {
          try {
            val key = text(op, "operatorType")
            val path = chain :+ id
            val inner = bodyFor(op, path)
              .getOrElse(throw new IllegalArgumentException(s"Missing macro version: $key"))
              .deepCopy[ObjectNode]()
            require(
              op.path("operatorVersion")
                .isTextual && text(op, "operatorVersion") == key.split(":").last,
              "Macro version does not match its pinned definition"
            )
            // An occurrence override is a distinct body. Its children may legitimately use the saved pin.
            val ancestors =
              if (draftBodies.contains(path)) Set.empty[String]
              else {
                require(!stack(key), "Recursive macro reference")
                stack + key
              }
            val innerPorts =
              visit(inner, path, ancestors, labels :+ label, op.path("operatorProperties"))
            List("inputs", "outputs").foreach { direction =>
              val boundary =
                MacroExpansion.boundary(inner, direction)
              val declared = array(
                op,
                if (direction == "inputs") "inputPorts" else "outputPorts",
                "Malformed operator"
              )
              require(
                declared.size == boundary.size && declared
                  .forall(p => boundary.exists(text(_, "portID") == text(p, "portID"))),
                "Macro instance interface differs from its pinned version"
              )
              boundary.foreach { port =>
                val targets = port
                  .path("targets")
                  .elements()
                  .asScala
                  .toList
                  .flatMap(t =>
                    innerPorts
                      .getOrElse(portKey(text(t, "operatorID"), direction, text(t, "portID")), Nil)
                  )
                resolved(portKey(id, direction, text(port, "portID"))) = targets
                // Nested boundaries let every inspector project this same authoritative expansion.
                ports(portKey(runtime, direction, text(port, "portID"))) = targets
              }
            }
          } catch {
            case error: InvalidMacro => throw error
            case scala.util.control.NonFatal(error) =>
              throw new InvalidMacro(
                chain.headOption.getOrElse(id),
                labels :+ label,
                error
              )
          }
        }
      }
      edges.foreach { link =>
        val sources = endpoint(link.path("source"), "outputs")
        val targets = endpoint(link.path("target"), "inputs")
        for (source <- sources; target <- targets) {
          val id =
            if (chain.nonEmpty || sources.size * targets.size > 1)
              runtimeID(json(List(json(chain), link.path("linkID"), source, target)))
            else text(link, "linkID")
          links += objectMapper
            .createObjectNode()
            .put("linkID", id)
            .set[ObjectNode]("source", source)
            .set[ObjectNode]("target", target)
        }
      }
      resolved.toMap
    }
    visit(content, Nil, Set.empty, Nil)
    // Draft property checks end at native descriptors. Links, regions and runtime mappings belong to compilation.
    if (propertiesOnly)
      return MacroExpansion(
        json(Map("operators" -> operators.toList)),
        paths.toMap,
        Map.empty,
        names.toMap
      )
    // Terminals are junctions, never workers. Collapse them only after the entire parent graph
    // is wired, so passthrough macros and mixed old/new definitions use the same lowering.
    if (terminals.nonEmpty) {
      def key(end: JsonNode): String =
        portKey(text(end, "operatorID"), "inputs", text(end, "portID"))
      val forward = links.toList.groupBy(link => key(link.path("source")))
      val backward = links.toList.groupBy(link => key(link.path("target")))
      def follow(
          end: JsonNode,
          downstream: Boolean,
          visiting: Set[String] = Set.empty
      ): List[JsonNode] = {
        if (!terminals.contains(text(end, "operatorID"))) List(end)
        else {
          val id = key(end)
          if (visiting(id))
            throw new InvalidMacro(
              terminals(text(end, "operatorID")),
              names(text(end, "operatorID")),
              new IllegalArgumentException("Workflow contains a cycle through macro terminals")
            )
          (if (downstream) forward else backward)
            .getOrElse(id, Nil)
            .flatMap(link =>
              follow(link.path(if (downstream) "target" else "source"), downstream, visiting + id)
            )
            .distinct
        }
      }
      ports.keys.toList.foreach { key =>
        val direction = objectMapper.readTree(key).get(1).asText()
        ports(key) = ports(key).flatMap(follow(_, direction == "inputs")).distinct
      }
      val lowered = links.toList
        .filterNot(link => terminals.contains(text(link.path("source"), "operatorID")))
        .flatMap { link =>
          follow(link.path("target"), downstream = true).map { target =>
            if (target == link.path("target")) link
            else
              link
                .deepCopy[ObjectNode]()
                .put("linkID", runtimeID(json(List(link.path("source"), target))))
                .set[JsonNode]("target", target)
          }
        }
      links.clear()
      links ++= lowered.distinct
    }
    val children = links.toList.groupBy(_.path("source").path("operatorID").asText())
    val visiting = mutable.Set.empty[String]
    val visited = mutable.Set.empty[String]
    def checkCycle(id: String): Unit = {
      if (visiting(id))
        throw new InvalidMacro(
          paths(id).head,
          names(id),
          new IllegalArgumentException("Workflow contains a cycle")
        )
      if (!visited(id)) {
        visiting += id
        children
          .getOrElse(id, Nil)
          .foreach(link => checkCycle(text(link.path("target"), "operatorID")))
        visiting -= id
        visited += id
      }
    }
    paths.keys.foreach(checkCycle)
    val expanded = content.deepCopy[ObjectNode]()
    expanded.set[JsonNode]("operators", json(operators.toList))
    expanded.set[JsonNode]("links", json(links.toList))
    expanded.remove(List("macroInterface", "macroDependencies").asJava)
    val native = operators.iterator.map(op => text(op, "operatorID") -> op).toMap
    val members =
      paths.toList.flatMap { case (id, path) => path.init.inits.map(_ -> id) }.groupMap(_._1)(_._2)
    val ordinals = native.toList.flatMap {
      case (id, op) =>
        List(true, false).flatMap { input =>
          values(op.path(if (input) "inputPorts" else "outputPorts")).zipWithIndex.map {
            case (port, index) =>
              (id, input, text(port, "portID")) -> index
          }
        }
    }.toMap
    def nativePort(end: JsonNode, input: Boolean): MacroResultTarget = {
      val id = text(end, "operatorID")
      MacroResultTarget(id, ordinals.getOrElse((id, input, text(end, "portID")), -1))
    }
    val boundaries = boundaryOps.map {
      case (id, (path, op)) =>
        val terminal = isTerminal(op)
        def resolve(publicPorts: List[String], input: Boolean): List[List[MacroResultTarget]] =
          publicPorts.map { port =>
            ports
              .getOrElse(portKey(id, if (input) "inputs" else "outputs", port), Nil)
              .map(nativePort(_, input))
          }
        id -> MacroBoundary(
          members.getOrElse(path, Nil),
          resolve(values(op.path("inputPorts")).map(text(_, "portID")), !terminal),
          resolve(values(op.path("outputPorts")).map(text(_, "portID")), input = false),
          terminal
        )
    }.toMap
    val targets = boundaries
      .collect {
        case (id, b) if !b.terminal =>
          b.outputs.headOption.flatMap(_.headOption).orElse(sinkTargets.get(id)).map(id -> _)
      }
      .flatten
      .toMap
    // Index each occurrence against one native graph. Ancestors share its operator bodies and links.
    val nativePaths = paths.iterator.map { case (id, path) => id -> (path, native(id)) }.toMap
    val direct = (nativePaths ++ boundaryOps).toList.groupBy(_._2._1.init)
    val endpoints = links.iterator
      .map(link =>
        nativePort(link.path("source"), input = false) -> nativePort(
          link.path("target"),
          input = true
        )
      )
      .toVector
    val byTarget = endpoints.indices.groupBy(i => endpoints(i)._2)
    val bySource = endpoints.indices.groupBy(i => endpoints(i)._1)
    val terminalsByScope = boundaryOps.collect {
      case (id, (path, op)) if isTerminal(op) =>
        (path.init, text(op, "operatorType") == "MacroInput") -> id
    }.toMap
    def between(from: Set[MacroResultTarget], to: Set[MacroResultTarget]): List[Int] = {
      // At a fan-in or fan-out, visit only the cheaper set of candidate links.
      val incoming = to.toList.flatMap(byTarget.get)
      val outgoing = from.toList.flatMap(bySource.get)
      val indices =
        if (outgoing.iterator.map(_.size).sum <= incoming.iterator.map(_.size).sum)
          outgoing.iterator.flatten.filter(i => to(endpoints(i)._2))
        else incoming.iterator.flatten.filter(i => from(endpoints(i)._1))
      indices.toList.sorted
    }
    val crossings = boundaryOps.values.collect {
      case (scope, op) if isMacro(op) =>
        scope -> List(true, false).flatMap { input =>
          val direction = if (input) "inputs" else "outputs"
          val terminal = terminalsByScope((scope, input))
          values(op.path(if (input) "inputPorts" else "outputPorts")).map { port =>
            val portID = text(port, "portID")
            val from = ports
              .getOrElse(portKey(terminal, "outputs", portID), Nil)
              .map(nativePort(_, input = false))
              .toSet
            val to = ports
              .getOrElse(portKey(terminal, "inputs", portID), Nil)
              .map(nativePort(_, input = true))
              .toSet
            MacroInspectorPort(direction, portID, between(from, to))
          }
        }
    }.toMap
    val inspectors = (List.empty[String] :: crossings.keys.toList).map { scope =>
      val canvas = direct.getOrElse(scope, Nil)
      val leaves = members.getOrElse(scope, Nil)
      val ids = leaves.map { runtime =>
        val relative = paths(runtime).drop(scope.size)
        operatorID(relative.init, relative.last) -> runtime
      }.toMap ++ canvas.map { case (runtime, (path, _)) => path.last -> runtime }
      val inputs = canvas.flatMap {
        case (runtime, (path, op)) =>
          val refs = if (isMacro(op)) crossings(path).collect {
            case p if p.direction == "inputs" => p.links
          }
          else
            values(op.path("inputPorts")).indices.toList.map(input =>
              byTarget.getOrElse(MacroResultTarget(runtime, input), Nil)
            )
          val incoming = refs.zipWithIndex.flatMap {
            case (links, input) =>
              links.map(i => MacroInspectorInput(i, input, endpoints(i)._1.portIndex))
          }
          Option.when(incoming.nonEmpty)(path.last -> incoming)
      }.toMap
      val visible = leaves.toSet
      val external = crossings.getOrElse(scope, Nil).map { port =>
        port.copy(links =
          port.links.filterNot(i =>
            visible(
              (if (port.direction == "inputs") endpoints(i)._1 else endpoints(i)._2).operatorID
            )
          )
        )
      }
      json(scope).toString -> MacroInspector(ids, inputs, external)
    }.toMap
    MacroExpansion(expanded, paths.toMap, ports.toMap, names.toMap, targets, boundaries, inspectors)
  }
}

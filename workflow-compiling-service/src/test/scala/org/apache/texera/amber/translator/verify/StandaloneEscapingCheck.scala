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

package org.apache.texera.amber.translator.verify

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.apache.texera.amber.operator.source.SourceOperatorDescriptor
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.sys.process.{Process, ProcessLogger}
import scala.util.Try

/**
  * Configures every operator against hostile column names and parses what
  * `generateStandaloneCode` produces. A value spliced in without
  * `pyStringLiteral` is silent until someone names a column `a"b`, and then the
  * exported script will not compile.
  *
  * The check has to be behavioural. A generator that builds its quoted literal
  * inside a helper shows no quotes in its template, which is how RadarPlot and
  * Aggregate survived a source-level sweep that reported zero remaining sites.
  */
object StandaloneEscapingCheck {

  private val schemas = CanonicalFixture.schemasByPort
  private val columns = CanonicalFixture.schema.getAttributes.map(_.getName).toSet

  /** Every problem found, empty when the suite is clean. An operator whose config
    * cannot be built lands here too: dropping it would check less while still
    * passing.
    */
  def run(): Seq[String] = {
    // Sources have no input schema and so no column knobs. Their free-text knobs
    // already get a hostile variant in the normal verify run.
    val operators = OperatorBehaviorSpec
      .discoverStandaloneOperators()
      .filterNot(classOf[SourceOperatorDescriptor].isAssignableFrom)
    val dir = Files.createTempDirectory("standalone-escaping-")
    dir.toFile.deleteOnExit()

    val (unconfigurable, code) = operators
      .map(op => op.getSimpleName -> codeFor(op, dir))
      .partitionMap {
        case (name, Left(why)) => Left(s"$name: $why")
        case (name, Right(variants)) =>
          Right(variants.map { case (label, src) => s"$name/$label" -> src })
      }
    unconfigurable ++ parse(code.flatten, dir)
  }

  /** Holds the four characters that end a Python literal, and carries the column
    * it replaces so that two knobs never collide on one name.
    */
  private def hostile(column: String): String = "a\"b'c\\d\ne_" + column

  private def hostilize(node: JsonNode): Unit =
    node match {
      case obj: ObjectNode =>
        obj.fields().asScala.toSeq.foreach { e =>
          if (isColumn(e.getValue)) obj.put(e.getKey, hostile(e.getValue.asText))
          else hostilize(e.getValue)
        }
      case arr: ArrayNode =>
        (0 until arr.size).foreach { i =>
          if (isColumn(arr.get(i)))
            arr.set(i, objectMapper.getNodeFactory.textNode(hostile(arr.get(i).asText)))
          else hostilize(arr.get(i))
        }
      case _ => ()
    }

  private def isColumn(n: JsonNode): Boolean = n.isTextual && columns.contains(n.asText)

  /** Split the way the runner splits: a curated operator's config is hand-written
    * because the generator cannot derive one. Auto operators contribute every
    * variant, since a knob only `optionals` fills is a site the base never reaches.
    */
  private[verify] def codeFor(
      opClass: Class[_ <: LogicalOp],
      dir: Path
  ): Either[String, Seq[(String, String)]] = {
    val configs = CuratedHandlers.byClass.get(opClass) match {
      // The handler's config, not its enum sweep: sweeping moves an enum value,
      // never a column name, and would need schemas only the runner holds.
      case Some(h) =>
        Try(Seq("curated" -> h.fixture(dir)._1)).toEither.left.map(e => s"curated: ${e.getMessage}")
      case None => ConfigGenerator.generateVariants(opClass, schemas)
    }
    configs.flatMap { variants =>
      val results = variants.map {
        case (label, op) =>
          val node = objectMapper.valueToTree[ObjectNode](op)
          hostilize(node)
          Try(
            objectMapper
              .treeToValue(node, opClass)
              .asInstanceOf[StandaloneCodeGenerator]
              .generateStandaloneCode()
          ).toEither.left.map(e => s"$label: $e").map(label -> _)
      }
      results.collectFirst { case Left(why) => why }.toLeft(results.collect { case Right(r) => r })
    }
  }

  /** One Python process for all of them; the cost is startup, not parsing. The
    * snippets stay in `dir` so a reported operator can be opened as generated.
    */
  private def parse(snippets: Seq[(String, String)], dir: Path): Seq[String] = {
    val payload = objectMapper.createObjectNode()
    snippets.foreach { case (name, src) => payload.put(name, src) }
    val input = write(dir, "snippets.json", payload.toString)
    val script = write(
      dir,
      "parse_all.py",
      """import ast, json, sys
        |for name, code in json.load(open(sys.argv[1])).items():
        |    try:
        |        ast.parse(code)
        |    except SyntaxError as e:
        |        print(f"{name}: line {e.lineno}: {e.msg}")
        |""".stripMargin
    )
    val out = Seq.newBuilder[String]
    val err = Seq.newBuilder[String]
    val python = sys.env.get("UDF_PYTHON_PATH").filter(_.nonEmpty).getOrElse("python3.12")
    val exit = Process(Seq(python, script.toString, input.toString))
      .!(ProcessLogger(out += _, err += _))
    // Without this a parser that never ran reads as "nothing failed".
    require(exit == 0, s"parse_all.py exited $exit: ${err.result().mkString("\n")}")
    out.result()
  }

  private def write(dir: Path, name: String, content: String): Path =
    Files.write(dir.resolve(name), content.getBytes(UTF_8))
}

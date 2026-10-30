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

package org.apache.texera.operatorreference

import org.apache.texera.amber.operator.metadata.{
  GroupInfo,
  OperatorMetadata,
  OperatorMetadataGenerator
}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._

/**
  * Regenerates the operator reference pages from the descriptors on the classpath. Run it from
  * the repository root, where the default output directory resolves:
  *
  *   sbt "OperatorReferenceGenerator/runMain org.apache.texera.operatorreference.OperatorDocScanner [outputDir]"
  *
  * Every markdown file under the output directory is replaced, except the hand-written
  * `output-modes.md`.
  */
object OperatorDocScanner {

  private val DefaultOutputDir = "docs/reference/operators"
  private val HandWrittenPages = Set("output-modes.md")
  // Placeholder operator the frontend never lists; it gets no renderPage.
  private val HiddenOperatorTypes = Set("Dummy")

  private val LicenseHeader =
    """<!--
      |  ~ Licensed to the Apache Software Foundation (ASF) under one
      |  ~ or more contributor license agreements.  See the NOTICE file
      |  ~ distributed with this work for additional information
      |  ~ regarding copyright ownership.  The ASF licenses this file
      |  ~ to you under the Apache License, Version 2.0 (the
      |  ~ "License"); you may not use this file except in compliance
      |  ~ with the License.  You may obtain a copy of the License at
      |  ~
      |  ~   http://www.apache.org/licenses/LICENSE-2.0
      |  ~
      |  ~ Unless required by applicable law or agreed to in writing,
      |  ~ software distributed under the License is distributed on an
      |  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
      |  ~ KIND, either express or implied.  See the License for the
      |  ~ specific language governing permissions and limitations
      |  ~ under the License.
      |-->
      |
      |""".stripMargin

  private case class ParameterSet(enumClass: Class[_], operators: List[OperatorMetadata])

  private lazy val operatorClasses: Map[String, Class[_]] =
    OperatorMetadataGenerator.operatorTypeMap.map { case (clazz, name) => name -> clazz }

  private lazy val groupPaths: Map[String, String] = {
    def walk(groups: List[GroupInfo], prefix: String): Map[String, String] =
      groups.flatMap { g =>
        val path = if (prefix.isEmpty) toSlug(g.groupName) else s"$prefix/${toSlug(g.groupName)}"
        Map(g.groupName -> path) ++ walk(childGroups(g), path)
      }.toMap
    walk(OperatorMetadataGenerator.allOperatorMetadata.groups, "")
  }

  def main(args: Array[String]): Unit = {
    val outputDir = Paths.get(args.headOption.getOrElse(DefaultOutputDir))
    val metadata = OperatorMetadataGenerator.allOperatorMetadata
    val operators =
      metadata.operators.filterNot(op => HiddenOperatorTypes.contains(op.operatorType))
    val byGroup = operators.groupBy(op => groupPath(op.additionalMetadata.operatorGroupName))

    deleteGeneratedPages(outputDir)

    operators.foreach { op =>
      val dir = groupPath(op.additionalMetadata.operatorGroupName)
      writeFile(outputDir.resolve(dir).resolve(s"${operatorSlug(op)}.md"), operatorPage(op, dir))
    }
    metadata.groups.zipWithIndex.foreach {
      case (group, i) => writeGroupIndexes(outputDir, group, Nil, i + 1, byGroup)
    }

    val parameterSets = operators
      .flatMap(op =>
        JsonPropertyReflectionUtils.getEnumClass(operatorClasses(op.operatorType)).map(_ -> op)
      )
      .groupBy(_._1)
      .map { case (enumClass, pairs) => ParameterSet(enumClass, pairs.map(_._2)) }
      .toList
      .sortBy(_.enumClass.getSimpleName)
    parameterSets.foreach { set =>
      val file = s"${JsonPropertyReflectionUtils.parameterPageSlug(set.enumClass)}.md"
      writeFile(outputDir.resolve("parameters").resolve(file), parameterPage(set))
    }
    writeFile(outputDir.resolve("parameters/_index.md"), parameterIndex(parameterSets))
    writeFile(outputDir.resolve("_index.md"), rootIndex(metadata.groups, byGroup))

    println(
      s"Wrote ${operators.size} operator pages, ${groupPaths.size} group indexes and " +
        s"${parameterSets.size} parameter pages to $outputDir"
    )
  }

  private def operatorPage(op: OperatorMetadata, dir: String): String = {
    val info = op.additionalMetadata
    val depth = dir.count(_ == '/') + 1
    val rootPath = "../" * (depth + 1)
    val operatorClass = operatorClasses(op.operatorType)

    val frontMatter = List(
      Some(s"""title: "${escapeYamlString(info.userFriendlyName)}""""),
      Option(info.operatorDescription)
        .filter(_.nonEmpty)
        .map(d => s"""description: "${escapeYamlString(toSingleLine(d))}""""),
      Some(s"""category: "${escapeYamlString(info.operatorGroupName)}""""),
      Some(s"""operator_type: "${escapeYamlString(op.operatorType)}""""),
      Some(s"tags: [${dir.replace("/", ", ")}]")
    ).flatten

    val ancestors = groupAncestry(info.operatorGroupName)
    val breadcrumb = (s"[Home]($rootPath)" :: ancestors.zipWithIndex.map {
      case (name, i) => s"[$name](${"../" * (ancestors.size - i)})"
    }).mkString(" > ")

    val properties = JsonPropertyReflectionUtils
      .generatePropertiesTable(operatorClass, rootPath)
      .map(table => s"### Input Properties\n\n$table")
    val outputs = JsonPropertyReflectionUtils.generateOutputSection(operatorClass, rootPath)

    renderPage(frontMatter, breadcrumb :: properties.toList ::: List(outputs))
  }

  private def writeGroupIndexes(
      outputDir: Path,
      group: GroupInfo,
      ancestors: List[String],
      weight: Int,
      byGroup: Map[String, List[OperatorMetadata]]
  ): Unit = {
    val dir = groupPath(group.groupName)
    val depth = ancestors.size + 1
    val frontMatter = List(
      s"""title: "${escapeYamlString(group.groupName)}"""",
      s"""description: "Operators in the ${escapeYamlString(group.groupName)} category"""",
      s"weight: $weight",
      "categories: [Operators]",
      s"tags: [${dir.replace("/", ", ")}]"
    )
    val breadcrumb = (s"[Home](${"../" * depth})" :: ancestors.zipWithIndex.map {
      case (name, i) => s"[$name](${"../" * (ancestors.size - i)})"
    } ::: List(group.groupName)).mkString(" > ")

    val subcategories = Some(childGroups(group)).filter(_.nonEmpty).map { subs =>
      "## Subcategories\n\n" + subs
        .map(c => s"- [${c.groupName}](${toSlug(c.groupName)}/)")
        .mkString("\n")
    }
    val operators = byGroup.get(dir).map { ops =>
      val rows = ops.sortBy(_.additionalMetadata.userFriendlyName).map { op =>
        val description = Option(op.additionalMetadata.operatorDescription)
          .filter(_.nonEmpty)
          .map(toSingleLine)
          .getOrElse("-")
        s"| [${op.additionalMetadata.userFriendlyName}](${operatorSlug(op)}/) | $description |"
      }
      "## Operators\n\n| Operator | Description |\n|----------|-------------|\n" +
        rows.mkString("\n") + s"\n\n**Total**: ${operatorCount(ops.size)}"
    }

    writeFile(
      outputDir.resolve(dir).resolve("_index.md"),
      renderPage(frontMatter, breadcrumb :: subcategories.toList ::: operators.toList)
    )
    childGroups(group).zipWithIndex.foreach {
      case (child, i) =>
        writeGroupIndexes(outputDir, child, ancestors :+ group.groupName, i + 1, byGroup)
    }
  }

  private def rootIndex(
      groups: List[GroupInfo],
      byGroup: Map[String, List[OperatorMetadata]]
  ): String = {
    def total(group: GroupInfo): Int = {
      val own = byGroup.get(groupPath(group.groupName)).map(_.size).getOrElse(0)
      own + childGroups(group).map(total).sum
    }
    def section(group: GroupInfo, level: Int): List[String] = {
      val operators =
        Some(total(group)).filter(_ > 0).map(n => s" (${operatorCount(n)})").getOrElse("")
      s"${"  " * level}- [${group.groupName}](${groupPath(group.groupName)}/)$operators" ::
        childGroups(group).flatMap(section(_, level + 1))
    }
    val frontMatter = List(
      """title: "Operators"""",
      """description: "Complete reference for all Texera operators organized by category"""",
      "weight: 1",
      "categories: [Documentation]",
      "tags: [operators, reference, index]"
    )
    renderPage(
      frontMatter,
      List(
        "## Quick Links\n\n" +
          "- [Output Port Modes](output-modes/) - how each output port delivers its results\n" +
          "- [Parameter Reference](parameters/) - ML parameter specifications",
        "---",
        "## Operator Categories\n\n" + groups.flatMap(section(_, 0)).mkString("\n")
      )
    )
  }

  private def parameterPage(set: ParameterSet): String = {
    val name = set.enumClass.getSimpleName.stripSuffix("Parameters")
    val frontMatter = List(
      s"""title: "${escapeYamlString(name)} Parameters"""",
      s"""description: "Hyperparameters accepted by ${escapeYamlString(name)}"""",
      "categories: [Parameters]",
      "tags: [ml, sklearn, parameters]"
    )
    val usedBy = set.operators.sortBy(_.additionalMetadata.userFriendlyName).map { op =>
      val dir = groupPath(op.additionalMetadata.operatorGroupName)
      s"- [${op.additionalMetadata.userFriendlyName}](../../$dir/${operatorSlug(op)}/)"
    }
    val rows = JsonPropertyReflectionUtils
      .extractEnumMetadata(set.enumClass)
      .map(p => s"| ${p.name} | `${p.paramType}` | ${p.defaultValue} | ${p.constraint} |")
    renderPage(
      frontMatter,
      List(
        "[← Parameters Index](../)",
        "## Used By\n\nThis parameter set is used by the following operators:\n\n" + usedBy
          .mkString("\n"),
        "## Parameters\n\n| Parameter | Type | Default | Allowed values |\n" +
          "|-----------|------|---------|----------------|\n" + rows.mkString("\n")
      )
    )
  }

  private def parameterIndex(sets: List[ParameterSet]): String = {
    val frontMatter = List(
      """title: "Parameter Reference"""",
      """description: "Complete reference for machine learning operator parameters"""",
      "weight: 13",
      "categories: [Reference]",
      "tags: [parameters, ml, reference]"
    )
    val rows = sets.map { set =>
      val name = set.enumClass.getSimpleName.stripSuffix("Parameters")
      val operators = set.operators.map(_.additionalMetadata.userFriendlyName).sorted
      s"| [$name](${JsonPropertyReflectionUtils.parameterPageSlug(set.enumClass)}/) | " +
        s"${operators.size} | ${operators.mkString(", ")} |"
    }
    renderPage(
      frontMatter,
      List(
        "[← Home](../)",
        "## Available Parameter Sets\n\n| Parameter Set | Used By | Operators |\n" +
          "|---------------|---------|----------|\n" + rows.mkString("\n")
      )
    )
  }

  private def renderPage(frontMatter: List[String], sections: List[String]): String = {
    val header = LicenseHeader + "---\n" + frontMatter.mkString("\n") + "\n---\n\n"
    header + sections.mkString("\n\n") + "\n"
  }

  // Descriptions land in YAML front matter and table cells, so they must be one line.
  private def toSingleLine(s: String): String = {
    val joined = s.trim.replaceAll("\\s*\\n\\s*", " ")
    s"${joined.head.toUpper}${joined.tail}"
  }

  private def groupPath(groupName: String): String =
    groupPaths.getOrElse(
      groupName,
      throw new NoSuchElementException(
        s"operator group '$groupName' is not in OperatorGroupConstants.OperatorGroupOrderList"
      )
    )

  private def childGroups(group: GroupInfo): List[GroupInfo] = Option(group.children).getOrElse(Nil)

  private def groupAncestry(groupName: String): List[String] = {
    def find(groups: List[GroupInfo], path: List[String]): Option[List[String]] =
      groups.view.flatMap { g =>
        if (g.groupName == groupName) Some(path :+ g.groupName)
        else find(childGroups(g), path :+ g.groupName)
      }.headOption
    find(OperatorMetadataGenerator.allOperatorMetadata.groups, Nil).get
  }

  private def operatorCount(n: Int): String = if (n == 1) "1 operator" else s"$n operators"

  private def operatorSlug(op: OperatorMetadata): String =
    toSlug(op.additionalMetadata.userFriendlyName)

  private def toSlug(name: String): String =
    name.trim.replaceAll("\\s+", "-").replaceAll("[^a-zA-Z0-9-]", "").toLowerCase

  private def escapeYamlString(s: String): String = s.replace("\\", "\\\\").replace("\"", "\\\"")

  private def deleteGeneratedPages(outputDir: Path): Unit =
    if (Files.isDirectory(outputDir)) {
      Files
        .walk(outputDir)
        .iterator()
        .asScala
        .filter(p =>
          p.toString.endsWith(".md") && !HandWrittenPages.contains(p.getFileName.toString)
        )
        .toList
        .foreach(Files.delete)
    }

  private def writeFile(path: Path, content: String): Unit = {
    Files.createDirectories(path.getParent)
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
  }
}

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import sbt._
import sbt.Keys._
import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport._

/**
 * Generates per-module LICENSE files for jar META-INF and dist zip top level.
 *
 * Each jar's META-INF/LICENSE describes only what is in that specific jar:
 *  - Modules without vendored code get Apache 2.0 only.
 *  - workflow-operator gets Apache 2.0 plus the mbknor-jackson-jsonschema
 *    attribution and the full MIT license text.
 *
 * NOTICE and DISCLAIMER are copied as-is from the repo root.
 *
 * See https://github.com/apache/texera/issues/4131
 */
object AddMetaInfLicenseFiles {

  private lazy val rootDir = LocalRootProject / baseDirectory

  private val ThirdPartyHeader = "THIRD-PARTY DEPENDENCIES"

  /** Extract the Apache 2.0 license text (before the THIRD-PARTY section) from root LICENSE. */
  private def apacheLicenseText(rootDir: File): String = {
    val lines = IO.readLines(rootDir / "LICENSE")
    val headerIndex = lines.indexWhere(_.trim == ThirdPartyHeader)
    val cutoffIndex =
      if (headerIndex >= 0) {
        // Cut at the "---" delimiter line preceding the header
        val delimiterIndex = lines.lastIndexWhere(_.startsWith("---"), headerIndex - 1)
        if (delimiterIndex >= 0) delimiterIndex else headerIndex
      } else {
        lines.length
      }
    lines.take(cutoffIndex).mkString("\n").trim + "\n"
  }

  /** The vendored code section for workflow-operator (mbknor-jackson-jsonschema). */
  private def workflowOperatorVendoredSection(rootDir: File): String = {
    val mitLicense = IO.read(rootDir / "licenses" / "LICENSE-MIT.txt")
    s"""
       |--------------------------------------------------------------------------------
       |THIRD-PARTY DEPENDENCIES
       |--------------------------------------------------------------------------------
       |
       |This jar bundles compiled code from the following third-party project.
       |The full license text is included below.
       |
       |MIT License
       |--------------------------------------
       |
       |This product bundles code derived from mbknor-jackson-jsonschema:
       |  - com/kjetland/jackson/jsonSchema/
       |  Copyright (c) 2016 Kjell Tore Eliassen (mbknor)
       |  Source: https://github.com/mbknor/mbknor-jackson-jsonschema
       |
       |--------------------------------------------------------------------------------
       |Full text of the MIT License:
       |--------------------------------------------------------------------------------
       |
       |${mitLicense.trim}
       |""".stripMargin
  }

  private def writeToMetaInf(managed: File, fileName: String, content: String): File = {
    val dest = managed / "META-INF" / fileName
    IO.write(dest, content)
    dest
  }

  private def copyToMetaInf(managed: File, src: File, fileName: String): File = {
    val dest = managed / "META-INF" / fileName
    IO.copyFile(src, dest)
    dest
  }

  private def noticeAndDisclaimer(managed: File, rootDir: File): Seq[File] = {
    val files = Seq(copyToMetaInf(managed, rootDir / "NOTICE", "NOTICE"))
    val disclaimer = rootDir / "DISCLAIMER"
    if (disclaimer.exists()) files :+ copyToMetaInf(managed, disclaimer, "DISCLAIMER")
    else files
  }

  /** Settings for modules WITHOUT vendored third-party code.
   *  META-INF/LICENSE contains only the Apache 2.0 license text. */
  lazy val defaultSettings: Seq[Setting[_]] = Seq(
    Compile / resourceGenerators += Def.task {
      val managed = (Compile / resourceManaged).value
      val root = rootDir.value
      val licenseContent = apacheLicenseText(root)
      writeToMetaInf(managed, "LICENSE", licenseContent) +: noticeAndDisclaimer(managed, root)
    }.taskValue
  )

  /** Settings for workflow-operator which contains vendored mbknor-jackson-jsonschema code.
   *  META-INF/LICENSE contains Apache 2.0 plus the mbknor attribution and MIT license text. */
  lazy val workflowOperatorSettings: Seq[Setting[_]] = Seq(
    Compile / resourceGenerators += Def.task {
      val managed = (Compile / resourceManaged).value
      val root = rootDir.value
      val licenseContent = apacheLicenseText(root) + "\n" + workflowOperatorVendoredSection(root)
      writeToMetaInf(managed, "LICENSE", licenseContent) +: noticeAndDisclaimer(managed, root)
    }.taskValue
  )

  /** Ships LICENSE-binary, NOTICE-binary, DISCLAIMER (if present), and
   *  licenses/ at the Universal zip's top level. LICENSE-binary and
   *  NOTICE-binary are required; DISCLAIMER is optional (it will be
   *  removed at graduation). */
  def distMappings(existing: Seq[(File, String)], rootDir: File): Seq[(File, String)] = {
    val licenseBinary = rootDir / "LICENSE-binary"
    val noticeBinary = rootDir / "NOTICE-binary"
    val disclaimerFile = rootDir / "DISCLAIMER"
    val licensesDir = rootDir / "licenses"

    require(licenseBinary.isFile,
      s"LICENSE-binary not found at $licenseBinary; required for binary-distribution packaging.")
    require(noticeBinary.isFile,
      s"NOTICE-binary not found at $noticeBinary; required for binary-distribution packaging.")
    require(licensesDir.isDirectory,
      s"licenses/ directory not found at $licensesDir; required for binary-distribution packaging.")

    val reserved = Set("LICENSE", "NOTICE", "DISCLAIMER")
    val filtered = existing.filterNot {
      case (_, path) => reserved.contains(path) || path.startsWith("licenses/")
    }

    val licenseTexts = (licensesDir ** "*.txt").get.map(f => f -> s"licenses/${f.getName}")

    val base = Seq(licenseBinary -> "LICENSE", noticeBinary -> "NOTICE")
    val disclaimer =
      if (disclaimerFile.isFile) Seq(disclaimerFile -> "DISCLAIMER")
      else Seq.empty

    filtered ++ base ++ disclaimer ++ licenseTexts
  }
}

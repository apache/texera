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
 * Copies the repo-root LICENSE, NOTICE, and DISCLAIMER-WIP into jar META-INF
 * directories and sbt-native-packager dist zip top level.
 *
 * Modeled after Apache Pekko's project/AddMetaInfLicenseFiles.scala.
 * See https://github.com/apache/texera/issues/4131
 */
object AddMetaInfLicenseFiles {

  private lazy val rootDir = LocalRootProject / baseDirectory

  private def addFileToMetaInf(resourceManaged: File, file: File, targetName: String): File = {
    val metaInfDir = resourceManaged / "META-INF"
    val dest = metaInfDir / targetName
    IO.copyFile(file, dest)
    dest
  }

  lazy val defaultLicenseFile = Def.setting { rootDir.value / "LICENSE" }
  lazy val defaultNoticeFile = Def.setting { rootDir.value / "NOTICE" }
  lazy val defaultDisclaimerFile = Def.setting { rootDir.value / "DISCLAIMER-WIP" }

  /** Settings for all modules: copies LICENSE, NOTICE, and DISCLAIMER-WIP
   *  into the jar's META-INF directory. */
  lazy val defaultSettings: Seq[Setting[_]] = Seq(
    Compile / resourceGenerators += Def.task {
      val managed = (Compile / resourceManaged).value
      val licenseFile = defaultLicenseFile.value
      val noticeFile = defaultNoticeFile.value
      val disclaimerFile = defaultDisclaimerFile.value
      val files = Seq(
        addFileToMetaInf(managed, licenseFile, "LICENSE"),
        addFileToMetaInf(managed, noticeFile, "NOTICE")
      )
      if (disclaimerFile.exists()) files :+ addFileToMetaInf(managed, disclaimerFile, "DISCLAIMER-WIP") else files
    }.taskValue
  )

  /** Additional settings for dist-producing modules: places the same files
   *  at the top level of the sbt-native-packager Universal zip so they
   *  appear alongside lib/ and bin/ in the distribution. */
  lazy val distSettings: Seq[Setting[_]] = Seq(
    Universal / mappings := {
      val existing = (Universal / mappings).value
      val licenseFile = defaultLicenseFile.value
      val noticeFile = defaultNoticeFile.value
      val disclaimerFile = defaultDisclaimerFile.value
      val reserved = Set("LICENSE", "NOTICE", "DISCLAIMER", "DISCLAIMER-WIP")
      val filtered = existing.filterNot { case (_, path) => reserved.contains(path) }
      val extras = Seq(
        licenseFile -> "LICENSE",
        noticeFile -> "NOTICE"
      ) ++ (if (disclaimerFile.exists()) Seq(disclaimerFile -> "DISCLAIMER-WIP") else Seq.empty)
      filtered ++ extras
    }
  )
}

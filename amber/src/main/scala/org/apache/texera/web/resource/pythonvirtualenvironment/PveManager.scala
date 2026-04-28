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

package org.apache.texera.web.resource.pythonvirtualenvironment

import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.concurrent.BlockingQueue
import scala.collection.mutable.Map
import scala.jdk.CollectionConverters._
import scala.sys.process._
import java.util.Comparator
import org.apache.texera.amber.config.UdfConfig

/**
  * PveManager is responsible for managing Python Virtual Environments (PVEs)
  * for each Computing Unit
  *
  * It supports:
  * - Creating and initializing isolated Python environments
  * - Installing and uninstalling Python packages
  * - Tracking system vs user-installed packages via metadata files
  * - Streaming pip output logs back to the caller
  *
  * Each PVE is stored under:
  *   /tmp/texera-pve/venvs/{cuid}/{pveName}/
  *
  * The structure includes:
  * - pve/              -> actual virtual environment
  * - metadata/         -> package tracking (system + user)
  */

object PveManager {

  private val VenvRoot: Path = Paths.get("/tmp/texera-pve/venvs")

  private def cuidDir(cuid: Int, pveName: String): Path = {
    VenvRoot.resolve(cuid.toString).resolve(pveName)
  }

  private def pveDir(cuid: Int, pveName: String): Path =
    cuidDir(cuid, pveName).resolve("pve")

  private def pythonBinPath(cuid: Int, pveName: String): Path =
    pveDir(cuid, pveName).resolve("bin").resolve("python")

  private def metadataDir(cuid: Int, pveName: String): Path =
    pveDir(cuid, pveName).resolve("metadata")

  private def systemPackagesPath(cuid: Int, pveName: String): Path =
    metadataDir(cuid, pveName).resolve("system-packages.txt")

  private def writeMetadata(path: Path, lines: Seq[String]): Unit = {
    if (path.getParent != null) {
      Files.createDirectories(path.getParent)
    }
    Files.write(
      path,
      lines.asJava,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.WRITE
    )
  }

  private def readMetadataList(path: Path): List[String] = {
    if (!Files.exists(path)) return Nil
    Files.readAllLines(path).asScala.map(_.trim).filter(_.nonEmpty).toList
  }

  private def pipEnv: Map[String, String] =
    Map(
      "PYTHONUNBUFFERED" -> "1",
      "PIP_PROGRESS_BAR" -> "off",
      "PIP_DISABLE_PIP_VERSION_CHECK" -> "1",
      "PIP_NO_INPUT" -> "1"
    )

  def getSystemPackages(cuid: Int, pveName: String): (Seq[String]) = {
    val sys = readMetadataList(systemPackagesPath(cuid, pveName))
    (sys)
  }

  /**
    * Creates a new PVE for a CU.
    *
    * Behavior:
    * Creates a fresh venv and installs dependencies
    *
    * Steps:
    * 2. Install system dependencies
    * 3. Record installed packages as system metadata
    *
    * Logs progress to the provided queue.
    */
  def createNewPve(
      cuid: Int,
      queue: BlockingQueue[String],
      pveName: String,
      isLocal: Boolean
  ): Unit = {
    queue.put(s"[PVE] Creating new PVE for cuid: $cuid with name: $pveName")

    val venvDirPath = pveDir(cuid, pveName).toAbsolutePath
    val python = pythonBinPath(cuid, pveName).toAbsolutePath.toString
    val envVars = pipEnv

    val pythonPath = UdfConfig.pythonPath.trim
    val createVenvPython = if (pythonPath.isEmpty) "python3" else pythonPath

    // NOTE: This following paths are derived from the computing-unit-master.dockerfile. If
    // the location of requirements.txt or operator-requirements.txt changes, this path
    // must be updated accordingly.

    val requirementsPath =
      if (isLocal) Paths.get("amber", "requirements.txt")
      else Paths.get("/tmp", "requirements.txt")

    val operatorRequirementsPath =
      if (isLocal) Paths.get("amber", "operator-requirements.txt")
      else Paths.get("/tmp", "operator-requirements.txt")

    Files.createDirectories(venvDirPath.getParent)

    val createCode = Process(Seq(createVenvPython, "-m", "venv", venvDirPath.toString)).!(
      ProcessLogger(
        out => queue.put(s"[pve] $out"),
        err => queue.put(s"[pve][ERR] $err")
      )
    )

    queue.put(s"[pve] venv creation finished with exit code $createCode")

    if (createCode != 0) {
      queue.put(s"[PVE][ERR] Failed to create venv (exit=$createCode)")
      return
    }

    if (!Files.exists(requirementsPath)) {
      queue.put(s"[PVE][ERR] requirements.txt not found at ${requirementsPath.toAbsolutePath}")
      return
    }

    if (!Files.exists(operatorRequirementsPath)) {
      queue.put(
        s"[PVE][ERR] operator-requirements.txt not found at ${operatorRequirementsPath.toAbsolutePath}"
      )
      return
    }

    Files.createDirectories(metadataDir(cuid, pveName))

    queue.put(s"[PVE] Installing requirements from ${requirementsPath.toAbsolutePath}")

    val installReqCode = Process(
      Seq(
        python,
        "-u",
        "-m",
        "pip",
        "install",
        "--progress-bar",
        "off",
        "-r",
        requirementsPath.toString
      ),
      None,
      envVars.toSeq: _*
    ).!(
      ProcessLogger(
        out => queue.put(s"[pip] $out"),
        err => queue.put(s"[pip][ERR] $err")
      )
    )

    queue.put(s"[PVE] requirements install finished with exit code $installReqCode")

    if (installReqCode != 0) {
      queue.put(s"[PVE][ERR] Failed to install requirements.txt (exit=$installReqCode)")
      return
    }

    queue.put(
      s"[PVE] Installing operator requirements from ${operatorRequirementsPath.toAbsolutePath}"
    )

    val installOperatorReqCode = Process(
      Seq(
        python,
        "-u",
        "-m",
        "pip",
        "install",
        "--progress-bar",
        "off",
        "-r",
        operatorRequirementsPath.toString
      ),
      None,
      envVars.toSeq: _*
    ).!(
      ProcessLogger(
        out => queue.put(s"[pip] $out"),
        err => queue.put(s"[pip][ERR] $err")
      )
    )

    queue.put(
      s"[PVE] operator requirements install finished with exit code $installOperatorReqCode"
    )

    if (installOperatorReqCode != 0) {
      queue.put(
        s"[PVE][ERR] Failed to install operator-requirements.txt (exit=$installOperatorReqCode)"
      )
      return
    }

    queue.put("[PVE] Running pip freeze")

    val freezeOutput = Process(Seq(python, "-m", "pip", "freeze"), None, envVars.toSeq: _*).!!
    val installedLines = freezeOutput.split("\n").map(_.trim).filter(_.nonEmpty).toSeq

    writeMetadata(systemPackagesPath(cuid, pveName), installedLines)

    queue.put(s"[PVE] Created new environment for cuid = $cuid")
  }

  def getEnvironments(cuid: Int): List[String] = {

    val cuPath = VenvRoot.resolve(cuid.toString)

    if (!Files.isDirectory(cuPath)) {
      return List()
    }

    val stream = Files.list(cuPath)

    try {
      stream
        .iterator()
        .asScala
        .filter(path => Files.isDirectory(path))
        .map(path => path.getFileName.toString)
        .toList
    } finally {
      stream.close()
    }
  }

  // Deletes all PVE environments for a given CU (when running locally)
  def deleteEnvironments(cuid: Int): Unit = {
    val cuPath = VenvRoot.resolve(cuid.toString)

    if (!Files.isDirectory(cuPath)) {
      return
    }

    val stream = Files.walk(cuPath)

    try {
      stream
        .sorted(Comparator.reverseOrder())
        .iterator()
        .asScala
        .foreach(path => Files.deleteIfExists(path))
    } finally {
      stream.close()
    }
  }
}

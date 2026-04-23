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

import java.io.{File, RandomAccessFile}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.concurrent.BlockingQueue
import scala.collection.mutable.Map
import scala.jdk.CollectionConverters._
import scala.sys.process._

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

  private def cuidDir(cuid: Int, pvename: String): Path = {
    val dir = VenvRoot.resolve(cuid.toString).resolve(pvename)
    Files.createDirectories(dir)
    dir
  }

  private def pveDir(cuid: Int, pveName: String): Path =
    cuidDir(cuid, pveName).resolve("pve")

  private def pythonBinPath(cuid: Int, pveName: String): Path =
    pveDir(cuid, pveName).resolve("bin").resolve("python")

  private def pipBinPath(cuid: Int, pveName: String): Path =
    pveDir(cuid, pveName).resolve("bin").resolve("pip")

  private def metadataDir(cuid: Int, pveName: String): Path =
    pveDir(cuid, pveName).resolve("metadata")

  private def systemPackagesPath(cuid: Int, pveName: String): Path =
    metadataDir(cuid, pveName).resolve("system-packages.txt")

  private def userPackagesPath(cuid: Int, pveName: String): Path =
    metadataDir(cuid, pveName).resolve("user-packages.txt")

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

  def getSystemAndUserPackages(cuid: Int, pveName: String): (Seq[String], Seq[String]) = {
    val sys = readMetadataList(systemPackagesPath(cuid, pveName))
    val usr = readMetadataList(userPackagesPath(cuid, pveName))
    (sys, usr)
  }

  /**
    * Creates a new PVE for a CU.
    *
    * Behavior:
    * - If a base PVE exists (PVE_BASE), it clones it for faster setup
    * - Otherwise, creates a fresh venv and installs dependencies
    *
    * Steps:
    * 1. Remove existing environment (if present)
    * 2. Create or copy base environment
    * 3. Install system + operator dependencies
    * 4. Record installed packages as system metadata
    *
    * Logs progress to the provided queue.
    */
  def createNewPve(cuid: Int, queue: BlockingQueue[String], pveName: String): Unit = {
    queue.put(s"[PVE] Creating new PVE for cuid=$cuid with name=$pveName")

    val venvDirPath = pveDir(cuid, pveName).toAbsolutePath
    val python = pythonBinPath(cuid, pveName).toAbsolutePath.toString
    val envVars = pipEnv

    val pveBase = sys.env.getOrElse("PVE_BASE", "/opt/pve-base")
    val basePython = Paths.get(pveBase).resolve("bin").resolve("python")
    val hasBasePve = Files.exists(basePython)

    val requirementsPath =
      if (!hasBasePve) Paths.get("amber", "requirements.txt")
      else Paths.get("/tmp", "requirements.txt")

    val operatorRequirementsPath =
      if (!hasBasePve) Paths.get("amber", "operator-requirements.txt")
      else Paths.get("/tmp", "operator-requirements.txt")

    if (!hasBasePve) {
      if (Files.exists(venvDirPath)) {
        val rmCode = Process(Seq("bash", "-lc", s"rm -rf '${venvDirPath.toString}'")).!(
          ProcessLogger(
            out => queue.put(s"[pve] $out"),
            err => queue.put(s"[pve][ERR] $err")
          )
        )
        queue.put(s"[pve] removed existing venv with exit code $rmCode")
      }

      Files.createDirectories(venvDirPath.getParent)
      queue.put(s"[PVE] Creating fresh local venv at ${venvDirPath.toString}")

      val createCode = Process(Seq("python3", "-m", "venv", venvDirPath.toString)).!(
        ProcessLogger(
          out => queue.put(s"[pve] $out"),
          err => queue.put(s"[pve][ERR] $err")
        )
      )
      queue.put(s"[pve] local venv creation finished with exit code $createCode")

      if (createCode != 0) {
        queue.put(s"[PVE][ERR] Failed to create local venv (exit=$createCode)")
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

      queue.put(s"[PVE] Installing local base requirements from ${requirementsPath.toAbsolutePath}")

      val installReqCode = Process(
        Seq(python, "-m", "pip", "install", "-r", requirementsPath.toString),
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
        s"[PVE] Installing local operator requirements from ${operatorRequirementsPath.toAbsolutePath}"
      )

      val installOperatorReqCode = Process(
        Seq(python, "-m", "pip", "install", "-r", operatorRequirementsPath.toString),
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
      writeMetadata(userPackagesPath(cuid, pveName), Seq.empty)

      queue.put(s"[PVE] Created new local environment for cuid=$cuid")
      return
    }

    if (Files.exists(venvDirPath)) {
      val rmCode = Process(Seq("bash", "-lc", s"rm -rf '${venvDirPath.toString}'")).!(
        ProcessLogger(
          out => queue.put(s"[pve] $out"),
          err => queue.put(s"[pve][ERR] $err")
        )
      )
      queue.put(s"[pve] removed existing venv with exit code $rmCode")
    }

    Files.createDirectories(venvDirPath.getParent)
    queue.put(s"[PVE] Copying base venv from $pveBase to ${venvDirPath.toString}")

    val copyCode = Process(Seq("bash", "-lc", s"cp -a '${pveBase}' '${venvDirPath.toString}'")).!(
      ProcessLogger(
        out => queue.put(s"[pve] $out"),
        err => queue.put(s"[pve][ERR] $err")
      )
    )
    queue.put(s"[pve] base copy finished with exit code $copyCode")

    if (copyCode != 0) {
      queue.put(s"[PVE][ERR] Failed to copy base venv (exit=$copyCode)")
      return
    }

    val fixCode = Process(
      Seq(
        "bash",
        "-lc",
        s"""
           |set -e
           |PY='${python}'
           |BIN='${venvDirPath.toString}/bin'
           |for f in "$$BIN"/*; do
           |  [ -f "$$f" ] || continue
           |  head -n 1 "$$f" | grep -q '^#!' || continue
           |  head -n 1 "$$f" | grep -qi 'python' || continue
           |  sed -i.bak "1s|^#!.*python.*|#!$$PY|" "$$f" || true
           |  rm -f "$$f.bak" || true
           |done
           |""".stripMargin
      )
    ).!(
      ProcessLogger(
        out => queue.put(s"[pve] $out"),
        err => queue.put(s"[pve][ERR] $err")
      )
    )
    queue.put(s"[pve] rewrite finished with exit code $fixCode")

    Files.createDirectories(metadataDir(cuid, pveName))

    queue.put("[PVE] Base environment copied; skipping system requirements install.")

    queue.put("[PVE] Running pip freeze")
    val freezeOutput = Process(Seq(python, "-m", "pip", "freeze"), None, envVars.toSeq: _*).!!
    val systemFreezeLines = freezeOutput.split("\n").map(_.trim).filter(_.nonEmpty).toSeq

    writeMetadata(systemPackagesPath(cuid, pveName), systemFreezeLines)
    writeMetadata(userPackagesPath(cuid, pveName), Seq.empty)

    queue.put(s"[PVE] Created new environment for cuid=$cuid")
  }

  def pveExists(cuid: Int, pveName: String): Boolean =
    Files.exists(pythonBinPath(cuid, pveName)) && Files.exists(pipBinPath(cuid, pveName))

  def getEnvironments(cuid: Int): List[String] = {

    val cuPath = Paths.get("/tmp/texera-pve/venvs").resolve(cuid.toString)

    if (!Files.exists(cuPath) || !Files.isDirectory(cuPath)) {
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

  def getAllPveUserPackages(cuid: Int): Seq[(String, Seq[String])] = {
    val pveNames = getEnvironments(cuid)

    pveNames.map { pveName =>
      val userPkgs = readMetadataList(userPackagesPath(cuid, pveName))
      (pveName, Option(userPkgs).getOrElse(Seq.empty[String]))
    }
  }
}

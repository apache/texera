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

import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.BlockingQueue
import scala.collection.mutable.Map
import scala.jdk.CollectionConverters._
import scala.sys.process._
import java.util.Comparator
import org.apache.texera.amber.config.PythonUtils

/**
  * PveManager is responsible for managing Python Virtual Environments (PVEs)
  * for each Computing Unit
  *
  * It supports:
  * - Creating and initializing isolated Python environments (with system packages)
  * - Installing user defined packages
  * - Streaming pip output logs back to the caller
  *
  * Each PVE is stored under:
  *   /tmp/texera-pve/venvs/{cuid}/{pveName}/
  */

object PveManager {

  case class PvePackageResponse(
      pveName: String,
      userPackages: Seq[String]
  )

  private val VenvRoot: Path = Paths.get("/tmp/texera-pve/venvs")

  private def cuidDir(cuid: Int, pveName: String): Path = {
    VenvRoot.resolve(cuid.toString).resolve(pveName)
  }

  private def pveDir(cuid: Int, pveName: String): Path =
    cuidDir(cuid, pveName).resolve("pve")

  private def pythonBinPath(cuid: Int, pveName: String): Path =
    pveDir(cuid, pveName).resolve("bin").resolve("python")

  private def pipEnv: Map[String, String] =
    Map(
      "PYTHONUNBUFFERED" -> "1",
      "PIP_PROGRESS_BAR" -> "off",
      "PIP_DISABLE_PIP_VERSION_CHECK" -> "1",
      "PIP_NO_INPUT" -> "1"
    )

  private def readPackageFile(path: Path): Seq[String] = {
    if (Files.exists(path)) {
      Files
        .readAllLines(path)
        .asScala
        .map(_.trim)
        .filter(_.nonEmpty)
        .toSeq
    } else {
      Seq()
    }
  }

  private def getSystemPath(isLocal: Boolean): Path = {
    Paths.get(
      if (isLocal) "amber/system-requirements-lock.txt"
      else "/tmp/system-requirements-lock.txt"
    )
  }

  def getSystemPackages(isLocal: Boolean): Seq[String] = {
    if (!Files.exists(getSystemPath(isLocal))) {
      Seq()
    } else {
      Files
        .readAllLines(getSystemPath(isLocal))
        .asScala
        .map(_.trim)
        .filter(line => line.nonEmpty && !line.startsWith("#"))
        .toSeq
    }
  }

  private def runPipInstall(
      python: String,
      args: Seq[String],
      queue: BlockingQueue[String]
  ): Int = {
    Process(
      Seq(
        python,
        "-u",
        "-m",
        "pip",
        "install",
        "--progress-bar",
        "off",
        "--no-input"
      ) ++ args,
      None,
      pipEnv.toSeq: _*
    ).!(
      ProcessLogger(
        out => queue.put(s"[pip] $out"),
        err => queue.put(s"[pip][ERR] $err")
      )
    )
  }

  /**
    * Creates a new PVE for a CU.
    *
    * Behavior:
    * Creates a fresh venv and installs dependencies
    *
    * Steps:
    * 1. Install system dependencies
    * 2. Logs progress to the provided queue.
    */
  def createNewPve(
      cuid: Int,
      queue: BlockingQueue[String],
      pveName: String,
      isLocal: Boolean
  ): Unit = {
    queue.put(s"[PVE] Creating new PVE for cuid: $cuid with name: $pveName")

    // NOTE: These paths are derived from computing-unit-master.dockerfile.
    // If requirements.txt location changes, update these paths.
    val requirementsPath =
      if (isLocal) Paths.get("amber", "requirements.txt")
      else Paths.get("/tmp", "requirements.txt")

    if (!Files.exists(requirementsPath)) {
      queue.put(s"[PVE][ERR] System requirements not found")
      return
    }

    val venvDirPath = pveDir(cuid, pveName).toAbsolutePath
    val python = pythonBinPath(cuid, pveName).toAbsolutePath.toString

    val createVenvPython = PythonUtils.getPythonExecutable

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

    queue.put(
      s"[PVE] Installing requirements from ${requirementsPath.toAbsolutePath}"
    )

    val installReqCode = runPipInstall(
      python,
      Seq(
        "-r",
        requirementsPath.toString
      ),
      queue
    )

    queue.put(s"[PVE] requirements install finished with exit code $installReqCode")

    if (installReqCode != 0) {
      queue.put(s"[PVE][ERR] Failed to install requirements files (exit=$installReqCode)")
      return
    }

    queue.put(s"[PVE] Created new environment for cuid = $cuid")
  }

  // returns list of PVE names and corresponding user packages for a given CU
  def getEnvironments(cuid: Int): List[PvePackageResponse] = {

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
        .map { path =>
          val pveName = path.getFileName.toString
          val metadataPath = path.resolve("user-packages.txt")

          val userPackages = readPackageFile(metadataPath)

          PvePackageResponse(
            pveName = pveName,
            userPackages = userPackages
          )
        }
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

  /**
    * Installs user requested Python packages into the PVE.
    *
    * 1. Executes pip install for each package
    * 2. Prevents conflicts with system dependencies.
    * 3. Updates user metadata file
    * 4. Streams logs back via queue
    */
  def installUserPackages(
      packages: List[String],
      cuid: Int,
      queue: BlockingQueue[String],
      pveName: String,
      isLocal: Boolean
  ): Unit = {

    val python = pythonBinPath(cuid, pveName).toAbsolutePath.toString

    if (!Files.exists(Paths.get(python))) {
      queue.put(s"[PVE][ERR] Python executable not found for PVE: $python")
      return
    }

    val metadataPath = cuidDir(cuid, pveName).resolve("user-packages.txt")

    var installedPackages = readPackageFile(metadataPath).toSet

    val systemPackages =
      if (Files.exists(getSystemPath(isLocal))) {
        Files
          .readAllLines(getSystemPath(isLocal))
          .asScala
          .map(_.trim)
          .filter(line => line.nonEmpty && !line.startsWith("#"))
          .map(line => line.split("==")(0).trim.toLowerCase)
          .toSet
      } else {
        Set[String]()
      }

    packages.foreach { pkg =>
      val trimmedPkg = pkg.trim

      if (trimmedPkg.nonEmpty) {

        val userPackageName = trimmedPkg.split("==")(0).trim.toLowerCase

        if (systemPackages.contains(userPackageName)) {
          queue.put(
            s"[PVE][ERR] $trimmedPkg is a system package and cannot be installed or modified by the user."
          )
          return
        }

        queue.put(s"[PVE] Installing package: $trimmedPkg")

        val code = runPipInstall(
          python,
          Seq(
            "--constraint", // check against system-requirements-lock
            getSystemPath(isLocal).toString,
            trimmedPkg
          ),
          queue
        )

        queue.put(s"[pip] install($trimmedPkg) finished with exit code $code")

        if (code != 0) {
          queue.put(s"[PVE][ERR] Failed to install package: $trimmedPkg")
          return
        }

        installedPackages = installedPackages + trimmedPkg

        Files.write(
          metadataPath,
          installedPackages.toSeq.sorted.asJava
        )
      }
    }

    queue.put("[PVE] Final user package list:")

    installedPackages.toSeq.sorted.foreach { pkg =>
      queue.put(s"[user-package] $pkg")
    }
  }
}

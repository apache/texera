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
import org.apache.texera.common.config.PythonUtils
import org.apache.texera.dao.SqlServer
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.lang3.SystemUtils
import org.apache.texera.dao.jooq.generated.tables.daos.VirtualEnvironmentsDao
import org.apache.texera.dao.jooq.generated.tables.pojos.VirtualEnvironments
import org.jooq.JSONB

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

object PveManager extends LazyLogging {

  case class PvePackageResponse(
      pveName: String,
      userPackages: Seq[String]
  )

  case class StoredPve(veid: Int, name: String, packagesJson: String)

  private val VenvRoot: Path = Paths.get("/tmp/texera-pve/venvs")

  private val SafePveName = "^[A-Za-z0-9._-]+$".r

  def isValidPveName(name: String): Boolean =
    name != null && name.length <= 128 && SafePveName.pattern.matcher(name).matches()

  private def cuidDir(cuid: Int, pveName: String): Path = {
    VenvRoot.resolve(cuid.toString).resolve(pveName)
  }

  private def pveDir(cuid: Int, pveName: String): Path =
    cuidDir(cuid, pveName).resolve("pve")

  // Resolves the Python interpreter inside a venv. POSIX puts it at
  // `<venv>/bin/python`; Windows puts it at `<venv>/Scripts/python.exe`.
  private def venvPython(venvDir: Path): Path =
    if (SystemUtils.IS_OS_WINDOWS)
      venvDir.resolve("Scripts").resolve("python.exe")
    else
      venvDir.resolve("bin").resolve("python")

  private def pythonBinPath(cuid: Int, pveName: String): Path =
    venvPython(pveDir(cuid, pveName))

  /*
   * Validates the PVE name and returns the Python binary path if it exists,
   * is executable, and resolves within the allowed virtual environment root.
   */
  def getPythonBin(cuid: Int, pveName: String): Option[Path] = {
    if (!SafePveName.pattern.matcher(pveName).matches()) return None
    val resolved = pythonBinPath(cuid, pveName).toAbsolutePath.normalize()
    val root = VenvRoot.toAbsolutePath.normalize()
    if (!resolved.startsWith(root)) return None
    if (Files.exists(resolved) && Files.isExecutable(resolved)) Some(resolved) else None
  }

  private def pipEnv: Map[String, String] =
    Map(
      "PYTHONUNBUFFERED" -> "1",
      "PIP_PROGRESS_BAR" -> "off",
      "PIP_DISABLE_PIP_VERSION_CHECK" -> "1",
      "PIP_NO_INPUT" -> "1"
    )

  // Test seam: every child process (venv creation, pip install/uninstall/freeze)
  // funnels through this so unit tests can run hermetically — no real venv, no
  // pip, no network. Production wiring runs the command for real; PveResourceSpec
  // swaps in a fake that fabricates the venv layout and emits canned output.
  private[pythonvirtualenvironment] type ProcessRunner =
    (Seq[String], Seq[(String, String)], ProcessLogger) => Int

  private[pythonvirtualenvironment] var runProcess: ProcessRunner =
    (command, env, logger) => Process(command, None, env: _*).!(logger)

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

  private def locateRequirementsTxt(): Option[Path] =
    Seq(Paths.get("/tmp", "requirements.txt"), Paths.get("amber", "requirements.txt"))
      .find(Files.exists(_))

  // Resolves the fully-pinned system package set by installing requirements.txt
  // into a throwaway venv and running `pip freeze`.
  private def resolveSystemPackages(): Seq[String] = {
    val requirementsPath = locateRequirementsTxt() match {
      case Some(p) => p
      case None =>
        logger.error("requirements.txt not found; system package set will be empty")
        return Seq.empty
    }

    val tempVenv = Files.createTempDirectory("texera-system-venv-")
    try {
      val python = venvPython(tempVenv).toString
      val createCode =
        runProcess(
          Seq(PythonUtils.getPythonExecutable, "-m", "venv", tempVenv.toString),
          Nil,
          ProcessLogger(_ => (), _ => ())
        )
      if (createCode != 0) {
        logger.error(s"failed to create temp venv for system-package resolution (exit=$createCode)")
        return Seq.empty
      }

      val installCode = runProcess(
        Seq(
          python,
          "-u",
          "-m",
          "pip",
          "install",
          "--progress-bar",
          "off",
          "--no-input",
          "-r",
          requirementsPath.toString
        ),
        pipEnv.toSeq,
        ProcessLogger(_ => (), _ => ())
      )
      if (installCode != 0) {
        logger.error(s"failed to install requirements into temp venv (exit=$installCode)")
        return Seq.empty
      }

      val collected = scala.collection.mutable.ListBuffer[String]()
      val freezeCode = runProcess(
        Seq(python, "-m", "pip", "freeze"),
        Nil,
        ProcessLogger(line => collected += line, _ => ())
      )
      if (freezeCode != 0) {
        logger.error(s"pip freeze failed (exit=$freezeCode)")
        return Seq.empty
      }
      collected.toSeq.map(_.trim).filter(line => line.nonEmpty && !line.startsWith("#"))
    } finally {
      try {
        val stream = Files.walk(tempVenv)
        try stream
          .sorted(Comparator.reverseOrder())
          .iterator()
          .asScala
          .foreach(Files.deleteIfExists)
        finally stream.close()
      } catch {
        case _: Throwable => ()
      }
    }
  }

  // Cached for the JVM lifetime. The system Python + requirements.txt don't
  // change without an app restart, so resolving once is sufficient.
  private lazy val systemPackages: Seq[String] = resolveSystemPackages()

  // Normalised package names ("numpy", "pandas") — used to reject user
  // attempts to install or delete system packages.
  private lazy val systemPackageNames: Set[String] =
    systemPackages.map(_.split("==")(0).trim.toLowerCase).toSet

  // Materialised once: a file containing the frozen system requirements,
  // passed as `pip install --constraint` so user installs respect system pins.
  private lazy val systemConstraintFile: Path = {
    val f = Files.createTempFile("texera-system-constraint-", ".txt")
    Files.write(f, systemPackages.asJava)
    f.toFile.deleteOnExit()
    f
  }

  def getSystemPackages: Seq[String] = systemPackages

  private def runPipInstall(
      python: String,
      args: Seq[String],
      queue: BlockingQueue[String]
  ): Int = {
    runProcess(
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
      pipEnv.toSeq,
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
      pveName: String
  ): Unit = {
    queue.put(s"[PVE] Creating new PVE for cuid: $cuid with name: $pveName")

    val requirementsPath = locateRequirementsTxt() match {
      case Some(p) => p
      case None =>
        queue.put(s"[PVE][ERR] System requirements not found")
        return
    }

    val venvDirPath = pveDir(cuid, pveName).toAbsolutePath
    val python = pythonBinPath(cuid, pveName).toAbsolutePath.toString

    val createVenvPython = PythonUtils.getPythonExecutable

    Files.createDirectories(venvDirPath.getParent)

    val createCode = runProcess(
      Seq(createVenvPython, "-m", "venv", venvDirPath.toString),
      Nil,
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

  // Returns every PVE row belonging to the given user.
  def listPvesForUser(uid: Int): List[StoredPve] = {
    import org.apache.texera.dao.jooq.generated.Tables.VIRTUAL_ENVIRONMENTS
    SqlServer
      .getInstance()
      .createDSLContext()
      .selectFrom(VIRTUAL_ENVIRONMENTS)
      .where(VIRTUAL_ENVIRONMENTS.UID.eq(uid))
      .fetchInto(classOf[VirtualEnvironments])
      .asScala
      .map { row =>
        val pkgsJson = Option(row.getPackages).map(_.data).getOrElse("{}")
        StoredPve(row.getVeid, row.getName, pkgsJson)
      }
      .toList
  }

  // Deletes a PVE row owned by `uid`. Returns true if a row was deleted, false if no
  // matching row was found (either the veid doesn't exist or it belongs to another user).
  def deletePveFromDb(veid: Int, uid: Int): Boolean = {
    import org.apache.texera.dao.jooq.generated.Tables.VIRTUAL_ENVIRONMENTS
    val rows = SqlServer
      .getInstance()
      .createDSLContext()
      .deleteFrom(VIRTUAL_ENVIRONMENTS)
      .where(
        VIRTUAL_ENVIRONMENTS.VEID
          .eq(veid)
          .and(VIRTUAL_ENVIRONMENTS.UID.eq(uid))
      )
      .execute()
    rows > 0
  }

  // Updates an existing PVE row owned by `uid`. Returns true if a row was
  // updated, false if no matching row was found.
  def updatePve(veid: Int, uid: Int, name: String, packagesJson: String): Boolean = {
    import org.apache.texera.dao.jooq.generated.Tables.VIRTUAL_ENVIRONMENTS
    val rows = SqlServer
      .getInstance()
      .createDSLContext()
      .update(VIRTUAL_ENVIRONMENTS)
      .set(VIRTUAL_ENVIRONMENTS.NAME, name)
      .set(VIRTUAL_ENVIRONMENTS.PACKAGES, JSONB.valueOf(packagesJson))
      .where(
        VIRTUAL_ENVIRONMENTS.VEID
          .eq(veid)
          .and(VIRTUAL_ENVIRONMENTS.UID.eq(uid))
      )
      .execute()
    rows > 0
  }

  // Persists a PVE spec (name + packages JSON) for the given user. Returns the new veid.
  def savePve(uid: Int, name: String, packagesJson: String): Int = {
    val row = new VirtualEnvironments()
    row.setUid(uid)
    row.setName(name)
    row.setPackages(JSONB.valueOf(packagesJson))
    val dao = new VirtualEnvironmentsDao(
      SqlServer.getInstance().createDSLContext().configuration
    )
    dao.insert(row)
    row.getVeid
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
      pveName: String
  ): Unit = {

    val python = pythonBinPath(cuid, pveName).toAbsolutePath.toString

    if (!Files.exists(Paths.get(python))) {
      queue.put(s"[PVE][ERR] Python executable not found for PVE: $python")
      return
    }

    val metadataPath = cuidDir(cuid, pveName).resolve("user-packages.txt")

    var installedPackages = readPackageFile(metadataPath).toSet

    packages.foreach { pkg =>
      val trimmedPkg = pkg.trim

      if (trimmedPkg.nonEmpty) {

        val userPackageName = trimmedPkg.split("==")(0).trim.toLowerCase

        if (systemPackageNames.contains(userPackageName)) {
          queue.put(
            s"[PVE][ERR] $trimmedPkg is a system package and cannot be installed or modified by the user."
          )
          return
        }

        queue.put(s"[PVE] Installing package: $trimmedPkg")

        val code = runPipInstall(
          python,
          Seq(
            "--constraint", // pin to the runtime-resolved system set
            systemConstraintFile.toString,
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

  /**
    * Uninstalls a user-installed package from the PVE.
    * 1. Prevents deletion of system packages
    * 2. Updates user metadata upon success
    * 3. Returns status messages
    */
  def deletePackages(
      cuid: Int,
      packageName: String,
      pveName: String
  ): List[String] = {
    val python = pythonBinPath(cuid, pveName).toAbsolutePath.toString
    val metadataPath = cuidDir(cuid, pveName).resolve("user-packages.txt")

    if (!Files.exists(Paths.get(python))) {
      val msg = s"[PVE][ERR] Python executable not found for PVE: $python"
      logger.error(msg)
      return List(msg)
    }

    val trimmedPackageName = packageName.trim
    val normalizedPackageName = trimmedPackageName.split("==")(0).trim.toLowerCase

    if (systemPackageNames.contains(normalizedPackageName)) {
      return List(
        s"[PVE][ERR] $trimmedPackageName is a system package and cannot be deleted."
      )
    }

    try {
      val output = scala.collection.mutable.ListBuffer[String]()

      val exitCode = runProcess(
        Seq(
          python,
          "-u",
          "-m",
          "pip",
          "uninstall",
          "-y",
          trimmedPackageName
        ),
        pipEnv.toSeq,
        ProcessLogger(
          out => {
            logger.info(s"[pip] $out")
            output += s"[pip] $out"
          },
          err => {
            logger.error(s"[pip][ERR] $err")
            output += s"[pip][ERR] $err"
          }
        )
      )

      if (exitCode == 0) {
        val updatedPackages = readPackageFile(metadataPath)
          .filterNot(line => line.split("==")(0).trim.toLowerCase == normalizedPackageName)
          .sorted

        Files.write(metadataPath, updatedPackages.asJava)

        output += s"[pip] uninstall($trimmedPackageName) finished with exit code $exitCode"
        output += s"[PVE] Uninstalled $trimmedPackageName successfully"
      } else {
        output += s"[PVE][ERR] Failed to uninstall package: $trimmedPackageName"
      }

      output.toList
    } catch {
      case e: Exception =>
        List(s"[PVE][ERR] Failed to delete package for cuid=$cuid: ${e.getMessage}")
    }
  }

}

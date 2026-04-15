package org.apache.texera.web.resource.pythonvirtualenvironment

import java.io.{File, RandomAccessFile}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.concurrent.BlockingQueue
import scala.collection.mutable.Map
import scala.jdk.CollectionConverters._
import scala.sys.process._

object PveManager {

  private val VenvRoot: Path = Paths.get("/tmp/texera-pve/venvs")

  private def ensureDirExists(path: Path): Unit = {
    if (!Files.exists(path)) Files.createDirectories(path)
  }

  private def cuidDir(cuid: Int, pvename: String): Path = {
    ensureDirExists(VenvRoot)
    val cuIdDir = VenvRoot.resolve(cuid.toString)
    ensureDirExists(cuIdDir)

    val dir = cuIdDir.resolve(pvename)
    ensureDirExists(dir)

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

  private def ensureParentDir(path: Path): Unit = {
    val parent = path.getParent
    if (parent != null && !Files.exists(parent)) Files.createDirectories(parent)
  }

  private def writeMetadata(path: Path, lines: Seq[String]): Unit = {
    ensureParentDir(path)
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

  private def parsePackageName(line: String): String =
    line.split("==", 2).headOption.getOrElse(line).trim.toLowerCase

  private def pipEnv: Map[String, String] =
    Map(
      "PYTHONUNBUFFERED" -> "1",
      "PIP_PROGRESS_BAR" -> "off",
      "PIP_DISABLE_PIP_VERSION_CHECK" -> "1",
      "PIP_NO_INPUT" -> "1"
    )

  def pythonBin(cuid: Int, pveName: String): String =
    pythonBinPath(cuid, pveName).toAbsolutePath.toString

  /** Return (systemPkgs, userPkgs) as plain strings from metadata. */
  def getSystemAndUserPackages(cuid: Int, pveName: String): (Seq[String], Seq[String]) = {
    val sys = readMetadataList(systemPackagesPath(cuid, pveName))
    val usr = readMetadataList(userPackagesPath(cuid, pveName))
    (sys, usr)
  }

  def deletePackages(cuid: Int, packageName: String, pveName: String): List[String] = {
    val pipPath = pipBinPath(cuid, pveName).toAbsolutePath
    val userFile = userPackagesPath(cuid, pveName)
    val systemFile = systemPackagesPath(cuid, pveName)

    val systemPackages: Set[String] =
      readMetadataList(systemFile).map(parsePackageName).toSet

    val normalizedName = packageName.toLowerCase
    if (systemPackages.contains(normalizedName)) {
      return List(s"ERROR: '$packageName' is a system package and cannot be deleted.")
    }

    if (!Files.exists(pipPath)) {
      val msg = s"[PveManager] No pip found at $pipPath — PVE may not exist or is not initialized."
      println(msg)
      return List(msg)
    }

    try {
      val command = Seq(pipPath.toString, "uninstall", "-y", packageName)

      val logger = ProcessLogger(
        (out: String) => println(s"[pip] $out"),
        (err: String) => System.err.println(s"[pip][ERR] $err")
      )

      val exitCode = command.!(logger)

      if (exitCode == 0) {
        val existing = readMetadataList(userFile)
        val updated =
          existing.filterNot(line => parsePackageName(line) == normalizedName).sorted

        writeMetadata(userFile, updated)

        List(s"Exit code: $exitCode", s"Uninstalled $packageName successfully")
      } else {
        List(s"[PveManager] pip uninstall for '$packageName' failed with exit code $exitCode")
      }
    } catch {
      case e: Exception =>
        List(s"[PveManager] Failed to delete package for cuid=$cuid: ${e.getMessage}")
    }
  }

  private def tailFileToQueue(
      file: File,
      queue: BlockingQueue[String],
      prefix: String = "[pip] "
  ): AutoCloseable = {
    val raf = new RandomAccessFile(file, "r")
    raf.seek(raf.length())
    @volatile var running = true

    val thread = new Thread(() => {
      var buf = new StringBuilder
      val charset = StandardCharsets.UTF_8
      try {
        while (running) {
          val available = raf.length() - raf.getFilePointer
          if (available > 0) {
            val bytes = new Array[Byte](math.min(available, 8192).toInt)
            val n = raf.read(bytes)
            if (n > 0) {
              buf.append(new String(bytes, 0, n, charset))
              var newlineIndex = buf.indexOf("\n")
              while (newlineIndex >= 0) {
                val line = buf.substring(0, newlineIndex).trim
                if (shouldStream(line)) queue.put(prefix + line)
                buf = buf.delete(0, newlineIndex + 1)
                newlineIndex = buf.indexOf("\n")
              }
            }
          } else {
            Thread.sleep(100)
          }
        }
        val last = buf.result().trim
        if (shouldStream(last)) queue.put(prefix + last)
      } catch {
        case _: InterruptedException => ()
        case e: Exception            => queue.put(s"[pip][ERR] tail exception: ${e.getMessage}")
      } finally {
        raf.close()
      }
    })

    thread.setDaemon(true)
    thread.start()

    new AutoCloseable {
      override def close(): Unit = {
        running = false
        thread.interrupt()
        thread.join(500)
      }
    }
  }

  private def shouldStream(line: String): Boolean = {
    val s = line.trim
    if (s.isEmpty) return false

    val lower = s.toLowerCase

    if (lower.contains("found link")) return false
    if (lower.contains("skipping link")) return false
    if (lower.contains("cache")) return false
    if (lower.contains("caching")) return false

    true
  }

  private def runPipWithLog(
      cmd: Seq[String],
      env: Map[String, String],
      queue: BlockingQueue[String]
  ): Int = {
    val logFile = File.createTempFile("pip-live-", ".log")
    val fullCmd = if (cmd.contains("--log")) cmd else cmd ++ Seq("--log", logFile.getAbsolutePath)

    val tailer = tailFileToQueue(logFile, queue)

    val logger = ProcessLogger(
      out => if (shouldStream(out)) queue.put(s"[pip/stdout] $out"),
      err => if (shouldStream(err)) queue.put(s"[pip/stderr] $err")
    )

    val proc = Process(fullCmd, None, env.toSeq: _*).run(logger)
    val exitCode = proc.exitValue()

    try tailer.close()
    catch { case _: Throwable => () }

    queue.put(s"[pip] (log at ${logFile.getAbsolutePath})")
    exitCode
  }

//  def createNewPve(cuid: Int, queue: BlockingQueue[String], pveName: String): Unit = {
//    queue.put(s"[PVE with heartbeat and custom] Creating new PVE for cuid=$cuid and name=$pveName")
//
//    val venvDirPath = pveDir(cuid, pveName).toAbsolutePath
//    ensureDirExists(cuidDir(cuid, pveName))
//
//    val python = pythonBinPath(cuid, pveName).toAbsolutePath.toString
//    val envVars = pipEnv
//
//    val pveBase = sys.env.getOrElse("PVE_BASE", "/opt/pve-base")
//    val basePython = Paths.get(pveBase).resolve("bin").resolve("python")
//
//    if (!Files.exists(basePython)) {
//      queue.put(s"[PVE][ERR] Base venv not found at ${basePython.toString} (PVE_BASE=$pveBase)")
//      return
//    }
//
//    if (Files.exists(venvDirPath)) {
//      val rmCode = Process(Seq("bash", "-lc", s"rm -rf '${venvDirPath.toString}'")).!(
//        ProcessLogger(
//          out => queue.put(s"[pve] $out"),
//          err => queue.put(s"[pve][ERR] $err")
//        )
//      )
//      queue.put(s"[pve] removed existing venv with exit code $rmCode")
//    }
//
//    ensureDirExists(venvDirPath.getParent)
//    queue.put(s"[PVE] Copying base venv from $pveBase to ${venvDirPath.toString}")
//
//    val copyCode = Process(Seq("bash", "-lc", s"cp -a '${pveBase}' '${venvDirPath.toString}'")).!(
//      ProcessLogger(
//        out => queue.put(s"[pve] $out"),
//        err => queue.put(s"[pve][ERR] $err")
//      )
//    )
//    queue.put(s"[pve] base copy finished with exit code $copyCode")
//
//    if (copyCode != 0) {
//      queue.put(s"[PVE][ERR] Failed to copy base venv (exit=$copyCode)")
//      return
//    }
//
//    val fixCode = Process(
//      Seq(
//        "bash",
//        "-lc",
//        s"""
//           |set -e
//           |PY='${python}'
//           |BIN='${venvDirPath.toString}/bin'
//           |for f in "$$BIN"/*; do
//           |  [ -f "$$f" ] || continue
//           |  head -n 1 "$$f" | grep -q '^#!' || continue
//           |  head -n 1 "$$f" | grep -qi 'python' || continue
//           |  sed -i.bak "1s|^#!.*python.*|#!$$PY|" "$$f" || true
//           |  rm -f "$$f.bak" || true
//           |done
//           |""".stripMargin
//      )
//    ).!(
//      ProcessLogger(
//        out => queue.put(s"[pve] $out"),
//        err => queue.put(s"[pve][ERR] $err")
//      )
//    )
//    queue.put(s"[pve] rewrite finished with exit code $fixCode")
//
//    val Requirements: String =
//      """wheel==0.41.2
//        |setuptools==80.10.2
//        |numpy==2.1.0
//        |pandas==2.2.3
//        |ruff==0.14.7
//        |iniconfig==1.1.1
//        |loguru==0.7.0
//        |pyarrow==21.0.0
//        |pytest==7.4.0
//        |python-dateutil==2.8.2
//        |pytest-timeout==2.2.0
//        |protobuf==4.25.8
//        |betterproto==2.0.0b7
//        |typing==3.7.4.3
//        |pampy==0.3.0
//        |overrides==7.4.0
//        |typing_extensions==4.10.0
//        |pytest-reraise==2.1.2
//        |dataclasses==0.6
//        |Deprecated==1.2.14
//        |fs==2.4.16
//        |praw==7.6.1
//        |python-lsp-server[all]==1.12.0
//        |python-lsp-server[websockets]==1.12.0
//        |bidict==0.22.0
//        |cached_property==1.5.2
//        |psutil==5.9.0
//        |tzlocal==2.1
//        |pyiceberg==0.8.1
//        |readerwriterlock==1.0.9
//        |tenacity==8.5.0
//        |SQLAlchemy==2.0.37
//        |pg8000==1.31.5
//        |pympler==1.1
//        |""".stripMargin
//
//    val OperatorRequirements: String =
//      """|wordcloud==1.9.3
//         |plotly==5.24.1
//         |praw==7.6.1
//         |pillow==10.2.0
//         |pybase64==1.3.2
//         |torch==2.8.0
//         |scikit-learn==1.5.0
//         |transformers==4.57.3
//         |boto3==1.40.53
//         |""".stripMargin
//
//    ensureDirExists(metadataDir(cuid, pveName))
//    val reqFile1 = metadataDir(cuid, pveName).resolve("requirements.txt")
//    val reqFile2 = metadataDir(cuid, pveName).resolve("operator-requirements.txt")
//    Files.write(reqFile1, Requirements.getBytes(StandardCharsets.UTF_8))
//    Files.write(reqFile2, OperatorRequirements.getBytes(StandardCharsets.UTF_8))
//
//    queue.put("[PVE] Base environment copied; skipping system requirements install.")
//
//    val freezeOutput = Process(Seq(python, "-m", "pip", "freeze"), None, envVars.toSeq: _*).!!
//    val systemFreezeLines = freezeOutput.split("\n").map(_.trim).filter(_.nonEmpty).toSeq
//
//    writeMetadata(systemPackagesPath(cuid, pveName), systemFreezeLines)
//    writeMetadata(userPackagesPath(cuid, pveName), Seq.empty)
//
//    queue.put(s"[PVE] Created new environment for cuid=$cuid")
//  }

  def createNewPve(cuid: Int, queue: BlockingQueue[String], pveName: String): Unit = {
    queue.put(s"[PVE] Creating new PVE for cuid=$cuid with name=$pveName")

    val venvDirPath = pveDir(cuid, pveName).toAbsolutePath
    ensureDirExists(cuidDir(cuid, pveName))

    val python = pythonBinPath(cuid, pveName).toAbsolutePath.toString
    val envVars = pipEnv

    val Requirements: String =
      """wheel==0.41.2
        |setuptools==80.10.2
        |numpy==2.1.0
        |pandas==2.2.3
        |ruff==0.14.7
        |iniconfig==1.1.1
        |loguru==0.7.0
        |pyarrow==21.0.0
        |pytest==7.4.0
        |python-dateutil==2.8.2
        |pytest-timeout==2.2.0
        |protobuf==4.25.8
        |betterproto==2.0.0b7
        |typing==3.7.4.3
        |pampy==0.3.0
        |overrides==7.4.0
        |typing_extensions==4.10.0
        |pytest-reraise==2.1.2
        |dataclasses==0.6
        |Deprecated==1.2.14
        |fs==2.4.16
        |python-lsp-server[all]==1.12.0
        |python-lsp-server[websockets]==1.12.0
        |bidict==0.22.0
        |cached_property==1.5.2
        |psutil==5.9.0
        |tzlocal==2.1
        |pyiceberg==0.8.1
        |readerwriterlock==1.0.9
        |tenacity==8.5.0
        |SQLAlchemy==2.0.37
        |pg8000==1.31.5
        |pympler==1.1
        |""".stripMargin

    val OperatorRequirements: String =
      """|wordcloud==1.9.3
         |plotly==5.24.1
         |praw==7.6.1
         |pillow==10.2.0
         |pybase64==1.3.2
         |torch==2.8.0
         |scikit-learn==1.5.0
         |transformers==4.57.3
         |boto3==1.40.53
         |""".stripMargin

    val pveBase = sys.env.getOrElse("PVE_BASE", "/opt/pve-base")
    val basePython = Paths.get(pveBase).resolve("bin").resolve("python")

    val hasBasePve = Files.exists(basePython)

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

      ensureDirExists(venvDirPath.getParent)
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

      ensureDirExists(metadataDir(cuid, pveName))
      val reqFile1 = metadataDir(cuid, pveName).resolve("requirements.txt")
      val reqFile2 = metadataDir(cuid, pveName).resolve("operator-requirements.txt")
      Files.write(reqFile1, Requirements.getBytes(StandardCharsets.UTF_8))
      Files.write(reqFile2, OperatorRequirements.getBytes(StandardCharsets.UTF_8))

      queue.put("[PVE] Installing local base requirements")

      val installReqCode = Process(
        Seq(python, "-m", "pip", "install", "-r", reqFile1.toString),
        None,
        envVars.toSeq: _*
      ).!(ProcessLogger(_ => (), _ => ()))

      if (installReqCode != 0) {
        queue.put(s"[PVE][ERR] Failed to install requirements.txt (exit=$installReqCode)")
        return
      }

      queue.put("[PVE] Installing local operator requirements")

      val installOperatorReqCode = Process(
        Seq(python, "-m", "pip", "install", "-r", reqFile2.toString),
        None,
        envVars.toSeq: _*
      ).!(ProcessLogger(_ => (), _ => ()))

      if (installOperatorReqCode != 0) {
        queue.put(s"[PVE][ERR] Failed to install operator-requirements.txt (exit=$installOperatorReqCode)")
        return
      }

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

    ensureDirExists(venvDirPath.getParent)
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

    ensureDirExists(metadataDir(cuid, pveName))
    val reqFile1 = metadataDir(cuid, pveName).resolve("requirements.txt")
    val reqFile2 = metadataDir(cuid, pveName).resolve("operator-requirements.txt")
    Files.write(reqFile1, Requirements.getBytes(StandardCharsets.UTF_8))
    Files.write(reqFile2, OperatorRequirements.getBytes(StandardCharsets.UTF_8))

    queue.put("[PVE] Base environment copied; skipping system requirements install.")

    val freezeOutput = Process(Seq(python, "-m", "pip", "freeze"), None, envVars.toSeq: _*).!!
    val systemFreezeLines = freezeOutput.split("\n").map(_.trim).filter(_.nonEmpty).toSeq

    writeMetadata(systemPackagesPath(cuid, pveName), systemFreezeLines)
    writeMetadata(userPackagesPath(cuid, pveName), Seq.empty)

    queue.put(s"[PVE] Created new environment for cuid=$cuid")
  }

  def installPackages(
      packages: List[String],
      cuid: Int,
      queue: BlockingQueue[String],
      pveName: String
  ): Unit = {
    if (packages.isEmpty) return

    val python = pythonBinPath(cuid, pveName).toAbsolutePath.toString
    val pip = pipBinPath(cuid, pveName).toAbsolutePath.toString
    val userFile = userPackagesPath(cuid, pveName)
    val envVars = pipEnv

    val existing: Seq[String] = readMetadataList(userFile)
    val installedEntries = scala.collection.mutable.ListBuffer[String]()

    packages.foreach { pkg =>
      queue.put(s"[PVE] Installing package: $pkg")

      val pipCmd = Seq(
        python,
        "-u",
        "-m",
        "pip",
        "install",
        "-q",
        "--no-input",
        "--progress-bar",
        "off",
        pkg
      )

      val code = runPipWithLog(pipCmd, envVars, queue)
      queue.put(s"[pip] install($pkg) finished with exit code $code")

      if (code == 0) {
        val pkgNameOnly = pkg.trim
          .takeWhile(ch => ch.isLetterOrDigit || ch == '-' || ch == '_' || ch == '.')
        val showOut = Process(Seq(pip, "show", pkgNameOnly)).!!
        val lines = showOut.split("\n").map(_.trim)

        val nameOpt = lines.find(_.startsWith("Name:")).map(_.substring(5).trim)
        val versionOpt = lines.find(_.startsWith("Version:")).map(_.substring(8).trim)

        (nameOpt, versionOpt) match {
          case (Some(name), Some(version)) => installedEntries += s"${name.toLowerCase}==$version"
          case _                           => queue.put(s"[PVE][ERR] Could not extract version for $pkg")
        }
      }
    }

    val updated = (existing ++ installedEntries).distinct.sorted
    writeMetadata(userFile, updated)

    queue.put("\n\n\n[PVE] Final package list (pip list --format=freeze):")

    val out =
      Process(Seq(python, "-m", "pip", "list", "--format=freeze"), None, envVars.toSeq: _*).!!
    out.split("\n").foreach { line =>
      val trimmed = line.trim
      if (trimmed.nonEmpty) queue.put(s"[pip/list] $trimmed")
    }
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

package edu.uci.ics.amber.util

import org.jooq.types.UInteger

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.IteratorHasAsScala

object PathUtils {
  val coreDirectoryName = "core"

  /**
    * Gets the real path of the workflow-compiling-service home directory by:
    * 1) Checking if the current directory is workflow-compiling-service.
    * If it's not, then:
    * 2) Searching the siblings and children to find the home path.
    *
    * @return the real absolute path to the home directory
    */
  lazy val corePath: Path = {
    val currentWorkingDirectory = Paths.get(".").toRealPath()
    // check if the current directory is the home path
    if (isCorePath(currentWorkingDirectory)) {
      currentWorkingDirectory
    } else {
      // from current path's parent directory, search its children to find home path
      val searchChildren = Files
        .walk(currentWorkingDirectory.getParent, 3)
        .filter((path: Path) => isCorePath(path))
        .findAny
      if (searchChildren.isPresent) {
        searchChildren.get
      } else {
        throw new RuntimeException(
          f"Finding $coreDirectoryName home path failed. Current working directory is " + currentWorkingDirectory
        )
      }
    }
  }

  lazy val workflowCompilingServicePath: Path = corePath.resolve("workflow-compiling-service")

  private lazy val datasetsRootPath =
    corePath.resolve("amber").resolve("user-resources").resolve("datasets")

  def getDatasetPath(did: UInteger): Path = {
    datasetsRootPath.resolve(did.toString)
  }

  lazy val gitDirectoryPath: Path = corePath.getParent

  def getAllDatasetDirectories(): List[Path] = {
    if (Files.exists(datasetsRootPath)) {
      Files
        .list(datasetsRootPath)
        .filter(Files.isDirectory(_))
        .iterator()
        .asScala
        .toList
    } else {
      List.empty[Path]
    }
  }

  private def isCorePath(path: Path): Boolean = {
    path.toRealPath().endsWith(coreDirectoryName)
  }
}

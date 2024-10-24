package edu.uci.ics.amber.util

import java.nio.file.{Files, Path, Paths}

object PathUtils {
  val amberHomeDirectoryName = "amber"

  /**
    * Gets the real path of the workflow-compiling-service home directory by:
    * 1) Checking if the current directory is workflow-compiling-service.
    * If it's not, then:
    * 2) Searching the siblings and children to find the home path.
    *
    * @return the real absolute path to the home directory
    */
  lazy val amberHomePath: Path = {
    val currentWorkingDirectory = Paths.get(".").toRealPath()
    // check if the current directory is the home path
    if (isHomePath(currentWorkingDirectory)) {
      currentWorkingDirectory
    } else {
      // from current path's parent directory, search its children to find home path
      val searchChildren = Files
        .walk(currentWorkingDirectory.getParent, 3)
        .filter((path: Path) => isHomePath(path))
        .findAny
      if (searchChildren.isPresent) {
        searchChildren.get
      } else {
        throw new RuntimeException(
          f"Finding $amberHomeDirectoryName home path failed. Current working directory is " + currentWorkingDirectory
        )
      }
    }
  }

  // path of the dropwizard config file
  lazy val userResourcesConfigPath: Path = amberHomePath.resolve("user-resources")

  lazy val gitDirectoryPath: Path = amberHomePath.getParent.getParent

  private def isHomePath(path: Path): Boolean = {
    path.toRealPath().endsWith(amberHomeDirectoryName)
  }
}

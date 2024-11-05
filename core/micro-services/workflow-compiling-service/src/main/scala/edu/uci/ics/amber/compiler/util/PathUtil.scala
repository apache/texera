package edu.uci.ics.amber.compiler.util

import java.nio.file.{Files, Path, Paths}

object PathUtil {
  val homeDirectoryName = "workflow-compiling-service"

  /**
   * Gets the real path of the workflow-compiling-service home directory by:
   * 1) Checking if the current directory is workflow-compiling-service.
   * If it's not, then:
   * 2) Searching the siblings and children to find the home path.
   *
   * @return the real absolute path to the home directory
   */
  lazy val homePath: Path = {
    val currentWorkingDirectory = Paths.get(".").toRealPath()
    // check if the current directory is the home path
    if (isHomePath(currentWorkingDirectory)) {
      currentWorkingDirectory
    } else {
      // from current path's parent directory, search its children to find home path
      val searchChildren = Files
        .walk(currentWorkingDirectory.getParent, 2)
        .filter((path: Path) => isHomePath(path))
        .findAny
      if (searchChildren.isPresent) {
        searchChildren.get
      } else {
        throw new RuntimeException(
          f"Finding $homeDirectoryName home path failed. Current working directory is " + currentWorkingDirectory
        )
      }
    }
  }

  // path of the dropwizard config file
  lazy val webConfigFilePath: Path = homePath.resolve("src").resolve("main").resolve("resources").resolve("web-config.yaml")

  private def isHomePath(path: Path): Boolean = {
    path.toRealPath().endsWith(homeDirectoryName)
  }
}

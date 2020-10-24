package edu.uci.ics.texera.workflow.common

import java.nio.file.{Files, Path, Paths}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object Utils {

  val AMBER_HOME_FOLDER_NAME = "amber";

  final val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  /**
    * Gets the real path of the amber home directory by:
    * 1): check if the current directory is texera/core/amber
    * if it's not then:
    * 2): search the siblings and children to find the texera home path
    *
    * @return the real absolute path to amber home directory
    */
  lazy val amberHomePath: Path = {
    val currentWorkingDirectory = Paths.get(".").toRealPath()
    // check if the current directory is the amber home path
    if (isAmberHomePath(currentWorkingDirectory)) {
      currentWorkingDirectory
    } else {
      // from current path's parent directory, search its children to find amber home path
      // current max depth is set to 2 (current path's siblings and direct children)
      val searchChildren = Files
        .walk(currentWorkingDirectory.getParent, 2)
        .filter((path: Path) => isAmberHomePath(path))
        .findAny
      if (searchChildren.isPresent) {
        searchChildren.get
      }
      throw new RuntimeException(
        "Finding texera home path failed. Current working directory is " + currentWorkingDirectory
      )
    }
  }

  private def isAmberHomePath(path: Path): Boolean = {
    path.toRealPath().endsWith(AMBER_HOME_FOLDER_NAME)
  }

}

package texera.common

import java.nio.file.{Files, Path, Paths}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object TexeraUtils {

  val HOME_FOLDER_NAME = "AkkaExp";

  final val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  /**
   * Gets the real path of the texera home directory by:
   * 1): check if the current directory is texera/core (where TEXERA_HOME should be),
   * if it's not then:
   * 2): search the siblings and children to find the texera home path
   *
   * Finding texera home directory will fail
   *
   * @return the real absolute path to texera home directory
   */
  lazy val texeraHomePath: Path = {
    val currentWorkingDirectory = Paths.get(".").toRealPath()
    // check if the current directory is the texera home path
    if (isTexeraHomePath(currentWorkingDirectory)) {
      currentWorkingDirectory
    } else {
      // from current path's parent directory, search itschildren to find texera home path
      // current max depth is set to 2 (current path's siblings and direct children)
      val searchChildren = Files.walk(currentWorkingDirectory.getParent, 2).filter((path: Path) => isTexeraHomePath(path)).findAny
      if (searchChildren.isPresent) {
        searchChildren.get
      }
      throw new RuntimeException("Finding texera home path failed. Current working directory is " + currentWorkingDirectory)
    }
  }

  private def isTexeraHomePath(path: Path): Boolean = {
    path.toRealPath().endsWith(HOME_FOLDER_NAME)
  }


}

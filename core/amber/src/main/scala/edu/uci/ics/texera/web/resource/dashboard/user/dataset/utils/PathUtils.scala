package edu.uci.ics.texera.web.resource.dashboard.user.dataset.utils

import edu.uci.ics.texera.Utils
import org.jooq.types.UInteger

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.IteratorHasAsScala

object PathUtils {

  val DATASETS_ROOT = Utils.amberHomePath.resolve("user-resources").resolve("datasets")
  def getDatasetPath(did: UInteger): Path = {
    DATASETS_ROOT.resolve(did.toString)
  }

  def getAllDatasetDirectories(): List[Path] = {
    if (Files.exists(DATASETS_ROOT)) {
      Files
        .list(DATASETS_ROOT)
        .filter(Files.isDirectory(_))
        .iterator()
        .asScala
        .toList
    } else {
      List.empty[Path]
    }
  }
}

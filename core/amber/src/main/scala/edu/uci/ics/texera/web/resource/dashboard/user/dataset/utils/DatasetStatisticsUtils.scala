package edu.uci.ics.texera.web.resource.dashboard.user.dataset.utils

import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetResource.getUserCreatedDatasetList
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.utils.PathUtils.DATASETS_ROOT
import org.jooq.types.UInteger

import java.nio.file.{Files, Path}
import java.nio.file.attribute.BasicFileAttributes

object DatasetStatisticsUtils {
   private def getFolderSize(folderPath: Path): Long = {
    val walk = Files.walk(folderPath)
    try {
      walk
        .filter(Files.isRegularFile(_))
        .mapToLong(p => Files.readAttributes(p, classOf[BasicFileAttributes]).size())
        .sum()
    } finally {
      walk.close()
    }
  }

  def getUserDatasetSize(uid: UInteger): Long = {
    val datasetIDs = getUserCreatedDatasetList(uid)

    datasetIDs.dids.map { did =>
      val datasetPath = DATASETS_ROOT.resolve(did.toString)
      getFolderSize(datasetPath)
    }.sum
  }
}

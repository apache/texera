package edu.uci.ics.texera.web.resource.dashboard.user.dataset.utils

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.tables.Dataset.DATASET
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetResource.{DatasetIDs}
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.utils.PathUtils.DATASETS_ROOT
import org.jooq.types.UInteger
import scala.jdk.CollectionConverters._

import java.nio.file.{Files, Path}
import java.nio.file.attribute.BasicFileAttributes

object DatasetStatisticsUtils {
  final private lazy val context = SqlServer.createDSLContext()
  // this function retrieves the total counts of dataset that belongs to the user
  def getUserCreatedDatasetCount(uid: UInteger): Int = {
    val count = context
      .selectCount()
      .from(DATASET)
      .where(DATASET.OWNER_UID.eq(uid))
      .fetchOne(0, classOf[Int])

    count
  }

  // this function would return a list of dataset ids that belongs to the user
  private def getUserCreatedDatasetList(uid: UInteger): DatasetIDs = {
    val datasetIDs = context
      .select(
        DATASET.DID
      )
      .from(DATASET)
      .where(DATASET.OWNER_UID.eq(uid))
      .fetch()

    val idsList: List[UInteger] = datasetIDs.asScala.map(_.getValue(DATASET.DID)).toList

    DatasetIDs(idsList)
  }
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

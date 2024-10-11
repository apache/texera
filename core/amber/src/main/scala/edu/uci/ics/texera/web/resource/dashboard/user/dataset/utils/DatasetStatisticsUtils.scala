package edu.uci.ics.texera.web.resource.dashboard.user.dataset.utils

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.tables.Dataset.DATASET
import edu.uci.ics.texera.web.resource.dashboard.user.quota.UserQuotaResource.{DatasetQuota}
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
  private def getUserCreatedDatasetList(uid: UInteger): List[DatasetQuota] = {
    val result = context
      .select(
        DATASET.DID,
        DATASET.NAME,
        DATASET.CREATION_TIME
      )
      .from(DATASET)
      .where(DATASET.OWNER_UID.eq(uid))
      .fetch()

    result.asScala
      .map(record =>
        DatasetQuota(
          did = record.getValue(DATASET.DID),
          name = record.getValue(DATASET.NAME),
          creationTime = record.getValue(DATASET.CREATION_TIME).getTime(),
          size = 0
        )
      )
      .toList
  }

  private def getFolderSize(folderPath: Path): Long = {
    calculateSize(folderPath, isInHiddenDirectory)
  }

  private def calculateSize(folderPath: Path, filter: Path => Boolean): Long = {
    val walk = Files.walk(folderPath)
    try {
      walk
        .filter(p => Files.isRegularFile(p) && !filter(p))
        .mapToLong(p => Files.readAttributes(p, classOf[BasicFileAttributes]).size())
        .sum()
    } finally {
      walk.close()
    }
  }

  private def isInHiddenDirectory(path: Path): Boolean = {
    path.iterator().asScala.exists(p => Files.isHidden(p))
  }

  def getUserCreatedDatasets(uid: UInteger): List[DatasetQuota] = {
    val datasetList = getUserCreatedDatasetList(uid)
    datasetList.map { dataset =>
      val size = calculateDatasetSize(dataset.did)
      dataset.copy(size = size)
    }
  }

  def calculateDatasetSize(did: UInteger): Long = {
    val datasetPath = PathUtils.getDatasetPath(did)
    calculateSize(datasetPath, isInHiddenDirectory)
  }
}

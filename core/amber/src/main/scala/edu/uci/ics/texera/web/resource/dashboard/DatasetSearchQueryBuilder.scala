package edu.uci.ics.texera.web.resource.dashboard
import org.jooq.impl.DSL
import org.jooq.{Condition, GroupField, Record, TableLike}
import org.jooq.types.UInteger
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{DATASET, DATASET_USER_ACCESS}
import edu.uci.ics.texera.web.model.jooq.generated.enums.DatasetUserAccessPrivilege
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.Dataset
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource.DashboardClickableFileEntry
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetResource
import edu.uci.ics.texera.web.resource.dashboard.FulltextSearchQueryUtils.{
  getContainsFilter,
  getDateFilter,
  getFullTextSearchFilter,
  getSubstringSearchFilter
}
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetResource.DashboardDataset

import scala.jdk.CollectionConverters.CollectionHasAsScala
object DatasetSearchQueryBuilder extends SearchQueryBuilder {
  override protected val mappedResourceSchema: UnifiedResourceSchema = UnifiedResourceSchema(
    resourceType = DSL.inline(SearchQueryBuilder.DATASET_RESOURCE_TYPE),
    name = DATASET.NAME,
    description = DATASET.DESCRIPTION,
    creationTime = DATASET.CREATION_TIME,
    did = DATASET.DID,
    ownerId = DATASET.OWNER_UID,
    isDatasetPublic = DATASET.IS_PUBLIC,
    datasetStoragePath = DATASET.STORAGE_PATH,
    datasetUserAccess = DATASET_USER_ACCESS.PRIVILEGE
  )

  // Notice that this only select those datasets that users have access record.
  // For those public datasets, need external union to merge them
  override protected def constructFromClause(
      uid: UInteger,
      params: DashboardResource.SearchQueryParams
  ): TableLike[_] = {
    DATASET
      .leftJoin(DATASET_USER_ACCESS)
      .on(DATASET_USER_ACCESS.DID.eq(DATASET.DID))
      .where(
        DATASET_USER_ACCESS.UID.eq(uid)
      )
  }

  override protected def constructWhereClause(
      uid: UInteger,
      params: DashboardResource.SearchQueryParams
  ): Condition = {
    val splitKeywords = params.keywords.asScala
      .flatMap(_.split("[+\\-()<>~*@\"]"))
      .filter(_.nonEmpty)
      .toSeq

    getDateFilter(
      params.creationStartDate,
      params.creationEndDate,
      DATASET.CREATION_TIME
    )
      .and(getContainsFilter(params.datasetIds, DATASET.DID))
      .and(
        getFullTextSearchFilter(splitKeywords, List(DATASET.NAME, DATASET.DESCRIPTION))
          .or(
            getSubstringSearchFilter(
              splitKeywords,
              List(DATASET.NAME, DATASET.DESCRIPTION)
            )
          )
      )
  }
  override protected def getGroupByFields: Seq[GroupField] = Seq.empty
  override protected def toEntryImpl(
      uid: UInteger,
      record: Record
  ): DashboardResource.DashboardClickableFileEntry = {
    val dataset = record.into(DATASET).into(classOf[Dataset])

    val dd = DashboardDataset(
      dataset,
      record
        .get(
          DATASET_USER_ACCESS.PRIVILEGE,
          classOf[DatasetUserAccessPrivilege]
        ),
      dataset.getOwnerUid == uid
    )
    DashboardClickableFileEntry(
      resourceType = SearchQueryBuilder.DATASET_RESOURCE_TYPE,
      dataset = Some(dd)
    )
  }
}
class DatasetSearchQueryBuilder {}

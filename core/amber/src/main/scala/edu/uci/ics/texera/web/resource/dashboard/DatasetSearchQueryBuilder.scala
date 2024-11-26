package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.web.model.jooq.generated.Tables.{DATASET, DATASET_USER_ACCESS}
import edu.uci.ics.texera.web.model.jooq.generated.enums.DatasetUserAccessPrivilege
import edu.uci.ics.texera.web.model.jooq.generated.tables.User.USER
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{Dataset, User}
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource.DashboardClickableFileEntry
import edu.uci.ics.texera.web.resource.dashboard.FulltextSearchQueryUtils.{
  getContainsFilter,
  getDateFilter,
  getFullTextSearchFilter,
  getSubstringSearchFilter
}
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetResource
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetResource.DashboardDataset
import org.jooq.impl.DSL
import org.jooq.types.UInteger
import org.jooq.{Condition, GroupField, Record, TableLike}

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
    datasetUserAccess = DATASET_USER_ACCESS.PRIVILEGE
  )

  /*
   * constructs the FROM clause for querying datasets with specific access controls.
   *
   * Parameter:
   * - uid: UInteger - Represents the unique identifier of the current user.
   *  - uid is 'null' if the user is not logged in or performing a public search.
   *  - Otherwise, `uid` holds the identifier for the logged-in user.
   * - includePublic - Boolean - Specifies whether to include public datasets in the result.
   */
  override protected def constructFromClause(
      uid: UInteger,
      params: DashboardResource.SearchQueryParams,
      includePublic: Boolean = false
  ): TableLike[_] = {
    val baseJoin = DATASET
      .leftJoin(DATASET_USER_ACCESS)
      .on(DATASET_USER_ACCESS.DID.eq(DATASET.DID))
      .leftJoin(USER)
      .on(USER.UID.eq(DATASET.OWNER_UID))

    // Default condition starts as true, ensuring all datasets are selected initially.
    var condition: Condition = DSL.trueCondition()

    if (uid == null) {
      // If `uid` is null, the user is not logged in or performing a public search
      // We only select datasets marked as public
      condition = DATASET.IS_PUBLIC.eq(1.toByte)
    } else {
      // When `uid` is present, we add a condition to only include datasets with direct user access.
      val userAccessCondition = DATASET_USER_ACCESS.UID.eq(uid)

      if (includePublic) {
        // If `includePublic` is true, we extend visibility to public datasets as well.
        condition = userAccessCondition.or(DATASET.IS_PUBLIC.eq(1.toByte))
      } else {
        condition = userAccessCondition
      }
    }
    baseJoin.where(condition)
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

  override protected def getGroupByFields: Seq[GroupField] = {
    Seq(DATASET.DID)
  }

  override protected def toEntryImpl(
      uid: UInteger,
      record: Record
  ): DashboardResource.DashboardClickableFileEntry = {
    val dataset = record.into(DATASET).into(classOf[Dataset])
    val owner = record.into(USER).into(classOf[User])
    val dd = DashboardDataset(
      dataset,
      owner.getEmail,
      record
        .get(
          DATASET_USER_ACCESS.PRIVILEGE,
          classOf[DatasetUserAccessPrivilege]
        ),
      dataset.getOwnerUid == uid,
      List(),
      DatasetResource.calculateLatestDatasetVersionSize(dataset.getDid)
    )
    DashboardClickableFileEntry(
      resourceType = SearchQueryBuilder.DATASET_RESOURCE_TYPE,
      dataset = Some(dd)
    )
  }
}

class DatasetSearchQueryBuilder {}

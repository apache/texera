package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.web.model.jooq.generated.Tables.{PROJECT, PROJECT_USER_ACCESS}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.Project
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource.DashboardClickableFileEntry
import edu.uci.ics.texera.web.resource.dashboard.FulltextSearchQueryUtils.{
  getContainsFilter,
  getDateFilter,
  getFullTextSearchFilter,
  getSubstringSearchFilter
}
import org.jooq.impl.DSL
import org.jooq.types.UInteger
import org.jooq.{Condition, GroupField, Record, TableLike}

import scala.jdk.CollectionConverters.CollectionHasAsScala

object ProjectSearchQueryBuilder extends SearchQueryBuilder {

  override val mappedResourceSchema: UnifiedResourceSchema = UnifiedResourceSchema(
    resourceType = DSL.inline(SearchQueryBuilder.PROJECT_RESOURCE_TYPE),
    name = PROJECT.NAME,
    description = PROJECT.DESCRIPTION,
    creationTime = PROJECT.CREATION_TIME,
    lastModifiedTime = PROJECT.CREATION_TIME,
    pid = PROJECT.PID,
    ownerId = PROJECT.OWNER_ID,
    projectColor = PROJECT.COLOR
  )

  override protected def constructFromClause(
      uid: UInteger,
      params: DashboardResource.SearchQueryParams,
      includePublic: Boolean = false
  ): TableLike[_] = {
    PROJECT
      .leftJoin(PROJECT_USER_ACCESS)
      .on(PROJECT_USER_ACCESS.PID.eq(PROJECT.PID))
      .where(PROJECT_USER_ACCESS.UID.eq(uid))
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
      PROJECT.CREATION_TIME
    )
      .and(getContainsFilter(params.projectIds, PROJECT.PID))
      .and(
        getFullTextSearchFilter(splitKeywords, List(PROJECT.NAME, PROJECT.DESCRIPTION))
          .or(
            getSubstringSearchFilter(
              splitKeywords,
              List(PROJECT.NAME, PROJECT.DESCRIPTION)
            )
          )
      )
  }

  override protected def getGroupByFields: Seq[GroupField] = Seq.empty

  override def toEntryImpl(
      uid: UInteger,
      record: Record
  ): DashboardResource.DashboardClickableFileEntry = {
    val dp = record.into(PROJECT).into(classOf[Project])
    DashboardClickableFileEntry(SearchQueryBuilder.PROJECT_RESOURCE_TYPE, project = Some(dp))
  }
}

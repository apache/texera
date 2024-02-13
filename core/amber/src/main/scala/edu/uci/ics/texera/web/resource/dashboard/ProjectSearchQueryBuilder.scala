package edu.uci.ics.texera.web.resource.dashboard
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{PROJECT, PROJECT_USER_ACCESS}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.Project
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource.DashboardClickableFileEntry
import edu.uci.ics.texera.web.resource.dashboard.FulltextSearchQueryUtils.{
  getContainsFilter,
  getDateFilter,
  getFulltextSearchConditions
}
import org.jooq.{Condition, GroupField, OrderField, Record, TableLike}
import org.jooq.impl.DSL

import scala.jdk.CollectionConverters.CollectionHasAsScala
class ProjectSearchQueryBuilder extends SearchQueryBuilder {

  override val mappedResourceSchema: UnifiedResourceSchema = UnifiedResourceSchema(
    resourceType = DSL.inline(SearchQueryBuilder.PROJECT_RESOURCE_TYPE),
    name = PROJECT.NAME,
    description = PROJECT.DESCRIPTION,
    creationTime = PROJECT.CREATION_TIME,
    pid = PROJECT.PID,
    projectOwnerId = PROJECT.OWNER_ID,
    projectColor = PROJECT.COLOR
  )

  override protected def constructFromClause(
      user: SessionUser,
      params: DashboardResource.SearchQueryParams
  ): TableLike[_] = {
    PROJECT
      .leftJoin(PROJECT_USER_ACCESS)
      .on(PROJECT_USER_ACCESS.PID.eq(PROJECT.PID))
      .where(PROJECT_USER_ACCESS.UID.eq(user.getUid))
  }

  override protected def constructWhereClause(
      user: SessionUser,
      params: DashboardResource.SearchQueryParams
  ): Condition = {
    val splitKeywords = params.keywords.asScala
      .flatMap(_.split("[+\\-()<>~*@\"]"))
      .filter(_.nonEmpty)
      .toSeq

    getFulltextSearchConditions(
      splitKeywords,
      List(mappedResourceSchema.name, mappedResourceSchema.description)
    )
      .and(
        getDateFilter(
          params.creationStartDate,
          params.creationEndDate,
          mappedResourceSchema.creationTime
        )
      )
      .and(getContainsFilter(params.projectIds, mappedResourceSchema.pid))
  }

  override protected def constructGroupByClause(
      user: SessionUser,
      params: DashboardResource.SearchQueryParams
  ): Seq[GroupField] = Seq(PROJECT.PID)

  override protected def getOrderFields(
      user: SessionUser,
      params: DashboardResource.SearchQueryParams
  ): Seq[OrderField[_]] =
    FulltextSearchQueryUtils.getOrderFields(SearchQueryBuilder.PROJECT_RESOURCE_TYPE, params)

  override def toEntry(
      user: SessionUser,
      record: Record
  ): DashboardResource.DashboardClickableFileEntry = {
    val dp = record.into(PROJECT).into(classOf[Project])
    DashboardClickableFileEntry(SearchQueryBuilder.PROJECT_RESOURCE_TYPE, project = Some(dp))
  }
}

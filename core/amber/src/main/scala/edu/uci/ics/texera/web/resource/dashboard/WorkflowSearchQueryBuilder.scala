package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.web.model.jooq.generated.Tables.{
  PROJECT_USER_ACCESS,
  USER,
  WORKFLOW,
  WORKFLOW_OF_PROJECT,
  WORKFLOW_OF_USER,
  WORKFLOW_USER_ACCESS
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.Workflow
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource.DashboardClickableFileEntry
import edu.uci.ics.texera.web.resource.dashboard.FulltextSearchQueryUtils.{
  getFullTextSearchFilter,
  getSubstringSearchFilter,
  getContainsFilter,
  getDateFilter,
  getOperatorsFilter
}
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource.DashboardWorkflow
import org.jooq.{Condition, GroupField, Record, TableLike}
import org.jooq.impl.DSL
import org.jooq.impl.DSL.groupConcatDistinct
import org.jooq.types.UInteger

import scala.jdk.CollectionConverters.CollectionHasAsScala

object WorkflowSearchQueryBuilder extends SearchQueryBuilder {

  override val mappedResourceSchema: UnifiedResourceSchema = {
    UnifiedResourceSchema(
      resourceType = DSL.inline(SearchQueryBuilder.WORKFLOW_RESOURCE_TYPE),
      name = WORKFLOW.NAME,
      description = WORKFLOW.DESCRIPTION,
      creationTime = WORKFLOW.CREATION_TIME,
      wid = WORKFLOW.WID,
      lastModifiedTime = WORKFLOW.LAST_MODIFIED_TIME,
      workflowUserAccess = WORKFLOW_USER_ACCESS.PRIVILEGE,
      uid = WORKFLOW_OF_USER.UID,
      ownerId = WORKFLOW_OF_USER.UID,
      userName = USER.NAME,
      projectsOfWorkflow = groupConcatDistinct(WORKFLOW_OF_PROJECT.PID)
    )
  }

  override protected def constructFromClause(
      uid: UInteger,
      params: DashboardResource.SearchQueryParams
  ): TableLike[_] = {
    WORKFLOW
      .leftJoin(WORKFLOW_USER_ACCESS)
      .on(WORKFLOW_USER_ACCESS.WID.eq(WORKFLOW.WID))
      .leftJoin(WORKFLOW_OF_USER)
      .on(WORKFLOW_OF_USER.WID.eq(WORKFLOW.WID))
      .leftJoin(USER)
      .on(USER.UID.eq(WORKFLOW_OF_USER.UID))
      .leftJoin(WORKFLOW_OF_PROJECT)
      .on(WORKFLOW_OF_PROJECT.WID.eq(WORKFLOW.WID))
      .leftJoin(PROJECT_USER_ACCESS)
      .on(PROJECT_USER_ACCESS.PID.eq(WORKFLOW_OF_PROJECT.PID))
      .where(
        WORKFLOW_USER_ACCESS.UID.eq(uid).or(PROJECT_USER_ACCESS.UID.eq(uid))
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
      WORKFLOW.CREATION_TIME
    )
      // Apply lastModified_time date filter
      .and(
        getDateFilter(
          params.modifiedStartDate,
          params.modifiedEndDate,
          WORKFLOW.LAST_MODIFIED_TIME
        )
      )
      // Apply workflowID filter
      .and(getContainsFilter(params.workflowIDs, WORKFLOW.WID))
      // Apply owner filter
      .and(getContainsFilter(params.owners, USER.EMAIL))
      // Apply operators filter
      .and(getOperatorsFilter(params.operators, WORKFLOW.CONTENT))
      // Apply projectId filter
      .and(getContainsFilter(params.projectIds, WORKFLOW_OF_PROJECT.PID))
      // Apply fulltext search filter
      .and(
        getFullTextSearchFilter(
          splitKeywords,
          List(WORKFLOW.NAME, WORKFLOW.DESCRIPTION, WORKFLOW.CONTENT)
        ).or(
          getSubstringSearchFilter(
            splitKeywords,
            List(WORKFLOW.NAME, WORKFLOW.DESCRIPTION, WORKFLOW.CONTENT)
          )
        )
      )
  }

  override protected def getGroupByFields: Seq[GroupField] = {
    Seq(WORKFLOW.WID)
  }

  override def toEntryImpl(
      uid: UInteger,
      record: Record
  ): DashboardResource.DashboardClickableFileEntry = {
    val pidField = groupConcatDistinct(WORKFLOW_OF_PROJECT.PID)
    val dw = DashboardWorkflow(
      record.into(WORKFLOW_OF_USER).getUid.eq(uid),
      record
        .get(WORKFLOW_USER_ACCESS.PRIVILEGE)
        .toString,
      record.into(USER).getName,
      record.into(WORKFLOW).into(classOf[Workflow]),
      if (record.get(pidField) == null) {
        List[UInteger]()
      } else {
        record
          .get(pidField)
          .asInstanceOf[String]
          .split(',')
          .map(number => UInteger.valueOf(number))
          .toList
      },
      record.into(USER).getUid
    )
    DashboardClickableFileEntry(SearchQueryBuilder.WORKFLOW_RESOURCE_TYPE, workflow = Some(dw))
  }
}

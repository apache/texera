package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.web.auth.SessionUser
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
  getContainsFilter,
  getDateFilter,
  getFulltextSearchConditions,
  getOperatorsFilter
}
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource.DashboardWorkflow
import org.jooq.{Condition, GroupField, OrderField, Record, TableLike}
import org.jooq.impl.DSL
import org.jooq.impl.DSL.groupConcatDistinct
import org.jooq.types.UInteger

import scala.jdk.CollectionConverters.CollectionHasAsScala

class WorkflowSearchQueryBuilder extends SearchQueryBuilder {

  override val mappedResourceSchema: ResourceFieldEntry = {
    new ResourceFieldEntry(
      _resourceType = DSL.inline(SearchQueryBuilder.WORKFLOW_RESOURCE_TYPE),
      _name = WORKFLOW.NAME,
      _description = WORKFLOW.DESCRIPTION,
      _creationTime = WORKFLOW.CREATION_TIME,
      _wid = WORKFLOW.WID,
      _workflowLastModifiedTime = WORKFLOW.LAST_MODIFIED_TIME,
      _workflowUserAccess = WORKFLOW_USER_ACCESS.PRIVILEGE,
      _uid = WORKFLOW_OF_USER.UID,
      _projectsOfWorkflow = groupConcatDistinct(WORKFLOW_OF_PROJECT.PID)
    )
  }

  override protected def constructFromClause(
      user: SessionUser,
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
        WORKFLOW_USER_ACCESS.UID.eq(user.getUid).or(PROJECT_USER_ACCESS.UID.eq(user.getUid))
      )
  }

  override protected def constructWhereClause(
      user: SessionUser,
      params: DashboardResource.SearchQueryParams
  ): Condition = {
    val splitKeywords = params.keywords.asScala
      .flatMap(_.split("[+\\-()<>~*@\"]"))
      .filter(_.nonEmpty)
      .toSeq
    getDateFilter(
      params.creationStartDate,
      params.creationEndDate,
      mappedResourceSchema.creationTime
    )
      // Apply lastModified_time date filter
      .and(
        getDateFilter(
          params.modifiedStartDate,
          params.modifiedEndDate,
          mappedResourceSchema.workflowLastModifiedTime
        )
      )
      // Apply workflowID filter
      .and(getContainsFilter(params.workflowIDs, mappedResourceSchema.wid))
      // Apply owner filter
      .and(getContainsFilter(params.owners, USER.EMAIL))
      // Apply operators filter
      .and(getOperatorsFilter(params.operators, WORKFLOW.CONTENT))
      // Apply projectId filter
      .and(getContainsFilter(params.projectIds, mappedResourceSchema.pid))
      // Apply fulltext search filter
      .and(
        getFulltextSearchConditions(
          splitKeywords,
          List(mappedResourceSchema.name, mappedResourceSchema.description, WORKFLOW.CONTENT)
        )
      )
  }

  override protected def constructGroupByClause(
      user: SessionUser,
      params: DashboardResource.SearchQueryParams
  ): Seq[GroupField] = {
    Seq(WORKFLOW.WID)
  }

  override protected def getOrderFields(
      user: SessionUser,
      params: DashboardResource.SearchQueryParams
  ): Seq[OrderField[_]] = {
    FulltextSearchQueryUtils.getOrderFields(SearchQueryBuilder.WORKFLOW_RESOURCE_TYPE, params)
  }

  override def toEntry(
      user: SessionUser,
      record: Record
  ): DashboardResource.DashboardClickableFileEntry = {
    val dw = DashboardWorkflow(
      record.into(WORKFLOW_OF_USER).getUid.eq(user.getUid),
      record
        .get(mappedResourceSchema.workflowUserAccess)
        .toString,
      record.into(USER).getName,
      record.into(WORKFLOW).into(classOf[Workflow]),
      if (record.get(mappedResourceSchema.projectsOfWorkflow) == null) {
        List[UInteger]()
      } else {
        record
          .get(mappedResourceSchema.projectsOfWorkflow)
          .asInstanceOf[String]
          .split(',')
          .map(number => UInteger.valueOf(number))
          .toList
      }
    )
    DashboardClickableFileEntry(SearchQueryBuilder.WORKFLOW_RESOURCE_TYPE, workflow = Some(dw))
  }
}

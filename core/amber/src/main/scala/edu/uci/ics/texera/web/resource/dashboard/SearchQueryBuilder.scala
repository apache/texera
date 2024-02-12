package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource.{
  DashboardClickableFileEntry,
  SearchQueryParams
}
import edu.uci.ics.texera.web.resource.dashboard.SearchQueryBuilder.context
import org.jooq.{Condition, GroupField, OrderField, Record, SelectLimitStep, TableLike}
object SearchQueryBuilder {

  final lazy val context = SqlServer.createDSLContext()
  val FILE_RESOURCE_TYPE = "file"
  val WORKFLOW_RESOURCE_TYPE = "workflow"
  val PROJECT_RESOURCE_TYPE = "project"
  val ALL_RESOURCE_TYPE = ""
}

trait SearchQueryBuilder {

  protected val mappedResourceSchema: UnifiedResourceSchema

  protected def constructFromClause(user: SessionUser, params: SearchQueryParams): TableLike[_]

  protected def constructWhereClause(user: SessionUser, params: SearchQueryParams): Condition

  protected def constructGroupByClause(
      user: SessionUser,
      params: SearchQueryParams
  ): Seq[GroupField] = Seq.empty

  protected def getOrderFields(user: SessionUser, params: SearchQueryParams): Seq[OrderField[_]]

  def toEntry(user: SessionUser, record: Record): DashboardClickableFileEntry

  final def constructQuery(
      user: SessionUser,
      params: SearchQueryParams
  ): SelectLimitStep[Record] = {
    context
      .select(mappedResourceSchema.getAllFields: _*)
      .from(constructFromClause(user, params))
      .where(constructWhereClause(user, params))
      .groupBy(constructGroupByClause(user, params): _*)
      .orderBy(getOrderFields(user, params): _*)
  }

}

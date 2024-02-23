package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource.{
  DashboardClickableFileEntry,
  SearchQueryParams
}
import edu.uci.ics.texera.web.resource.dashboard.SearchQueryBuilder.context
import org.jooq.types.UInteger
import org.jooq.{Condition, GroupField, Record, SelectGroupByStep, SelectHavingStep, TableLike}
object SearchQueryBuilder {

  final lazy val context = SqlServer.createDSLContext()
  val FILE_RESOURCE_TYPE = "file"
  val WORKFLOW_RESOURCE_TYPE = "workflow"
  val PROJECT_RESOURCE_TYPE = "project"
  val ALL_RESOURCE_TYPE = ""
}

trait SearchQueryBuilder {

  protected val mappedResourceSchema: UnifiedResourceSchema

  protected def constructFromClause(uid: UInteger, params: SearchQueryParams): TableLike[_]

  protected def constructWhereClause(uid: UInteger, params: SearchQueryParams): Condition

  protected def getGroupByFields: Seq[GroupField] = Seq.empty

  protected def toEntryImpl(uid: UInteger, record: Record): DashboardClickableFileEntry

  private def translateRecord(record: Record): Record = mappedResourceSchema.translateRecord(record)

  def toEntry(uid: UInteger, record: Record): DashboardClickableFileEntry = {
    toEntryImpl(uid, translateRecord(record))
  }

  final def constructQuery(
      uid: UInteger,
      params: SearchQueryParams
  ): SelectHavingStep[Record] = {
    val query: SelectGroupByStep[Record] = context
      .select(mappedResourceSchema.allFields: _*)
      .from(constructFromClause(uid, params))
      .where(constructWhereClause(uid, params))
    val groupByFields = getGroupByFields
    if (groupByFields.nonEmpty) {
      query.groupBy(groupByFields: _*)
    } else {
      query
    }
  }

}

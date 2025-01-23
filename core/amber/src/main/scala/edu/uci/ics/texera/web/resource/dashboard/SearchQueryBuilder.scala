package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource.{
  DashboardClickableFileEntry,
  SearchQueryParams
}
import edu.uci.ics.texera.web.resource.dashboard.SearchQueryBuilder.context
import org.jooq._
import org.jooq.types.UInteger

object SearchQueryBuilder {

  final lazy val context = SqlServer
    .getInstance(StorageConfig.jdbcUrl, StorageConfig.jdbcUsername, StorageConfig.jdbcPassword)
    .createDSLContext()
  val FILE_RESOURCE_TYPE = "file"
  val WORKFLOW_RESOURCE_TYPE = "workflow"
  val PROJECT_RESOURCE_TYPE = "project"
  val DATASET_RESOURCE_TYPE = "dataset"
  val ALL_RESOURCE_TYPE = ""
}

trait SearchQueryBuilder {

  protected val mappedResourceSchema: UnifiedResourceSchema

  protected def constructFromClause(
      uid: UInteger,
      params: SearchQueryParams,
      includePublic: Boolean = false
  ): TableLike[_]

  protected def constructWhereClause(uid: UInteger, params: SearchQueryParams): Condition

  protected def getGroupByFields: Seq[GroupField] = Seq.empty

  protected def toEntryImpl(uid: UInteger, record: Record): DashboardClickableFileEntry

  private def translateRecord(record: Record): Record = mappedResourceSchema.translateRecord(record)

  def toEntry(uid: UInteger, record: Record): DashboardClickableFileEntry = {
    toEntryImpl(uid, translateRecord(record))
  }

  final def constructQuery(
      uid: UInteger,
      params: SearchQueryParams,
      includePublic: Boolean
  ): SelectHavingStep[Record] = {
    val query: SelectGroupByStep[Record] = context
      .select(mappedResourceSchema.allFields: _*)
      .from(constructFromClause(uid, params, includePublic))
      .where(constructWhereClause(uid, params))
    val groupByFields = getGroupByFields
    if (groupByFields.nonEmpty) {
      query.groupBy(groupByFields: _*)
    } else {
      query
    }
  }

}

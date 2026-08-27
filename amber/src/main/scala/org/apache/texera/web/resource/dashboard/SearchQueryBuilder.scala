/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.web.resource.dashboard

import org.apache.texera.amber.core.storage.ResourceType
import org.apache.texera.dao.SqlServer
import org.apache.texera.web.resource.dashboard.DashboardResource.{
  DashboardClickableFileEntry,
  SearchQueryParams
}
import org.apache.texera.web.resource.dashboard.SearchQueryBuilder.context
import org.jooq._

object SearchQueryBuilder {

  def context =
    SqlServer
      .getInstance()
      .createDSLContext()
  // Resource-type discriminators for dashboard search. They are inlined into the search SQL,
  // pattern-matched in DashboardResource, and duplicated as bare literals in the frontend.
  // DATASET_RESOURCE_TYPE aliases ResourceType.Dataset, whose value doubles as the leading
  // segment of a storage logical path, so renaming that prefix silently changes search results
  // with no compile error here.
  // TODO: give every resource type (workflow, dataset, model) a single shared
  //       constant/enum instead of these raw strings, so the search contract has one source of
  //       truth and no longer borrows an unrelated storage constant.
  val WORKFLOW_RESOURCE_TYPE = "workflow"
  val DATASET_RESOURCE_TYPE = ResourceType.Dataset.toString
  val ALL_RESOURCE_TYPE = ""
}

trait SearchQueryBuilder {

  protected val mappedResourceSchema: UnifiedResourceSchema

  protected def constructFromClause(
      uid: Integer,
      params: SearchQueryParams,
      includePublic: Boolean = false
  ): TableLike[_]

  protected def constructWhereClause(uid: Integer, params: SearchQueryParams): Condition

  protected def getGroupByFields: Seq[GroupField] = Seq.empty

  protected def toEntryImpl(uid: Integer, record: Record): DashboardClickableFileEntry

  private def translateRecord(record: Record): Record = mappedResourceSchema.translateRecord(record)

  def toEntry(uid: Integer, record: Record): DashboardClickableFileEntry = {
    toEntryImpl(uid, translateRecord(record))
  }

  final def constructQuery(
      uid: Integer,
      params: SearchQueryParams,
      includePublic: Boolean
  ): SelectHavingStep[Record] = {
    val query: SelectGroupByStep[Record] = context
      .selectDistinct(mappedResourceSchema.allFields: _*)
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

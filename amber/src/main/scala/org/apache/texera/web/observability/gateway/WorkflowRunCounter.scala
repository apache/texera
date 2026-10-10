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

package org.apache.texera.web.observability.gateway

import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.WORKFLOW_EXECUTIONS
import org.apache.texera.web.observability.gateway.dtos.TimeWindow
import org.jooq.Condition

import java.sql.Timestamp

/**
  * Exact, system-wide count of workflow executions started within a time
  * window, read straight from the `workflow_executions` table.
  *
  * This is the authoritative source for the "total runs" stat: the metrics
  * counter (texera_workflow_starts_total) is sampled and lossy by design, so
  * it can only estimate. The relational store has one row per run, so a
  * COUNT(*) over `starting_time` is exact and auditable.
  *
  * The count is intentionally NOT scoped to a caller's workflows: the
  * observability dashboard is an admin-only, system-wide view. An optional
  * `userId` narrows the count to runs launched by one user.
  */
trait WorkflowRunCounter {

  /** Number of executions whose `starting_time` is in `[window.from,
    *  window.to)`. When `userId` is set, only that user's runs are counted.
    */
  def countRuns(window: TimeWindow, userId: Option[Long]): Long
}

object WorkflowRunCounter {

  /** Production path: a single COUNT(*) against `workflow_executions`. */
  class Jooq extends WorkflowRunCounter {
    override def countRuns(window: TimeWindow, userId: Option[Long]): Long = {
      val ctx = SqlServer.getInstance().createDSLContext()
      // Half-open [from, to): matches the per-bucket window semantics used
      // elsewhere, so adjacent windows tile without double-counting a run on
      // a boundary. starting_time is a plain TIMESTAMP; build it from epoch
      // millis the same way runs are stamped (System.currentTimeMillis).
      val from = new Timestamp(window.from.toEpochMilli)
      val to = new Timestamp(window.to.toEpochMilli)
      var cond: Condition = WORKFLOW_EXECUTIONS.STARTING_TIME
        .ge(from)
        .and(WORKFLOW_EXECUTIONS.STARTING_TIME.lt(to))
      // uid is a 32-bit column; user ids comfortably fit in an Int.
      userId.foreach { uid =>
        cond = cond.and(WORKFLOW_EXECUTIONS.UID.eq(Integer.valueOf(uid.toInt)))
      }
      val count = ctx
        .selectCount()
        .from(WORKFLOW_EXECUTIONS)
        .where(cond)
        .fetchOne(0, classOf[java.lang.Long])
      Option(count).map(_.longValue()).getOrElse(0L)
    }
  }

  /** Test double: returns a fixed count and records the last arguments so a
    *  spec can assert the window / userId were threaded through.
    */
  class Stub(result: Long) extends WorkflowRunCounter {
    @volatile var lastWindow: Option[TimeWindow] = None
    @volatile var lastUserId: Option[Long] = None
    override def countRuns(window: TimeWindow, userId: Option[Long]): Long = {
      lastWindow = Some(window)
      lastUserId = userId
      result
    }
  }
}

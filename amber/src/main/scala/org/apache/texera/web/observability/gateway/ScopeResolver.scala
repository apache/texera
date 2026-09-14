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

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.{
  PROJECT_USER_ACCESS,
  WORKFLOW_OF_USER,
  WORKFLOW_USER_ACCESS
}

import scala.jdk.CollectionConverters._

/**
  * Resolves a [[GatewayScope]] for a session user.
  *
  * The single security invariant: there is no public path through
  * which a caller can widen the scope. The resolver reads
  * authoritative state (jOOQ tables) and returns a closed set of
  * allowed workflow / project ids — every backend builder consumes
  * this set, and any request that names a workflow id outside it is
  * rejected at the resource layer.
  *
  * Provided in two flavours:
  *   - [[ScopeResolver.Jooq]] — the production path, queries the
  *     existing access-control tables.
  *   - [[ScopeResolver.Stub]] — used by tests so they don't need to
  *     stand up a real database.
  */
trait ScopeResolver {
  def resolve(user: SessionUser): GatewayScope

  /** Membership check: does the caller actually have access to the
    *  named workflow id? Returns true if no workflowId was supplied
    *  (defaults to caller's full scope) or if the id is in the
    *  resolved allow-set.
    *
    *  Implementation note: Jackson Scala module deserializes a JSON
    *  number that fits in 32 bits as java.lang.Integer regardless of
    *  the declared `Option[Long]` type (type parameters are erased on
    *  the JVM). A typed `id: Long` closure then triggers a runtime
    *  Integer→Long unbox via BoxesRunTime.unboxToLong, which throws a
    *  ClassCastException. We normalise to a primitive long up front
    *  via `Number.longValue()` so the contains-check is type-safe.
    */
  def assertWorkflowAllowed(scope: GatewayScope, workflowId: Option[Long]): Boolean =
    workflowId match {
      case None     => true
      case Some(id) =>
        // The `id` here may actually be a java.lang.Integer at runtime;
        // route through Number.longValue() so the unbox cannot fail.
        val asLong = id.asInstanceOf[Any] match {
          case n: java.lang.Number => n.longValue()
          case other               => other.toString.toLong
        }
        scope.allowedWorkflowIds.contains(asLong)
    }
}

object ScopeResolver {

  /** Production implementation backed by jOOQ. Queries
    *  WORKFLOW_OF_USER (owned workflows) ∪ WORKFLOW_USER_ACCESS
    *  (shared workflows) for the user, and PROJECT_USER_ACCESS for
    *  the set of projects the user can see.
    */
  class Jooq extends ScopeResolver with LazyLogging {
    override def resolve(user: SessionUser): GatewayScope = {
      val ctx = SqlServer.getInstance().createDSLContext()
      val uid = user.getUid

      // Owned + shared workflow ids. A jOOQ UNION call would be
      // ideal but the generated DSL is fussier with type
      // inference; two queries collected into a Set keeps this
      // straightforward and equally safe.
      val ownedWids: Set[Long] = ctx
        .selectFrom(WORKFLOW_OF_USER)
        .where(WORKFLOW_OF_USER.UID.eq(uid))
        .fetch()
        .asScala
        .map(_.getWid.longValue())
        .toSet

      val sharedWids: Set[Long] = ctx
        .selectFrom(WORKFLOW_USER_ACCESS)
        .where(WORKFLOW_USER_ACCESS.UID.eq(uid))
        .fetch()
        .asScala
        .map(_.getWid.longValue())
        .toSet

      val allowedProjects: Set[Long] = ctx
        .selectFrom(PROJECT_USER_ACCESS)
        .where(PROJECT_USER_ACCESS.UID.eq(uid))
        .fetch()
        .asScala
        .map(_.getPid.longValue())
        .toSet

      logger.debug(
        s"resolved observability scope for user $uid: ${ownedWids.size} owned + " +
          s"${sharedWids.size} shared workflow(s), ${allowedProjects.size} project(s)"
      )
      GatewayScope(
        userId = uid.longValue(),
        allowedWorkflowIds = ownedWids ++ sharedWids,
        allowedProjectIds = allowedProjects
      )
    }
  }

  /** Test double. Constructed with a static scope; ignores the
    *  caller's SessionUser.
    */
  class Stub(scope: GatewayScope) extends ScopeResolver {
    override def resolve(user: SessionUser): GatewayScope = scope
  }
}

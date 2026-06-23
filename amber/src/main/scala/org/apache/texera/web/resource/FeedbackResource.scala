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

package org.apache.texera.web.resource

import io.dropwizard.auth.Auth
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.FEEDBACK
import org.apache.texera.dao.jooq.generated.tables.pojos.Feedback
import org.apache.texera.web.resource.FeedbackResource.{
  FeedbackCount,
  FeedbackEntry,
  SubmitFeedbackRequest
}
import org.jooq.impl.DSL

import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core._
import scala.jdk.CollectionConverters._

object FeedbackResource {
  case class SubmitFeedbackRequest(message: String)
  case class FeedbackEntry(fid: Integer, uid: Integer, message: String, creationTime: Long)
  case class FeedbackCount(uid: Integer, count: Integer)
}

@Path("/feedback")
@RolesAllowed(Array("REGULAR", "ADMIN"))
class FeedbackResource {

  /**
    * Submit a new feedback message for the currently logged-in user. The fid
    * (SERIAL) and creation_time (DEFAULT) columns are populated by the database.
    */
  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def submitFeedback(request: SubmitFeedbackRequest, @Auth sessionUser: SessionUser): Unit = {
    val message = Option(request).flatMap(r => Option(r.message)).map(_.trim).getOrElse("")
    if (message.isEmpty) {
      throw new BadRequestException("feedback message cannot be empty")
    }
    SqlServer
      .getInstance()
      .createDSLContext()
      .insertInto(FEEDBACK, FEEDBACK.UID, FEEDBACK.MESSAGE)
      .values(sessionUser.getUid, message)
      .execute()
  }

  /**
    * List the feedback submitted by the currently logged-in user, newest first.
    */
  @GET
  @Produces(Array(MediaType.APPLICATION_JSON))
  def listMyFeedback(@Auth sessionUser: SessionUser): List[FeedbackEntry] = {
    fetchFeedbackByUid(sessionUser.getUid)
  }

  /**
    * Admin only: number of feedback messages per user, for users who have
    * submitted at least one. Users with zero feedback are omitted.
    */
  @GET
  @Path("/counts")
  @RolesAllowed(Array("ADMIN"))
  @Produces(Array(MediaType.APPLICATION_JSON))
  def feedbackCounts(): List[FeedbackCount] = {
    val countField = DSL.count()
    SqlServer
      .getInstance()
      .createDSLContext()
      .select(FEEDBACK.UID, countField)
      .from(FEEDBACK)
      .groupBy(FEEDBACK.UID)
      .fetch()
      .asScala
      .map(record => FeedbackCount(record.get(FEEDBACK.UID), record.get(countField)))
      .toList
  }

  /**
    * Admin only: list the feedback submitted by a specific user, newest first.
    */
  @GET
  @Path("/user")
  @RolesAllowed(Array("ADMIN"))
  @Produces(Array(MediaType.APPLICATION_JSON))
  def listUserFeedback(@QueryParam("user_id") userId: Integer): List[FeedbackEntry] = {
    if (userId == null) {
      throw new BadRequestException("user_id is required")
    }
    fetchFeedbackByUid(userId)
  }

  private def fetchFeedbackByUid(uid: Integer): List[FeedbackEntry] = {
    SqlServer
      .getInstance()
      .createDSLContext()
      .selectFrom(FEEDBACK)
      .where(FEEDBACK.UID.eq(uid))
      .orderBy(FEEDBACK.CREATION_TIME.desc())
      .fetchInto(classOf[Feedback])
      .asScala
      .map(feedback =>
        FeedbackEntry(
          feedback.getFid,
          feedback.getUid,
          feedback.getMessage,
          feedback.getCreationTime.getTime
        )
      )
      .toList
  }
}

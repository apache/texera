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

import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.DatabaseFlatSpec
import org.apache.texera.dao.jooq.generated.Tables.FEEDBACK
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.apache.texera.web.resource.FeedbackResource.SubmitFeedbackRequest

import java.util.UUID
import javax.ws.rs.BadRequestException

class FeedbackResourceSpec extends DatabaseFlatSpec() {

  private val testUid = 9000 + scala.util.Random.nextInt(1000)
  private val otherUid = testUid + 1
  private var sessionUser: SessionUser = _
  private var otherSessionUser: SessionUser = _
  private val resource = new FeedbackResource

  private def makeUser(uid: Int, name: String): User = {
    val user = new User
    user.setUid(uid)
    user.setName(name)
    user.setEmail(s"user_${UUID.randomUUID()}@example.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  override protected def onBeforeAll(): Unit = {
    val userDao = new UserDao(getDSLContext.configuration())
    val testUser = makeUser(testUid, "feedback_spec_user")
    val otherUser = makeUser(otherUid, "feedback_spec_other_user")
    userDao.insert(testUser)
    userDao.insert(otherUser)
    sessionUser = new SessionUser(testUser)
    otherSessionUser = new SessionUser(otherUser)
  }

  override protected def cleanup(): Unit =
    getDSLContext.deleteFrom(FEEDBACK).execute()

  private def submit(message: String, caller: SessionUser = sessionUser): Unit =
    resource.submitFeedback(SubmitFeedbackRequest(message), caller)

  "FeedbackResource" should "persist a submitted feedback and return it for the same user" in {
    submit("the editor is great")

    val feedback = resource.listMyFeedback(sessionUser)
    feedback should have size 1
    feedback.head.message shouldBe "the editor is great"
    feedback.head.uid shouldBe testUid
    feedback.head.fid should not be null
    feedback.head.creationTime should be > 0L
  }

  it should "return feedback newest first" in {
    submit("first message")
    Thread.sleep(1000) // creation_time has 1-second resolution
    submit("second message")

    val messages = resource.listMyFeedback(sessionUser).map(_.message)
    messages shouldBe List("second message", "first message")
  }

  it should "trim surrounding whitespace from the feedback message" in {
    submit("   padded message   ")
    resource.listMyFeedback(sessionUser).head.message shouldBe "padded message"
  }

  it should "reject an empty or whitespace-only feedback message" in {
    an[BadRequestException] should be thrownBy
      submit("   ")
    an[BadRequestException] should be thrownBy
      submit("")
    resource.listMyFeedback(sessionUser) shouldBe empty
  }

  it should "reject a null message body" in {
    an[BadRequestException] should be thrownBy
      submit(null)
  }

  it should "isolate feedback between users" in {
    submit("from test user")
    submit("from other user a", otherSessionUser)
    submit("from other user b", otherSessionUser)

    resource.listMyFeedback(sessionUser).map(_.message) shouldBe List("from test user")
    resource.listMyFeedback(otherSessionUser) should have size 2
  }

  "feedbackCounts" should "report per-user counts only for users with feedback" in {
    submit("a")
    submit("b")
    submit("c", otherSessionUser)

    val counts = resource.feedbackCounts().map(c => c.uid.intValue() -> c.count.intValue()).toMap
    counts(testUid) shouldBe 2
    counts(otherUid) shouldBe 1
  }

  it should "return an empty list when nobody has submitted feedback" in {
    resource.feedbackCounts() shouldBe empty
  }

  "listUserFeedback" should "return a specific user's feedback for admins" in {
    submit("target user feedback", otherSessionUser)
    submit("noise")

    val feedback = resource.listUserFeedback(otherUid)
    feedback.map(_.message) shouldBe List("target user feedback")
  }

  it should "reject a missing user_id" in {
    an[BadRequestException] should be thrownBy resource.listUserFeedback(null)
  }
}

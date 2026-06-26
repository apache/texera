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
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.FEEDBACK
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.apache.texera.web.resource.FeedbackResource.SubmitFeedbackRequest
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import javax.ws.rs.BadRequestException

class FeedbackResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

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
    user.setPassword("password")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    val userDao = new UserDao(getDSLContext.configuration())
    val testUser = makeUser(testUid, "feedback_spec_user")
    val otherUser = makeUser(otherUid, "feedback_spec_other_user")
    userDao.insert(testUser)
    userDao.insert(otherUser)
    sessionUser = new SessionUser(testUser)
    otherSessionUser = new SessionUser(otherUser)
  }

  override protected def afterAll(): Unit = shutdownDB()

  private def clearFeedback(): Unit = {
    getDSLContext.deleteFrom(FEEDBACK).execute()
  }

  "FeedbackResource" should "persist a submitted feedback and return it for the same user" in {
    clearFeedback()
    resource.submitFeedback(SubmitFeedbackRequest("the editor is great"), sessionUser)

    val feedback = resource.listMyFeedback(sessionUser)
    feedback should have size 1
    feedback.head.message shouldBe "the editor is great"
    feedback.head.uid shouldBe testUid
    feedback.head.fid should not be null
    feedback.head.creationTime should be > 0L
  }

  it should "return feedback newest first" in {
    clearFeedback()
    resource.submitFeedback(SubmitFeedbackRequest("first message"), sessionUser)
    Thread.sleep(1000) // creation_time has 1-second resolution
    resource.submitFeedback(SubmitFeedbackRequest("second message"), sessionUser)

    val messages = resource.listMyFeedback(sessionUser).map(_.message)
    messages shouldBe List("second message", "first message")
  }

  it should "trim surrounding whitespace from the feedback message" in {
    clearFeedback()
    resource.submitFeedback(SubmitFeedbackRequest("   padded message   "), sessionUser)
    resource.listMyFeedback(sessionUser).head.message shouldBe "padded message"
  }

  it should "reject an empty or whitespace-only feedback message" in {
    clearFeedback()
    an[BadRequestException] should be thrownBy
      resource.submitFeedback(SubmitFeedbackRequest("   "), sessionUser)
    an[BadRequestException] should be thrownBy
      resource.submitFeedback(SubmitFeedbackRequest(""), sessionUser)
    resource.listMyFeedback(sessionUser) shouldBe empty
  }

  it should "reject a null message body" in {
    clearFeedback()
    an[BadRequestException] should be thrownBy
      resource.submitFeedback(SubmitFeedbackRequest(null), sessionUser)
  }

  it should "isolate feedback between users" in {
    clearFeedback()
    resource.submitFeedback(SubmitFeedbackRequest("from test user"), sessionUser)
    resource.submitFeedback(SubmitFeedbackRequest("from other user a"), otherSessionUser)
    resource.submitFeedback(SubmitFeedbackRequest("from other user b"), otherSessionUser)

    resource.listMyFeedback(sessionUser).map(_.message) shouldBe List("from test user")
    resource.listMyFeedback(otherSessionUser) should have size 2
  }

  "feedbackCounts" should "report per-user counts only for users with feedback" in {
    clearFeedback()
    resource.submitFeedback(SubmitFeedbackRequest("a"), sessionUser)
    resource.submitFeedback(SubmitFeedbackRequest("b"), sessionUser)
    resource.submitFeedback(SubmitFeedbackRequest("c"), otherSessionUser)

    val counts = resource.feedbackCounts().map(c => c.uid.intValue() -> c.count.intValue()).toMap
    counts(testUid) shouldBe 2
    counts(otherUid) shouldBe 1
  }

  it should "return an empty list when nobody has submitted feedback" in {
    clearFeedback()
    resource.feedbackCounts() shouldBe empty
  }

  "listUserFeedback" should "return a specific user's feedback for admins" in {
    clearFeedback()
    resource.submitFeedback(SubmitFeedbackRequest("target user feedback"), otherSessionUser)
    resource.submitFeedback(SubmitFeedbackRequest("noise"), sessionUser)

    val feedback = resource.listUserFeedback(otherUid)
    feedback.map(_.message) shouldBe List("target user feedback")
  }

  it should "reject a missing user_id" in {
    an[BadRequestException] should be thrownBy resource.listUserFeedback(null)
  }
}

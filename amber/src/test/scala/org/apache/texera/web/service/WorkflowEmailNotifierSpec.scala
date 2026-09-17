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

package org.apache.texera.web.service

import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState._
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.WORKFLOW
import org.apache.texera.dao.jooq.generated.tables.daos.WorkflowDao
import org.apache.texera.dao.jooq.generated.tables.pojos.Workflow
import org.apache.texera.web.resource.EmailMessage
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, PrivateMethodTester}

import java.net.URI
import java.sql.Timestamp
import java.time.Instant
import java.util.{Locale, TimeZone, UUID}
import javax.ws.rs.NotFoundException

/**
  * Covers the parts of [[WorkflowEmailNotifier]] that decide *whether* to notify and *what* the
  * notification says. Concretely, these tests fail if:
  *   - the constructor stops resolving the workflow name from the `workflow` table (the name is
  *     seeded here with a random suffix, so a hard-coded or echoed value cannot satisfy the
  *     subject/content assertions), or stops surfacing a missing workflow as `NotFoundException`;
  *   - `createDashboardUrl` starts emitting `:80`/`:443`/no-port links, or drops the workflow id
  *     from the deep link, so the mail points somewhere the user cannot open;
  *   - `formatTimestamp` stops pinning UTC or changes the human-readable pattern (the JVM default
  *     zone is deliberately set to non-UTC around the assertion; the locale is pinned to US so the
  *     expected month/day text is stable across environments);
  *   - the terminal-state set stops covering a state that ends or suspends an execution, which
  *     would silently drop the only email a user gets for that run.
  *
  * SMTP is never exercised: only the pure message-building helpers and the state predicate are
  * invoked, so no test here reaches `GmailResource.sendEmail`.
  */
class WorkflowEmailNotifierSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with MockTexeraDB
    with PrivateMethodTester {

  private val testWid: Int = 7000 + scala.util.Random.nextInt(1000)
  private val unseededWid: Int = testWid + 1
  // Random suffix: the subject/content assertions can only pass if the notifier really read this
  // value back out of the database.
  private val testWorkflowName: String = "notifier_wf_" + UUID.randomUUID().toString.substring(0, 8)
  private val userEmail = "recipient@example.com"

  private val createEmailMessage = PrivateMethod[EmailMessage](Symbol("createEmailMessage"))
  private val createDashboardUrl = PrivateMethod[String](Symbol("createDashboardUrl"))
  private val createEmailSubject = PrivateMethod[String](Symbol("createEmailSubject"))
  private val createEmailContent = PrivateMethod[String](Symbol("createEmailContent"))
  private val formatTimestamp = PrivateMethod[String](Symbol("formatTimestamp"))
  private val isValidEmail = PrivateMethod[Boolean](Symbol("isValidEmail"))

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    deleteFixtureRows()

    val workflow = new Workflow
    workflow.setWid(testWid)
    workflow.setName(testWorkflowName)
    workflow.setContent("{}")
    workflow.setDescription("seeded by WorkflowEmailNotifierSpec")
    workflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
    new WorkflowDao(getDSLContext.configuration()).insert(workflow)
  }

  override protected def afterAll(): Unit = {
    deleteFixtureRows()
    shutdownDB()
  }

  private def deleteFixtureRows(): Unit =
    getDSLContext.deleteFrom(WORKFLOW).where(WORKFLOW.WID.in(testWid, unseededWid)).execute()

  /** Builds a notifier for the seeded workflow; the constructor hits the database every time. */
  private def notifierFor(sessionUri: String): WorkflowEmailNotifier =
    new WorkflowEmailNotifier(testWid.toLong, userEmail, new URI(sessionUri))

  // ─── createDashboardUrl ────────────────────────────────────────────────────

  "createDashboardUrl" should "omit the port when the session URI carries none" in {
    val url = notifierFor("http://texera.example.com/dashboard/user/workspace/9") invokePrivate
      createDashboardUrl()
    assert(url == s"http://texera.example.com/user/workspace/$testWid")
  }

  it should "omit an explicit port 80" in {
    val url = notifierFor("http://texera.example.com:80/dashboard") invokePrivate
      createDashboardUrl()
    assert(url == s"http://texera.example.com/user/workspace/$testWid")
  }

  // Note the assertion keeps `http://` even for the https-conventional :443. createDashboardUrl
  // hardcodes the scheme and never reads sessionUri.getScheme; that is today's behaviour, not an
  // endorsement of it. A fix that preserved the scheme would surface here.
  it should "omit an explicit port 443" in {
    val url = notifierFor("http://texera.example.com:443/dashboard") invokePrivate
      createDashboardUrl()
    assert(url == s"http://texera.example.com/user/workspace/$testWid")
  }

  it should "keep a non-default port" in {
    val url = notifierFor("http://localhost:8080/dashboard") invokePrivate createDashboardUrl()
    assert(url == s"http://localhost:8080/user/workspace/$testWid")
  }

  // ─── createEmailSubject / createEmailContent ───────────────────────────────

  "createEmailSubject" should "carry the workflow name read from the database" in {
    val subject = notifierFor("http://texera.example.com") invokePrivate
      createEmailSubject(COMPLETED)
    assert(subject == s"[Texera] Workflow $testWorkflowName ($testWid) Status: ${COMPLETED.name}")
  }

  "createEmailContent" should "list the database-resolved name, the id, the state and the link" in {
    val content = notifierFor("http://texera.example.com:8080/dashboard") invokePrivate
      createEmailContent(FAILED)

    // stripMargin + trim: a broken margin would leave the "|" prefixes and leading blank line.
    assert(content.startsWith("Hello,"))
    assert(content.contains(s"- Workflow ID: $testWid"))
    assert(content.contains(s"- Workflow Name: $testWorkflowName"))
    assert(content.contains(s"- State: ${FAILED.name}"))
    assert(
      content.contains(s"visiting: http://texera.example.com:8080/user/workspace/$testWid"),
      s"dashboard link missing from content:\n$content"
    )
    assert(
      content.linesIterator.exists(line =>
        line.startsWith("- Timestamp: ") && line.endsWith("(UTC)")
      ),
      s"no UTC timestamp line in content:\n$content"
    )
  }

  // ─── formatTimestamp ───────────────────────────────────────────────────────

  "formatTimestamp" should "render a fixed instant in UTC, independent of the JVM defaults" in {
    val previousLocale = Locale.getDefault
    val previousZone = TimeZone.getDefault
    // Non-UTC default zone: had the formatter used ZoneId.systemDefault() the clock time below
    // would read 06:07 instead of 14:07.
    Locale.setDefault(Locale.US)
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
    try {
      val rendered = notifierFor("http://texera.example.com") invokePrivate
        formatTimestamp(Instant.parse("2024-03-05T14:07:09Z"))
      assert(rendered == "March 5, 2024, 2:07:09 PM (UTC)")
    } finally {
      TimeZone.setDefault(previousZone)
      Locale.setDefault(previousLocale)
    }
  }

  // ─── isValidEmail ──────────────────────────────────────────────────────────

  "isValidEmail" should "accept a well-formed address" in {
    assert(notifierFor("http://texera.example.com") invokePrivate isValidEmail("user@example.com"))
  }

  it should "reject an address with no '@' and one with an empty domain" in {
    val notifier = notifierFor("http://texera.example.com")
    assert(!(notifier invokePrivate isValidEmail("user-at-example.com")))
    assert(!(notifier invokePrivate isValidEmail("user@")))
  }

  // ─── shouldSendEmail ───────────────────────────────────────────────────────

  "shouldSendEmail" should "accept the states that end or suspend an execution" in {
    val notifier = notifierFor("http://texera.example.com")
    Seq(COMPLETED, PAUSED, FAILED, KILLED).foreach { state =>
      assert(notifier.shouldSendEmail(state), s"expected $state to trigger a notification")
    }
  }

  it should "decline the states an execution merely passes through" in {
    val notifier = notifierFor("http://texera.example.com")
    // TERMINATED / UNKNOWN are intentionally left out: whether they should notify is a product
    // question, and pinning today's answer would block a harmless change.
    val inFlight: Seq[WorkflowAggregatedState] =
      Seq(UNINITIALIZED, READY, RUNNING, PAUSING, RESUMING)
    inFlight.foreach { state =>
      assert(!notifier.shouldSendEmail(state), s"expected $state not to trigger a notification")
    }
  }

  // ─── constructor ───────────────────────────────────────────────────────────

  "the constructor" should "propagate NotFoundException when the workflow row is missing" in {
    val ex = intercept[NotFoundException] {
      new WorkflowEmailNotifier(
        unseededWid.toLong,
        userEmail,
        new URI("http://texera.example.com")
      )
    }
    assert(ex.getMessage.contains(unseededWid.toString))
  }

  // ─── createEmailMessage ────────────────────────────────────────────────────

  "createEmailMessage" should "address the recipient and carry the subject and content for the state" in {
    val notifier = notifierFor("http://texera.example.com:8080/dashboard")
    val message = notifier invokePrivate createEmailMessage(KILLED)

    assert(message.receiver == userEmail)
    // the assembled subject must match what the dedicated builder produces for the same state
    assert(message.subject == (notifier invokePrivate createEmailSubject(KILLED)))
    assert(message.content.contains(testWorkflowName))
    assert(message.content.contains(testWid.toString))
    assert(message.content.contains(KILLED.name))
  }

  // ─── sendStatusEmail ───────────────────────────────────────────────────────

  // Only the invalid-recipient arm is driven here: it returns before reaching
  // GmailResource.sendEmail, so no SMTP call is made. The valid-address arm would dispatch
  // for real, so it belongs to the integration tier.
  "sendStatusEmail" should "return without dispatching when the recipient address is invalid" in {
    val notifier =
      new WorkflowEmailNotifier(
        testWid.toLong,
        "not-an-email",
        new URI("http://texera.example.com")
      )

    notifier.sendStatusEmail(COMPLETED) // must not throw and must not reach Gmail
    assert(!(notifier invokePrivate isValidEmail("not-an-email")))
  }
}

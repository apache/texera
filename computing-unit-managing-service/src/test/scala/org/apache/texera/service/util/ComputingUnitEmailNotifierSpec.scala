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

package org.apache.texera.service.util

import jakarta.mail.Message
import jakarta.mail.internet.{AddressException, InternetAddress, MimeMessage}
import org.apache.texera.common.config.UserSystemConfig
import org.apache.texera.dao.jooq.generated.tables.pojos.{User, WorkflowComputingUnit}
import org.apache.texera.service.util.ComputingUnitEmailNotifier.Email
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.Executor
import scala.collection.mutable.ListBuffer

// Which terminations send an email is pinned in ComputingUnitManagingResourceSpec.
class ComputingUnitEmailNotifierSpec extends AnyFlatSpec with Matchers {

  private def unit(name: String = "my-cu"): WorkflowComputingUnit = {
    val u = new WorkflowComputingUnit()
    u.setCuid(42)
    u.setUid(7)
    u.setName(name)
    u.setTerminateTime(Timestamp.from(Instant.parse("2026-10-26T15:04:05Z")))
    u
  }

  private def owner(email: String = "ada@example.com"): User = {
    val u = new User()
    u.setName("Ada")
    u.setEmail(email)
    u
  }

  private val directExecutor: Executor = (task: Runnable) => task.run()

  /** The returned buffer fills as the executor runs the send. */
  private def notifyOwner(
      owner: User,
      smtpConfigured: Boolean = true,
      executor: Executor = directExecutor
  ): ListBuffer[Email] = {
    val sent = ListBuffer.empty[Email]
    ComputingUnitEmailNotifier.notifyOwnerOfAdminTermination(
      unit(),
      owner,
      smtpConfigured,
      executor,
      email => sent += email
    )
    sent
  }

  private def email(
      ownerName: Option[String] = Some("Ada"),
      unitName: String = "my-cu",
      domain: Option[String] = None
  ): Email =
    ComputingUnitEmailNotifier.adminTerminationEmail(
      "ada@example.com",
      ownerName,
      unit(unitName),
      domain
    )

  private def mime(message: Email = email()): MimeMessage =
    ComputingUnitEmailNotifier.toMimeMessage("sender@example.com", message)

  "adminTerminationEmail" should "tell the owner which unit an administrator terminated, and when" in {
    val message = email()

    message.subject shouldBe "Your Computing Unit Has Been Terminated"
    message.body should include("Hello Ada,")
    message.body should include("An administrator terminated your computing unit")
    message.body should include("Name: my-cu")
    message.body should include("ID: 42")
    message.body should include("Terminated at: October 26, 2026, 3:04:05 PM (UTC)")
    message.body should include("please contact the administrator")
    message.body should include(s"Thank you for using ${UserSystemConfig.projectName}!")
  }

  it should "greet without a name when the owner has none" in {
    email(ownerName = None).body should include("Hello,\n")
    email(ownerName = Some("   ")).body should include("Hello,\n")
  }

  it should "name the deployment in the subject and footer when a domain is set" in {
    val message = email(domain = Some("https://texera.example.com"))

    message.subject shouldBe "Your Computing Unit Has Been Terminated for [texera.example.com]"
    message.body should endWith("—\nSent from: https://texera.example.com\n")
  }

  it should "leave the deployment out when no domain is set" in {
    val message = email(domain = None)

    message.subject should not include "for ["
    message.body should not include "Sent from"
  }

  "toMimeMessage" should "send from the configured address to the owner" in {
    val message = mime()

    message.getFrom.toSeq shouldBe Seq(new InternetAddress("sender@example.com"))
    message.getRecipients(Message.RecipientType.TO).toSeq shouldBe
      Seq(new InternetAddress("ada@example.com"))
    message.getSubject shouldBe email().subject
    message.getContent shouldBe email().body
  }

  it should "keep non-ASCII names intact and serialize to 7-bit text like Transport.send" in {
    // Serializing loads jakarta.activation, so a jar mismatch fails here, not on a real send.
    val message = mime(email(ownerName = Some("Zoë"), unitName = "分析-ユニット 🚀"))
    val wire = new ByteArrayOutputStream()
    message.writeTo(wire)

    message.getContent.asInstanceOf[String] should include("Hello Zoë,")
    message.getContent.asInstanceOf[String] should include("Name: 分析-ユニット 🚀")
    all(wire.toByteArray.toSeq) should be >= 0.toByte
    wire.toString(StandardCharsets.US_ASCII) should include(
      "Content-Type: text/plain; charset=UTF-8"
    )
  }

  it should "reject a malformed recipient address" in {
    an[AddressException] should be thrownBy mime(email().copy(recipient = "not an address"))
  }

  "smtpSession" should "use Gmail over SSL, check the server's identity, and bound every wait" in {
    val properties = ComputingUnitEmailNotifier.smtpSession().getProperties

    properties.getProperty("mail.smtp.host") shouldBe "smtp.gmail.com"
    properties.getProperty("mail.smtp.port") shouldBe "465"
    properties.getProperty("mail.smtp.auth") shouldBe "true"
    properties.getProperty("mail.smtp.ssl.enable") shouldBe "true"
    properties.getProperty("mail.smtp.ssl.checkserveridentity") shouldBe "true"
    Seq("connectiontimeout", "timeout", "writetimeout").foreach { key =>
      properties.getProperty(s"mail.smtp.$key").toInt should be > 0
    }
  }

  "notifyOwnerOfAdminTermination" should "email the owner's address off the caller's thread" in {
    val queued = ListBuffer.empty[Runnable]
    val sent = notifyOwner(owner(), executor = (task: Runnable) => queued += task)

    sent shouldBe empty
    queued.foreach(_.run())
    sent.map(_.recipient) shouldBe List("ada@example.com")
    sent.head.body should include("Hello Ada,")
  }

  it should "trim the owner's address" in {
    notifyOwner(owner(email = "  ada@example.com \n")).map(_.recipient) shouldBe
      List("ada@example.com")
  }

  it should "send nothing when no SMTP sender is configured" in {
    notifyOwner(owner(), smtpConfigured = false) shouldBe empty
  }

  it should "send nothing when the owner has no email address" in {
    notifyOwner(owner(email = null)) shouldBe empty
    notifyOwner(owner(email = "   ")) shouldBe empty
  }

  it should "swallow a failed send, since the unit is already terminated" in {
    noException should be thrownBy ComputingUnitEmailNotifier.notifyOwnerOfAdminTermination(
      unit(),
      owner(),
      smtpConfigured = true,
      directExecutor,
      _ => throw new RuntimeException("SMTP is down")
    )
  }

  it should "do nothing through the production entry point while SMTP is unset" in {
    // Cancelled if a real sender is configured, so it never mails anyone.
    assume(ComputingUnitEmailNotifier.senderAddress.isEmpty, "USER_SYS_GOOGLE_SMTP_GMAIL is set")
    noException should be thrownBy
      ComputingUnitEmailNotifier.notifyOwnerOfAdminTermination(unit(), owner())
  }
}

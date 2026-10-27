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

import com.typesafe.scalalogging.StrictLogging
import jakarta.mail.internet.{InternetAddress, MimeMessage}
import jakarta.mail.{Message, Session, Transport}
import org.apache.texera.common.config.UserSystemConfig
import org.apache.texera.dao.jooq.generated.tables.pojos.{User, WorkflowComputingUnit}

import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.{Locale, Properties}
import java.util.concurrent.{Executor, Executors}
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.control.NonFatal

/** Emails the owner when an admin terminates their unit, from the Gmail account amber uses. */
object ComputingUnitEmailNotifier extends StrictLogging {

  private[util] final class Email(val recipient: String, val subject: String, val body: String)

  // Trimmed once, so the configured check and the send see the same address.
  private[util] val senderAddress: String = UserSystemConfig.gmail.trim

  // Gmail over SSL like amber, with timeouts so a hung server cannot hold the sender thread.
  private val smtpProperties = Map(
    "mail.smtp.host" -> "smtp.gmail.com",
    "mail.smtp.port" -> "465",
    "mail.smtp.auth" -> "true",
    "mail.smtp.ssl.enable" -> "true",
    "mail.smtp.ssl.checkserveridentity" -> "true",
    "mail.smtp.connectiontimeout" -> "10000",
    "mail.smtp.timeout" -> "10000",
    "mail.smtp.writetimeout" -> "10000"
  )

  private val terminatedAtFormat =
    DateTimeFormatter
      .ofPattern("MMMM d, yyyy, h:mm:ss a '(UTC)'", Locale.ENGLISH)
      .withZone(ZoneOffset.UTC)

  // One daemon thread, so the terminate response never waits on SMTP.
  private val sendExecutor: Executor = Executors.newSingleThreadExecutor { (task: Runnable) =>
    val thread = new Thread(task, "computing-unit-email-notifier")
    thread.setDaemon(true)
    thread
  }

  /** The unit is already terminated, so a skipped or failed email is only logged. */
  def notifyOwnerOfAdminTermination(unit: WorkflowComputingUnit, owner: User): Unit =
    notifyOwnerOfAdminTermination(
      unit,
      owner,
      senderAddress.nonEmpty,
      sendExecutor,
      send(_, senderAddress, UserSystemConfig.smtpPassword)
    )

  private[util] def notifyOwnerOfAdminTermination(
      unit: WorkflowComputingUnit,
      owner: User,
      smtpConfigured: Boolean,
      executor: Executor,
      send: Email => Unit
  ): Unit = {
    val target = s"computing unit cuid=${unit.getCuid}, owner_uid=${unit.getUid}"
    Option(owner.getEmail).map(_.trim).filter(_.nonEmpty) match {
      case _ if !smtpConfigured =>
        logger.info(s"Not emailing the owner of $target: no SMTP sender is configured")
      case Some(recipient) =>
        executor.execute { () =>
          try {
            send(adminTerminationEmail(recipient, Option(owner.getName), unit))
            logger.info(s"Emailed the owner of $target about its termination by an administrator")
          } catch {
            case NonFatal(e) => logger.warn(s"Failed to email the owner of $target", e)
          }
        }
      case _ =>
        logger.info(s"Not emailing the owner of $target: the owner has no email address")
    }
  }

  /** Subject suffix and "Sent from" footer match amber's `EmailTemplate` and `GmailResource`. */
  private[util] def adminTerminationEmail(
      recipient: String,
      ownerName: Option[String],
      unit: WorkflowComputingUnit,
      domain: Option[String] = UserSystemConfig.appDomain
  ): Email = {
    val deployment = domain.map(_.replaceFirst("^https?://", "")).filter(_.nonEmpty)
    val subject =
      s"Your Computing Unit Has Been Terminated${deployment.fold("")(d => s" for [$d]")}"
    val greeting = ownerName.map(_.trim).filter(_.nonEmpty).fold("Hello,")(name => s"Hello $name,")
    val body =
      s"""
         |$greeting
         |
         |An administrator terminated your computing unit:
         |
         |Name: ${unit.getName}
         |ID: ${unit.getCuid}
         |Terminated at: ${terminatedAtFormat.format(unit.getTerminateTime.toInstant)}
         |
         |Workflows can no longer run on it. You can create a new computing unit from your workspace.
         |
         |If you have any questions, please contact the administrator.
         |
         |Thank you for using ${UserSystemConfig.projectName}!
         |""".stripMargin
    new Email(recipient, subject, domain.fold(body)(d => s"$body\n—\nSent from: $d\n"))
  }

  private[util] def smtpSession(): Session = {
    val properties = new Properties()
    properties.putAll(smtpProperties.asJava)
    Session.getInstance(properties)
  }

  private[util] def toMimeMessage(from: String, email: Email): MimeMessage = {
    val message = new MimeMessage(smtpSession())
    message.setFrom(new InternetAddress(from, true))
    message.setRecipient(Message.RecipientType.TO, new InternetAddress(email.recipient, true))
    // UTF-8, so a non-ASCII unit or owner name arrives intact.
    message.setSubject(email.subject, "UTF-8")
    message.setText(email.body, "UTF-8")
    message
  }

  private[util] def send(email: Email, from: String, password: String): Unit =
    Transport.send(toMimeMessage(from, email), from, password)
}

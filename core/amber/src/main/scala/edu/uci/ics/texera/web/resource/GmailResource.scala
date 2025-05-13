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

package edu.uci.ics.texera.web.resource

import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.texera.auth.SessionUser
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.jooq.generated.enums.UserRoleEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.web.resource.EmailTemplate.userRegistrationNotification
import edu.uci.ics.texera.web.resource.GmailResource.{isValidEmail, sendEmail, senderGmail, userDao}
import io.dropwizard.auth.Auth
import org.slf4j.LoggerFactory

import javax.annotation.security.RolesAllowed
import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{Message, PasswordAuthentication, Session, Transport}
import javax.ws.rs._
import scala.util.{Failure, Success, Try}

case class EmailMessage(receiver: String, subject: String, content: String)

object GmailResource {
  final private lazy val context = SqlServer
    .getInstance()
    .createDSLContext()
  final private lazy val userDao = new UserDao(context.configuration)

  private lazy val senderGmail: String = AmberConfig.gmail
  private val smtpProperties = Map(
    "mail.smtp.host" -> "smtp.gmail.com",
    "mail.smtp.port" -> "465",
    "mail.smtp.auth" -> "true",
    "mail.smtp.socketFactory.port" -> "465",
    "mail.smtp.socketFactory.class" -> "javax.net.ssl.SSLSocketFactory"
  )

  private def createSession(): Session = {
    Session.getInstance(
      smtpProperties.foldLeft(new java.util.Properties) {
        case (props, (key, value)) =>
          props.put(key, value)
          props
      },
      new javax.mail.Authenticator() {
        override def getPasswordAuthentication: PasswordAuthentication =
          new PasswordAuthentication(senderGmail, AmberConfig.smtpPassword)
      }
    )
  }

  private def createMimeMessage(
      session: Session,
      emailMessage: EmailMessage,
      recipientEmail: String
  ): MimeMessage = {
    val email = new MimeMessage(session)
    email.setFrom(new InternetAddress(senderGmail))
    email.addRecipient(Message.RecipientType.TO, new InternetAddress(recipientEmail))
    email.setSubject(emailMessage.subject)
    email.setText(emailMessage.content)
    email
  }

  def sendEmail(
      emailMessage: EmailMessage,
      recipientEmail: String
  ): Either[String, Unit] = {
    val logger = LoggerFactory.getLogger(this.getClass)

    if (!isValidEmail(recipientEmail)) {
      logger.warn(s"Attempted to send email to invalid address: $recipientEmail")
      return Left("Invalid email format")
    }

    Try {
      val session = createSession()
      val email = createMimeMessage(session, emailMessage, recipientEmail)
      Transport.send(email)
    } match {
      case Success(_)         => Right(())
      case Failure(exception) => Left(s"Failed to send email: ${exception.getMessage}")
    }
  }

  /**
    * Validates whether a given email address has a basic correct format.
    *
    * This method uses a regular expression to ensure the email:
    * - Has a valid local part containing letters, numbers, '+', '_', '.', or '-'
    * - Contains a single '@' character separating the local part and domain
    * - Has a valid domain containing letters, numbers, '.' or '-'
    * - Ends with a domain suffix (e.g., '.com', '.net') that is at least two letters long
    *
    * @param email the email address to validate
    * @return true if the email matches the expected format, false otherwise
    */
  private def isValidEmail(email: String): Boolean = {
    val emailRegex = "^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$".r
    email != null && emailRegex.matches(email)
  }
}

@Path("/gmail")
class GmailResource {
  @PUT
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/send")
  def sendEmailRequest(emailMessage: EmailMessage, @Auth user: SessionUser): Unit = {
    val recipientEmail = if (emailMessage.receiver.isEmpty) user.getEmail else emailMessage.receiver
    sendEmail(emailMessage, recipientEmail)
  }

  @GET
  @RolesAllowed(Array("ADMIN"))
  @Path("/sender/email")
  def getSenderEmail: String = senderGmail

  @POST
  @Path("/notify-unauthorized")
  def notifyUnauthorizedUser(emailMessage: EmailMessage): Unit = {
    val logger = LoggerFactory.getLogger(this.getClass)

    if (!isValidEmail(emailMessage.receiver)) {
      throw new ForbiddenException("Invalid email address.")
    }

    val adminUsers = userDao.fetchByRole(UserRoleEnum.ADMIN)
    val adminUserIterator = adminUsers.iterator()

    while (adminUserIterator.hasNext) {
      val admin = adminUserIterator.next()
      val adminEmail = admin.getEmail

      try {
        sendEmail(
          userRegistrationNotification(
            receiverEmail = adminEmail,
            userEmail = Some(emailMessage.receiver),
            toAdmin = true
          ),
          adminEmail
        )
      } catch {
        case ex: Exception =>
          logger.warn(s"Failed to send email to admin: $adminEmail. Error: ${ex.getMessage}")
      }
    }

    try {
      sendEmail(
        userRegistrationNotification(
          receiverEmail = emailMessage.receiver,
          userEmail = None,
          toAdmin = false
        ),
        emailMessage.receiver
      )
    } catch {
      case ex: Exception =>
        logger.warn(
          s"Failed to send notification to user: ${emailMessage.receiver}. Error: ${ex.getMessage}"
        )
    }
  }
}

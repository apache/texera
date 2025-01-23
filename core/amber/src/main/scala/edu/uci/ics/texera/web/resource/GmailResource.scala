package edu.uci.ics.texera.web.resource

import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.texera.web.auth.SessionUser
import io.dropwizard.auth.Auth

import javax.annotation.security.RolesAllowed
import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{Message, PasswordAuthentication, Session, Transport}
import javax.ws.rs._
import scala.util.{Failure, Success, Try}

case class EmailMessage(receiver: String, subject: String, content: String)

trait EmailService {
  def sendEmail(emailMessage: EmailMessage, recipientEmail: String): Either[String, Unit]
}

class GmailService extends EmailService {
  private lazy val gmail: String = AmberConfig.gmail
  private lazy val password: String = AmberConfig.smtpPassword

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
          new PasswordAuthentication(gmail, password)
      }
    )
  }

  private def createMimeMessage(
      session: Session,
      emailMessage: EmailMessage,
      recipientEmail: String
  ): MimeMessage = {
    val email = new MimeMessage(session)
    email.setFrom(new InternetAddress(gmail))
    email.addRecipient(Message.RecipientType.TO, new InternetAddress(recipientEmail))
    email.setSubject(emailMessage.subject)
    email.setText(emailMessage.content)
    email
  }

  override def sendEmail(
      emailMessage: EmailMessage,
      recipientEmail: String
  ): Either[String, Unit] = {
    Try {
      val session = createSession()
      val email = createMimeMessage(session, emailMessage, recipientEmail)
      Transport.send(email)
    } match {
      case Success(_)         => Right(())
      case Failure(exception) => Left(s"Failed to send email: ${exception.getMessage}")
    }
  }
}

@Path("/gmail")
class GmailResource(emailService: EmailService) {
  @GET
  @RolesAllowed(Array("ADMIN"))
  @Path("/sender/email")
  def getSenderEmail: String = AmberConfig.gmail

  /**
    * Send an email to the user.
    */
  @PUT
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/send")
  def sendEmail(emailMessage: EmailMessage, @Auth user: SessionUser): Either[String, Unit] = {
    val recipientEmail = if (emailMessage.receiver.isEmpty) user.getEmail else emailMessage.receiver
    emailService.sendEmail(emailMessage, recipientEmail)
  }
}

object GmailResource {
  private val emailService: EmailService = new GmailService()

  def sendEmail(emailMessage: EmailMessage, recipientEmail: String): Either[String, Unit] = {
    emailService.sendEmail(emailMessage, recipientEmail)
  }
}

package edu.uci.ics.texera.web.resource

import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.texera.web.auth.SessionUser
import io.dropwizard.auth.Auth

import java.util.Properties
import javax.annotation.security.RolesAllowed
import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{PasswordAuthentication, Session, Transport}
import javax.ws.rs._

case class EmailMessage(receiver: String, subject: String, content: String)
@Path("/gmail")
class GmailResource {
  final private lazy val gmail = AmberConfig.gmail
  final private lazy val password = AmberConfig.smtpPassword

  @GET
  @RolesAllowed(Array("ADMIN"))
  @Path("/sender/email")
  def getSenderEmail: String = gmail

  /**
    * Send an email to the user.
    */
  @PUT
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/send")
  def sendEmail(emailMessage: EmailMessage, @Auth user: SessionUser): Unit = {
    val prop = new Properties()
    prop.put("mail.smtp.host", "smtp.gmail.com")
    prop.put("mail.smtp.port", "465")
    prop.put("mail.smtp.auth", "true")
    prop.put("mail.smtp.socketFactory.port", "465")
    prop.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory")

    val email = new MimeMessage(
      Session.getInstance(
        prop,
        new javax.mail.Authenticator() {
          override def getPasswordAuthentication: PasswordAuthentication =
            new PasswordAuthentication(gmail, password)
        }
      )
    )
    email.setFrom(new InternetAddress("me"))
    email.addRecipient(
      javax.mail.Message.RecipientType.TO,
      new InternetAddress(if (emailMessage.receiver == "") user.getEmail else emailMessage.receiver)
    )
    email.setSubject(emailMessage.subject)
    email.setText(emailMessage.content)
    Transport.send(email)
  }
}

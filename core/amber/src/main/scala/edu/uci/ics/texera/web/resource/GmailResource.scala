package edu.uci.ics.texera.web.resource
import com.google.api.client.googleapis.auth.oauth2.{
  GoogleAuthorizationCodeTokenRequest,
  GoogleIdTokenVerifier,
  GoogleRefreshTokenRequest
}
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.auth.SessionUser
import io.dropwizard.auth.Auth

import java.nio.file.Files
import java.util.{Collections, Properties}
import javax.annotation.security.RolesAllowed
import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{Message, Session, Transport}
import javax.ws.rs._

case class EmailMessage(email: String, subject: String, content: String)

@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/gmail")
class GmailResource {
  final private lazy val path =
    Utils.amberHomePath.resolve("src").resolve("main").resolve("resources").resolve("gmail")
  final private lazy val clientId = AmberUtils.amberConfig.getString("user-sys.google.clientId")
  final private lazy val clientSecret =
    AmberUtils.amberConfig.getString("user-sys.google.clientSecret")

  def connect(transport: Transport, accessToken: String): Unit = {
    transport.connect(
      "smtp.gmail.com",
      587,
      new String(Files.readAllBytes(path.resolve("userEmail"))),
      accessToken
    )
  }
  private def saveAccessToken(): Unit = {
    Files.write(
      path.resolve("accessToken"),
      new GoogleRefreshTokenRequest(
        new NetHttpTransport(),
        new GsonFactory(),
        new String(Files.readAllBytes(path.resolve("refreshToken"))),
        clientId,
        clientSecret
      ).execute().getAccessToken.getBytes
    )
  }

  @GET
  @RolesAllowed(Array("ADMIN"))
  @Path("/email")
  def getEmail: String = {
    new String(Files.readAllBytes(path.resolve("userEmail")))
  }

  @DELETE
  @RolesAllowed(Array("ADMIN"))
  @Path("/revoke")
  def deleteEmail(): Unit = {
    if (Files.exists(path.resolve("userEmail"))) Files.delete(path.resolve("userEmail"))
    if (Files.exists(path.resolve("refreshToken"))) Files.delete(path.resolve("refreshToken"))
    if (Files.exists(path.resolve("accessToken"))) Files.delete(path.resolve("accessToken"))
  }
  @POST
  @RolesAllowed(Array("ADMIN"))
  @Path("/auth")
  def saveRefreshToken(code: String): Unit = {
    this.deleteEmail()
    val token = new GoogleAuthorizationCodeTokenRequest(
      new NetHttpTransport(),
      new GsonFactory(),
      clientId,
      clientSecret,
      code,
      "postmessage"
    ).execute()

    Files.write(
      path.resolve("refreshToken"),
      token.getRefreshToken.getBytes
    )

    Files.write(
      path.resolve("userEmail"),
      new GoogleIdTokenVerifier.Builder(new NetHttpTransport, GsonFactory.getDefaultInstance)
        .setAudience(Collections.singletonList(clientId))
        .build()
        .verify(token.getIdToken)
        .getPayload
        .getEmail
        .getBytes
    )
  }

  @PUT
  @Path("/send")
  def sendEmail(message: EmailMessage, @Auth user: SessionUser): Unit = {
    val props = new Properties()
    props.put("mail.smtp.starttls.required", "true")
    props.put("mail.smtp.sasl.enable", "true")
    props.put("mail.smtp.sasl.mechanisms", "XOAUTH2")
    val session = Session.getInstance(props)
    val transport = session.getTransport("smtp")
    val accessTokenPath = path.resolve("accessToken")
    if (!Files.exists(accessTokenPath)) saveAccessToken()
    var accessToken = new String(Files.readAllBytes(path.resolve(accessTokenPath)))

    connect(transport, accessToken)
    if (!transport.isConnected) {
      saveAccessToken()
      accessToken = new String(Files.readAllBytes(path.resolve(accessTokenPath)))
      connect(transport, accessToken)
    }
    val email = new MimeMessage(session)
    email.setFrom(new InternetAddress("me"))
    email.addRecipient(
      Message.RecipientType.TO,
      new InternetAddress(if (message.email == "") user.getEmail else message.email)
    )
    email.setSubject(message.subject)
    email.setText(message.content)
    transport.sendMessage(email, email.getAllRecipients)
  }
}

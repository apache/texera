package edu.uci.ics.texera.web.resource
import com.google.api.client.googleapis.auth.oauth2.{GoogleAuthorizationCodeTokenRequest, GoogleRefreshTokenRequest}
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.auth.SessionUser
import io.dropwizard.auth.Auth

import java.nio.file.Files
import java.util.Properties
import javax.annotation.security.RolesAllowed
import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{Message, Session, Transport}
import javax.ws.rs.{GET, POST, PUT, Path}

@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/gmail")
class GmailResource {
  final private lazy val path =
    Utils.amberHomePath.resolve("src").resolve("main").resolve("resources").resolve("gmail")
  final private lazy val clientId = AmberUtils.amberConfig.getString("user-sys.googleClientId")

  def connect(transport: Transport, accessToken: String): Unit = {
    transport.connect(
      "smtp.gmail.com",
      587,
      Files.readString(path.resolve("userEmail")),
      accessToken
    )
  }
  private def saveAccessToken(): Unit = {
    val response = new GoogleRefreshTokenRequest(
      new NetHttpTransport(),
      new GsonFactory(),
      Files.readString(path.resolve("refreshToken")),
      clientId,
      "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"
    ).execute()
    Files.write(
      path.resolve("accessToken"),
      response.getAccessToken.getBytes
    )
  }

  @POST
  @RolesAllowed(Array("ADMIN"))
  @Path("/auth")
  def saveRefreshToken(code: String, @Auth user: SessionUser): Unit = {
    val response = new GoogleAuthorizationCodeTokenRequest(
      new NetHttpTransport(),
      new GsonFactory(),
      clientId,
      "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",
      code,
      "postmessage"
    ).execute()
    Files.write(
      path.resolve("refreshToken"),
      response.getRefreshToken.getBytes
    )
    Files.write(
      path.resolve("userEmail"),
      user.getEmail.getBytes
    )
  }

  @PUT
  @Path("/send")
  def sendEmail(title: String, content: String): Unit = {
    val props = new Properties()
    props.put("mail.smtp.starttls.required", "true")
    props.put("mail.smtp.sasl.enable", "true")
    props.put("mail.smtp.sasl.mechanisms", "XOAUTH2")
    val session = Session.getInstance(props)
    val transport = session.getTransport("smtp")
    val accessTokenPath = path.resolve("accessToken")
    if (!Files.exists(accessTokenPath)) saveAccessToken()
    var accessToken = Files.readString(accessTokenPath)

    connect(transport, accessToken)
    if (!transport.isConnected) {
      saveAccessToken()
      accessToken = Files.readString(accessTokenPath)
      connect(transport, accessToken)
    }

    val email = new MimeMessage(session)
    email.setFrom(new InternetAddress("me"))
    email.addRecipient(Message.RecipientType.TO, new InternetAddress("linxinyuan@gmail.com"))
    email.setSubject("Ping")
    email.setText("Hello, this is example of sending email"+content)
    transport.sendMessage(email, email.getAllRecipients)
  }
}

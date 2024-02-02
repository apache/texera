package edu.uci.ics.texera.web.resource

import com.google.api.client.googleapis.auth.oauth2.{
  GoogleAuthorizationCodeTokenRequest,
  GoogleIdTokenVerifier,
  GoogleRefreshTokenRequest
}
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.gmail.Gmail
import com.google.api.services.gmail.model.Message
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.{AccessToken, GoogleCredentials}
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.auth.SessionUser
import io.dropwizard.auth.Auth
import org.apache.commons.codec.binary.Base64

import java.io.ByteArrayOutputStream
import java.nio.file.Files
import java.util.{Collections, Properties}
import javax.annotation.security.RolesAllowed
import javax.mail.Session
import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.ws.rs._

case class EmailMessage(receiver: String, subject: String, content: String)
@Path("/gmail")
class GmailResource {
  final private lazy val path =
    Utils.amberHomePath.resolve("src").resolve("main").resolve("resources").resolve("gmail")
  final private lazy val clientId = AmberConfig.googleClientId
  final private lazy val clientSecret =
    AmberConfig.googleClientSecret

  /**
    * Use the authorization code to get the refresh token and save it to the file.
    */
  @POST
  @RolesAllowed(Array("ADMIN"))
  @Path("/sender/auth")
  def saveRefreshToken(code: String): Unit = {
    this.revokeAuth()
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
      path.resolve("senderEmail"),
      new GoogleIdTokenVerifier.Builder(new NetHttpTransport, GsonFactory.getDefaultInstance)
        .setAudience(Collections.singletonList(clientId))
        .build()
        .verify(token.getIdToken)
        .getPayload
        .getEmail
        .getBytes
    )
  }

  /**
    * Delete all the files related to the sender.
    */
  @DELETE
  @RolesAllowed(Array("ADMIN"))
  @Path("/sender/revoke")
  def revokeAuth(): Unit = {
    if (Files.exists(path.resolve("senderEmail"))) Files.delete(path.resolve("senderEmail"))
    if (Files.exists(path.resolve("refreshToken"))) Files.delete(path.resolve("refreshToken"))
    if (Files.exists(path.resolve("accessToken"))) Files.delete(path.resolve("accessToken"))
  }

  /**
    * Get the sender email.
    */
  @GET
  @RolesAllowed(Array("ADMIN"))
  @Path("/sender/email")
  def getSenderEmail: String = {
    new String(Files.readAllBytes(path.resolve("senderEmail")))
  }

  /**
    * Send an email to the user.
    */
  @PUT
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/send")
  def sendEmail(emailMessage: EmailMessage, @Auth user: SessionUser): Unit = {
    val accessTokenPath = path.resolve("accessToken")
    if (!Files.exists(accessTokenPath)) {
      Files.write(
        accessTokenPath,
        new GoogleRefreshTokenRequest(
          new NetHttpTransport(),
          new GsonFactory(),
          new String(Files.readAllBytes(path.resolve("refreshToken"))),
          clientId,
          clientSecret
        ).execute().getAccessToken.getBytes
      )
    }
    val accessToken = new String(Files.readAllBytes(accessTokenPath))
    val credentials = GoogleCredentials.create(new AccessToken(accessToken, null))
    val service = new Gmail.Builder(
      new NetHttpTransport(),
      GsonFactory.getDefaultInstance,
      new HttpCredentialsAdapter(credentials)
    ).setApplicationName("Gmail samples").build()
    val email = new MimeMessage(Session.getDefaultInstance(new Properties(), null))
    email.setFrom(new InternetAddress("me"))
    email.addRecipient(
      javax.mail.Message.RecipientType.TO,
      new InternetAddress(if (emailMessage.receiver == "") user.getEmail else emailMessage.receiver)
    )
    email.setSubject(emailMessage.subject)
    email.setText(emailMessage.content)
    val buffer = new ByteArrayOutputStream()
    email.writeTo(buffer)
    val message = new Message()
    message.setRaw(Base64.encodeBase64URLSafeString(buffer.toByteArray))
    service.users().messages().send("me", message).execute()
  }
}

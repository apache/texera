package edu.uci.ics.texera.web

import edu.uci.ics.texera.web.basicauth.{AppAuthenticator, SessionUser}
import org.apache.http.client.utils.URLEncodedUtils
import org.jose4j.jwt.consumer.JwtConsumerBuilder
import org.jose4j.keys.HmacKey

import java.net.URI
import javax.websocket.HandshakeResponse
import javax.websocket.server.{HandshakeRequest, ServerEndpointConfig}
import scala.jdk.CollectionConverters.asScalaBufferConverter
import scala.util.Try

/**
  * This configurator extracts HTTPSession and associates it to ServerEndpointConfig,
  * allow it to be accessed by Websocket connections.
  * <pre>
  * See <a href="https://stackoverflow.com/questions/17936440/accessing-httpsession-
  *   from-httpservletrequest-in-a-web-socket-serverendpoint"></a>
  * </pre>
  */
class ServletAwareConfigurator extends ServerEndpointConfig.Configurator {
  val jwtTokenSecret: String = "dfwzsdzwh823zebdwdz772632gdsbd21"
  // create the JwtConsumer instance
  val consumer = new JwtConsumerBuilder()
    .setAllowedClockSkewInSeconds(30)
    .setRequireExpirationTime()
    .setRequireSubject()
    .setVerificationKey(new HmacKey(jwtTokenSecret.getBytes))
    .setRelaxVerificationKeyValidation()
    .build
  var authenticator = AppAuthenticator
  override def modifyHandshake(
      config: ServerEndpointConfig,
      request: HandshakeRequest,
      response: HandshakeResponse
  ): Unit = {
    val queryString = "?" + request.getQueryString
    val params = URLEncodedUtils.parse(new URI(queryString), "UTF8")
    val scalaParams: Seq[(String, String)] =
      collection.immutable.Seq(params.asScala: _*).map(pair => pair.getName -> pair.getValue)
    val paramsMap: Map[String, String] = scalaParams.toMap

    val sessionUser: SessionUser = Try(
      authenticator
        .authenticate(
          consumer.process(paramsMap.getOrElse("token", ""))
        )
        .get()
    ).getOrElse(null)

    config.getUserProperties.put(classOf[SessionUser].getName, sessionUser)

//    val httpSession = request.getHttpSession.asInstanceOf[HttpSession]
//    config.getUserProperties.put("httpSession", httpSession)
  }
}

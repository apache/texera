package edu.uci.ics.texera.web

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.web.auth.JwtAuth.jwtConsumer
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import org.apache.http.client.utils.URLEncodedUtils
import org.jooq.types.UInteger

import java.net.URI
import java.nio.charset.Charset
import javax.websocket.HandshakeResponse
import javax.websocket.server.{HandshakeRequest, ServerEndpointConfig}
import scala.jdk.CollectionConverters.asScalaBufferConverter

/**
  * This configurator extracts HTTPSession and associates it to ServerEndpointConfig,
  * allow it to be accessed by Websocket connections.
  * <pre>
  * See <a href="https://stackoverflow.com/questions/17936440/accessing-httpsession-
  *   from-httpservletrequest-in-a-web-socket-serverendpoint"></a>
  * </pre>
  */
class ServletAwareConfigurator extends ServerEndpointConfig.Configurator with LazyLogging {

  override def modifyHandshake(
      config: ServerEndpointConfig,
      request: HandshakeRequest,
      response: HandshakeResponse
  ): Unit = {
    try {
      val params =
        URLEncodedUtils.parse(new URI("?" + request.getQueryString), Charset.defaultCharset())
      params.asScala
        .map(pair => pair.getName -> pair.getValue)
        .toMap
        .get("access-token")
        .map(token => {
          val claims = jwtConsumer.process(token).getJwtClaims
          config.getUserProperties.put(
            classOf[User].getName,
            new User(
              claims.getSubject,
              UInteger.valueOf(claims.getClaimValue("userId").asInstanceOf[Long]),
              null,
              null
            )
          )
        })

    } catch {
      case e: Exception =>
        logger.error("Failed to retrieve the User during websocket handshake", e)
    }

  }
}

package edu.uci.ics.texera.web

import edu.uci.ics.texera.web.basicauth.JwtAuth.jwtConsumer
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import org.apache.http.client.utils.URLEncodedUtils
import org.jooq.types.UInteger

import java.net.URI
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
class ServletAwareConfigurator extends ServerEndpointConfig.Configurator {

  override def modifyHandshake(
      config: ServerEndpointConfig,
      request: HandshakeRequest,
      response: HandshakeResponse
  ): Unit = {
    try {
      val queryString = "?" + request.getQueryString
      val params = URLEncodedUtils.parse(new URI(queryString), "UTF8")
      val scalaParams: Seq[(String, String)] =
        collection.immutable.Seq(params.asScala: _*).map(pair => pair.getName -> pair.getValue)
      val paramsMap: Map[String, String] = scalaParams.toMap
      val token = paramsMap.get("token")
      val claims = jwtConsumer.process(token.get).getJwtClaims
      config.getUserProperties.put(
        classOf[User].getName,
        new User(
          claims.getSubject,
          UInteger.valueOf(claims.getClaimValue("userId").asInstanceOf[Long]),
          null,
          null
        )
      )
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }
}

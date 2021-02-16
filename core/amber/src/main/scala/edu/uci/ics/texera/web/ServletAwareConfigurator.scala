package edu.uci.ics.texera.web

import javax.servlet.http.HttpSession
import javax.websocket.HandshakeResponse
import javax.websocket.server.{HandshakeRequest, ServerEndpointConfig}

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
    val httpSession = request.getHttpSession.asInstanceOf[HttpSession]
    config.getUserProperties.put("httpSession", httpSession)
  }
}

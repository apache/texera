package edu.uci.ics.texera.web.resource

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.web.ServletAwareConfigurator
import edu.uci.ics.texera.web.resource.CollaborationResource.websocketSessionMap

import javax.websocket.server.ServerEndpoint
import javax.websocket.{OnClose, OnMessage, OnOpen, Session}
import scala.collection.mutable

object CollaborationResource {
  final val websocketSessionMap = new mutable.HashMap[String, Session]()
}

@ServerEndpoint(
  value = "/wsapi/collab",
  configurator = classOf[ServletAwareConfigurator]
)
class CollaborationResource extends LazyLogging {

  @OnOpen
  def myOnOpen(session: Session): Unit = {
    websocketSessionMap += (session.getId -> session)
  }

  @OnMessage
  def myOnMsg(senderSession: Session, message: String): Unit = {
    for (sessionId <- websocketSessionMap.keySet) {
      // only send to other sessions, not the session that sent the message
      val session = websocketSessionMap(sessionId)
      if (session != senderSession) {
        websocketSessionMap(sessionId).getAsyncRemote.sendText(message)
      }
    }
    logger.info("[collab] message propagated: " + message)
  }

  @OnClose
  def myOnClose(session: Session): Unit = {
    websocketSessionMap -= session.getId
    logger.info("[collab] session " + session.getId + " disconnected")
  }
}

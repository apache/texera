package edu.uci.ics.texera.web.resource

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.web.ServletAwareConfigurator

import javax.websocket._
import javax.websocket.server.ServerEndpoint
import java.net.URI
import scala.collection.mutable

@ServerEndpoint(
  value = "/wsapi/workflow-websocket",
  configurator = classOf[ServletAwareConfigurator]
)
class ProxyWebsocketResource extends LazyLogging {

  // Store proxy-to-target session mapping
  private val clientToTargetSessionMap: mutable.Map[String, Session] = mutable.Map()

  @OnOpen
  def myOnOpen(session: Session, config: EndpointConfig): Unit = {
    logger.info(s"Proxy connection open for session: ${session.getId}")
    try {
      // Establish a unique connection to the target WebSocket server for each client
      val container = ContainerProvider.getWebSocketContainer
      // Define an inline Endpoint for managing the target connection
      val targetSession = container.connectToServer(new Endpoint {
        override def onOpen(targetSession: Session, config: EndpointConfig): Unit = {
          // Forward messages from the target session to the client session
          targetSession.addMessageHandler(new MessageHandler.Whole[String] {
            override def onMessage(message: String): Unit = {
              session.getAsyncRemote.sendText(message) // Send message back to client
            }
          })
        }

        override def onClose(targetSession: Session, closeReason: CloseReason): Unit = {
          logger.info(s"Disconnected from target WebSocket server: $closeReason")
          session.close(closeReason) // Close client session if target disconnects
        }

        override def onError(targetSession: Session, thr: Throwable): Unit = {
          logger.error("Error in target WebSocket session", thr)
          session.close(new CloseReason(CloseReason.CloseCodes.UNEXPECTED_CONDITION, "Error in target WebSocket"))
        }
      }, new URI(s"ws://localhost:8085/wsapi/workflow-websocket?${session.getQueryString}"))
      // Map client session ID to its corresponding target session
      clientToTargetSessionMap.put(session.getId, targetSession)
    } catch {
      case e: Exception =>
        logger.error("Failed to connect to target WebSocket", e)
        session.close(new CloseReason(CloseReason.CloseCodes.CANNOT_ACCEPT, "Cannot connect to target"))
    }
  }

  @OnClose
  def myOnClose(session: Session, cr: CloseReason): Unit = {
    logger.info(s"Proxy connection closed for session: ${session.getId}")
    // Close and remove the target session associated with this client
    clientToTargetSessionMap.get(session.getId).foreach { targetSession =>
      if (targetSession.isOpen) {
        targetSession.close()
      }
      clientToTargetSessionMap.remove(session.getId)
    }
  }

  @OnMessage
  def myOnMsg(session: Session, message: String): Unit = {
    logger.info(s"Proxy forwarding message from session ${session.getId}: $message")
    // Forward the message to the target session associated with this client
    clientToTargetSessionMap.get(session.getId) match {
      case Some(targetSession) if targetSession.isOpen =>
        targetSession.getAsyncRemote.sendText(message)
      case _ =>
        logger.warn(s"No open target session found for client session ${session.getId}")
    }
  }
}



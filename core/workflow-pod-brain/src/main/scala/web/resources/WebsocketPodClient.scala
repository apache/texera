package web.resources

import jakarta.websocket._

class WebsocketPodClient(sharedState: SharedState) extends Endpoint {

  def onOpen(session: Session, config: EndpointConfig): Unit = {
    sharedState.podSession = Some(session)

    // Forward messages from the Pod to the user
    session.addMessageHandler(new MessageHandler.Whole[String] {
      override def onMessage(message: String): Unit = {
        sharedState.clientSession.foreach { clientSession =>
          if (clientSession.isOpen) {
            clientSession.getBasicRemote.sendText(message)
          }
        }
      }
    })
  }

  override def onError(session: Session, thr: Throwable): Unit = {
    thr.printStackTrace()
  }

  override def onClose(session: Session, closeReason: CloseReason): Unit = {
    println(s"Pod connection closed: ${closeReason.getReasonPhrase}")
  }
}

package web.resources

import java.net.URI
import jakarta.websocket.{ClientEndpointConfig, ContainerProvider, OnClose, OnMessage, OnOpen, Session}
import jakarta.websocket.server.ServerEndpoint

@ServerEndpoint(value = "/ws-proxy")
class WebsocketProxyEndpoint {

  val sharedState: SharedState = new SharedState()

  @OnOpen
  def onOpen(session: Session): Unit = {
    // Create a new SharedState instance for each connection
    sharedState.clientSession = Some(session)

    // Connect to the target WebSocket server (the Kubernetes Pod)
    try {
//      val targetUri = new URI("ws://user-pod-1.workflow-pods.default.svc.cluster.local:8010")
      val targetUri = new URI("ws://localhost:8010")

      // Use jarkata websockets to connect to User Pod
      val client = ContainerProvider.getWebSocketContainer
      val endpointInstance = new WebsocketPodClient(sharedState)
      val clientConfig = ClientEndpointConfig.Builder.create().build()
      client.connectToServer(endpointInstance, clientConfig, targetUri)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  @OnMessage
  def onMessage(message: String): Unit = {
    // Forward the message from the user to the Pod
    sharedState.podSession.foreach { podSession =>
      if (podSession.isOpen) {
        podSession.getBasicRemote.sendText(message)
      }
    }
  }

  @OnClose
  def onClose(): Unit = {
    // Close the connection to the Pod when the user disconnects
    sharedState.podSession.foreach(_.close())
    sharedState.clientSession.foreach(_.close())
  }
}
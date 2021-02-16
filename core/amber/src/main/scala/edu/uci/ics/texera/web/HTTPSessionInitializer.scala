package edu.uci.ics.texera.web

import javax.servlet.{ServletRequestEvent, ServletRequestListener}
import javax.servlet.annotation.WebListener
import javax.servlet.http.HttpServletRequest

/**
  * This Listener will create HTTPSession if not existing on an incoming request.
  * <pre>
  * See <a href="https://stackoverflow.com/questions/20240591/websocket-httpsess
  *    ion-returns-null"></a>
  * </pre>
  */
@WebListener
class HTTPSessionInitializer extends ServletRequestListener {
  override def requestDestroyed(sre: ServletRequestEvent): Unit = {
    // NOT NEEDED Auto-generated method stub
  }
  override def requestInitialized(sre: ServletRequestEvent): Unit =
    sre.getServletRequest.asInstanceOf[HttpServletRequest].getSession
}
